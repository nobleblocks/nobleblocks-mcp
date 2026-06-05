#!/usr/bin/env python3
"""
BigQuery Patent Export Pipeline
================================
Queries Google patents-public-data.patents.publications (130M+ patents)
Exports patent_id, assignees, IPC codes, CPC codes in batches
Uploads results to S3 for server-side ingestion

Usage:
    python3 bigquery_patent_export.py [--phase assignees|full|citations] [--limit N]
    
Phases:
    assignees  - Just export assignees for existing US patents (fast, ~10GB query)
    citations  - Export patent→paper citation links (academic references)
    full       - Full patent metadata export (title, abstract, assignees, codes)

Requires:
    - google-cloud-bigquery (pip install google-cloud-bigquery)
    - Application Default Credentials (gcloud auth application-default login)
    - AWS credentials for S3 upload (AWS_PROFILE=admin-delroy)
"""

import argparse
import gzip
import json
import logging
import os
import subprocess
import sys
import time
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
log = logging.getLogger(__name__)

# Suppress Google deprecation warnings
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)

from google.cloud import bigquery

# Configuration
PROJECT_ID = "gen-lang-client-0004533848"  # quota project for billing
S3_BUCKET = "nobleblocks-data"
S3_PREFIX = "bigquery-exports"
OUTPUT_DIR = Path("/tmp/bigquery_patents")
AWS_PROFILE = "admin-delroy"
AWS_REGION = "ap-southeast-1"


def get_client():
    """Create BigQuery client with application default credentials."""
    client = bigquery.Client(project=PROJECT_ID)
    log.info(f"Connected to BigQuery project: {PROJECT_ID}")
    return client


def export_assignees(client, limit=None):
    """
    Phase 1: Export assignees for US patents.
    This enriches our existing 9.4M US patents with assignee data.
    Query cost: ~5GB scanned (free tier: 1TB/month)
    """
    log.info("=== PHASE: ASSIGNEE EXPORT ===")
    
    # Query: Get patent_number -> assignee mapping for US patents
    query = """
    SELECT 
        publication_number,
        assignee_harmonized[SAFE_OFFSET(0)].name AS primary_assignee,
        ARRAY_TO_STRING(
            ARRAY(SELECT name FROM UNNEST(assignee_harmonized) LIMIT 5), 
            ' | '
        ) AS all_assignees,
        CASE 
            WHEN ARRAY_LENGTH(assignee_harmonized) > 0 
                AND assignee_harmonized[SAFE_OFFSET(0)].name IS NOT NULL
            THEN
                CASE
                    WHEN REGEXP_CONTAINS(LOWER(assignee_harmonized[SAFE_OFFSET(0)].name), 
                        r'university|college|institute|school|academia|research center')
                    THEN 'university'
                    WHEN REGEXP_CONTAINS(LOWER(assignee_harmonized[SAFE_OFFSET(0)].name),
                        r'inc\\.|corp|ltd|llc|gmbh|co\\.|company|pharmaceutical|pharma')
                    THEN 'corporate'
                    ELSE 'other'
                END
            ELSE NULL
        END AS assignee_type
    FROM `patents-public-data.patents.publications`
    WHERE country_code = 'US'
        AND grant_date > 0
        AND ARRAY_LENGTH(assignee_harmonized) > 0
    """
    if limit:
        query += f"\n    LIMIT {limit}"
    
    return run_export(client, query, "assignees", limit)


def export_full_metadata(client, limit=None):
    """
    Phase 2: Full patent metadata export (all countries).
    Query cost: ~50GB scanned
    """
    log.info("=== PHASE: FULL METADATA EXPORT ===")
    
    query = """
    SELECT 
        publication_number,
        application_number,
        country_code,
        CAST(filing_date AS STRING) AS filing_date,
        CAST(grant_date AS STRING) AS grant_date,
        title_localized[SAFE_OFFSET(0)].text AS title,
        abstract_localized[SAFE_OFFSET(0)].text AS abstract,
        assignee_harmonized[SAFE_OFFSET(0)].name AS primary_assignee,
        ARRAY_TO_STRING(
            ARRAY(SELECT name FROM UNNEST(assignee_harmonized) LIMIT 5), 
            ' | '
        ) AS all_assignees,
        ARRAY_TO_STRING(
            ARRAY(SELECT DISTINCT code FROM UNNEST(ipc) LIMIT 10), 
            ' | '
        ) AS ipc_codes,
        ARRAY_TO_STRING(
            ARRAY(SELECT DISTINCT code FROM UNNEST(cpc) LIMIT 10), 
            ' | '
        ) AS cpc_codes,
        ARRAY_TO_STRING(
            ARRAY(SELECT name FROM UNNEST(inventor_harmonized) LIMIT 10), 
            ' | '
        ) AS inventors,
        CASE 
            WHEN ARRAY_LENGTH(assignee_harmonized) > 0 
                AND assignee_harmonized[SAFE_OFFSET(0)].name IS NOT NULL
            THEN
                CASE
                    WHEN REGEXP_CONTAINS(LOWER(assignee_harmonized[SAFE_OFFSET(0)].name), 
                        r'university|college|institute|school|academia|research center')
                    THEN 'university'
                    WHEN REGEXP_CONTAINS(LOWER(assignee_harmonized[SAFE_OFFSET(0)].name),
                        r'inc\\.|corp|ltd|llc|gmbh|co\\.|company|pharmaceutical|pharma')
                    THEN 'corporate'
                    ELSE 'other'
                END
            ELSE NULL
        END AS assignee_type
    FROM `patents-public-data.patents.publications`
    WHERE grant_date > 0
        AND title_localized IS NOT NULL
        AND ARRAY_LENGTH(title_localized) > 0
    """
    if limit:
        query += f"\n    LIMIT {limit}"
    
    return run_export(client, query, "full_metadata", limit)


def export_citations(client, limit=None):
    """
    Phase 3: Patent→academic paper citation links.
    This is the KEY data for VC intelligence — which patents cite which papers.
    Uses the citation field which contains DOIs and paper references.
    Query cost: ~20GB scanned
    """
    log.info("=== PHASE: CITATION EXPORT ===")
    
    # The citations are in the 'citation' repeated field
    # Each citation has: publication_number (cited patent), npl_text (non-patent literature = papers)
    query = """
    SELECT 
        pub.publication_number AS citing_patent,
        pub.country_code,
        CAST(pub.filing_date AS STRING) AS filing_date,
        cit.npl_text AS npl_citation_text,
        -- Try to extract DOI from NPL text
        REGEXP_EXTRACT(cit.npl_text, r'(10\\.\\d{4,}/[^\\s,;]+)') AS extracted_doi
    FROM `patents-public-data.patents.publications` AS pub,
        UNNEST(citation) AS cit
    WHERE cit.npl_text IS NOT NULL 
        AND LENGTH(cit.npl_text) > 10
        AND pub.grant_date > 0
    """
    if limit:
        query += f"\n    LIMIT {limit}"
    
    return run_export(client, query, "citations", limit)


def export_ipc_codes(client, limit=None):
    """
    Phase 4: IPC/CPC classification codes for US patents.
    Enables the emerging-fields endpoint and technology domain analysis.
    Query cost: ~8GB scanned
    """
    log.info("=== PHASE: IPC/CPC CODE EXPORT ===")
    
    query = """
    SELECT 
        publication_number,
        ARRAY_TO_STRING(
            ARRAY(SELECT DISTINCT code FROM UNNEST(ipc) LIMIT 10), 
            ' | '
        ) AS ipc_codes,
        ARRAY_TO_STRING(
            ARRAY(SELECT DISTINCT code FROM UNNEST(cpc) LIMIT 10), 
            ' | '
        ) AS cpc_codes
    FROM `patents-public-data.patents.publications`
    WHERE country_code = 'US'
        AND grant_date > 0
        AND (ARRAY_LENGTH(ipc) > 0 OR ARRAY_LENGTH(cpc) > 0)
    """
    if limit:
        query += f"\n    LIMIT {limit}"
    
    return run_export(client, query, "ipc_codes", limit)


def run_export(client, query, phase_name, limit):
    """Execute query and stream results to gzipped JSONL file with retry."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    output_file = OUTPUT_DIR / f"patents_{phase_name}_{timestamp}.jsonl.gz"
    
    log.info(f"Running BigQuery query for phase: {phase_name}")
    log.info(f"Output file: {output_file}")
    
    # Estimate query cost (dry run)
    job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    try:
        dry_run = client.query(query, job_config=job_config)
        bytes_processed = dry_run.total_bytes_processed
        gb_processed = bytes_processed / (1024**3)
        cost_estimate = gb_processed * 5.0 / 1000  # $5 per TB
        log.info(f"Estimated query cost: {gb_processed:.2f} GB ({cost_estimate:.4f} USD)")
        
        if gb_processed > 100 and not limit:
            log.warning(f"Query would scan {gb_processed:.1f} GB. Use --limit to test first.")
            response = input("Continue? (y/n): ")
            if response.lower() != 'y':
                log.info("Aborted by user")
                return None
    except Exception as e:
        log.warning(f"Dry run failed (may still work): {e}")
    
    # Execute the actual query with longer timeout
    log.info("Executing query...")
    job_config = bigquery.QueryJobConfig(use_query_cache=True)
    query_job = client.query(query, job_config=job_config)
    
    # Stream results to gzipped JSONL with retry on connection errors
    row_count = 0
    start_time = time.time()
    max_retries = 5
    retry_count = 0
    
    with gzip.open(output_file, 'wt', encoding='utf-8') as f:
        rows_iter = query_job.result(page_size=50000, timeout=1800)
        
        for page in rows_iter.pages:
            retry_count = 0  # Reset on successful page
            for row in page:
                record = dict(row.items())
                f.write(json.dumps(record, default=str) + '\n')
                row_count += 1
            
            if row_count % 100000 == 0:
                elapsed = time.time() - start_time
                rate = row_count / elapsed if elapsed > 0 else 0
                log.info(f"  Exported {row_count:,} rows ({rate:.0f} rows/sec)")
                f.flush()
    
    elapsed = time.time() - start_time
    file_size_mb = output_file.stat().st_size / (1024**2)
    
    log.info(f"Export complete: {row_count:,} rows in {elapsed:.1f}s")
    log.info(f"File size: {file_size_mb:.1f} MB ({output_file})")
    
    return output_file


def upload_to_s3(local_file):
    """Upload exported file to S3 for server-side ingestion."""
    if not local_file or not local_file.exists():
        log.error("No file to upload")
        return False
    
    s3_key = f"{S3_PREFIX}/{local_file.name}"
    s3_uri = f"s3://{S3_BUCKET}/{s3_key}"
    
    log.info(f"Uploading to {s3_uri}")
    
    cmd = [
        "aws", "s3", "cp", str(local_file), s3_uri,
        "--region", AWS_REGION
    ]
    
    env = os.environ.copy()
    env["AWS_PROFILE"] = AWS_PROFILE
    
    result = subprocess.run(cmd, env=env, capture_output=True, text=True)
    
    if result.returncode == 0:
        log.info(f"Upload complete: {s3_uri}")
        return True
    else:
        log.error(f"Upload failed: {result.stderr}")
        return False


def main():
    parser = argparse.ArgumentParser(description="BigQuery Patent Export Pipeline")
    parser.add_argument(
        "--phase", 
        choices=["assignees", "full", "citations", "ipc"],
        default="assignees",
        help="Export phase (default: assignees)"
    )
    parser.add_argument(
        "--limit", 
        type=int, 
        default=None,
        help="Limit rows (for testing)"
    )
    parser.add_argument(
        "--no-upload",
        action="store_true",
        help="Skip S3 upload"
    )
    args = parser.parse_args()
    
    client = get_client()
    
    # Run the selected phase
    if args.phase == "assignees":
        output_file = export_assignees(client, args.limit)
    elif args.phase == "full":
        output_file = export_full_metadata(client, args.limit)
    elif args.phase == "citations":
        output_file = export_citations(client, args.limit)
    elif args.phase == "ipc":
        output_file = export_ipc_codes(client, args.limit)
    
    if output_file and not args.no_upload:
        upload_to_s3(output_file)
    
    log.info("Done!")


if __name__ == "__main__":
    main()
