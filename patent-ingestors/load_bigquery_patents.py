#!/usr/bin/env python3
"""
BigQuery Patent Loader — Server-side
=====================================
Downloads BigQuery export from S3 and loads into the patents table.
Handles both assignee-only updates and full patent inserts.

Usage (on paper-db server):
    python3 load_bigquery_patents.py --phase assignees
    python3 load_bigquery_patents.py --phase full
    python3 load_bigquery_patents.py --phase citations

Deployment:
    Upload to S3 → SSM pull → systemd-run
"""

import gzip
import json
import logging
import os
import subprocess
import sys
import time
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
log = logging.getLogger(__name__)

# Database config
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "paper_search",
    "user": "nobleblocks",
    "password": "nb_papers_2026_prod"
}

S3_BUCKET = "nobleblocks-data"
S3_PREFIX = "bigquery-exports"
DATA_DIR = Path("/opt/nobleblocks/paper-db/data/bigquery")
AWS_REGION = "ap-southeast-1"


def download_from_s3(phase):
    """Download the latest export file for the given phase from S3."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    # List files matching phase
    s3_path = f"s3://{S3_BUCKET}/{S3_PREFIX}/"
    result = subprocess.run(
        ["aws", "s3", "ls", s3_path, "--region", AWS_REGION],
        capture_output=True, text=True
    )
    
    if result.returncode != 0:
        log.error(f"Failed to list S3: {result.stderr}")
        return None
    
    # Find latest file for this phase
    files = []
    for line in result.stdout.strip().split('\n'):
        if f"patents_{phase}" in line and line.strip():
            parts = line.strip().split()
            if len(parts) >= 4:
                files.append(parts[-1])
    
    if not files:
        log.error(f"No export files found for phase: {phase}")
        return None
    
    # Get the latest one (sorted by timestamp in filename)
    latest_file = sorted(files)[-1]
    s3_uri = f"s3://{S3_BUCKET}/{S3_PREFIX}/{latest_file}"
    local_path = DATA_DIR / latest_file
    
    if local_path.exists():
        log.info(f"File already downloaded: {local_path}")
        return local_path
    
    log.info(f"Downloading: {s3_uri}")
    result = subprocess.run(
        ["aws", "s3", "cp", s3_uri, str(local_path), "--region", AWS_REGION],
        capture_output=True, text=True
    )
    
    if result.returncode != 0:
        log.error(f"Download failed: {result.stderr}")
        return None
    
    log.info(f"Downloaded: {local_path} ({local_path.stat().st_size / 1024**2:.1f} MB)")
    return local_path


def load_assignees(filepath):
    """
    Load assignee data — UPDATE existing patents with assignee info.
    Expects JSONL with: publication_number, primary_assignee, all_assignees, assignee_type
    """
    log.info("=== Loading assignees into patents table ===")
    
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    cur = conn.cursor()
    
    # Ensure assignee_type column exists
    cur.execute("""
        DO $$ BEGIN
            ALTER TABLE patents ADD COLUMN IF NOT EXISTS assignee_type TEXT;
        EXCEPTION WHEN duplicate_column THEN NULL;
        END $$;
    """)
    conn.commit()
    
    batch_size = 5000
    batch = []
    updated = 0
    skipped = 0
    start_time = time.time()
    
    opener = gzip.open if str(filepath).endswith('.gz') else open
    
    with opener(filepath, 'rt', encoding='utf-8') as f:
        for line in f:
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                skipped += 1
                continue
            
            pub_num = record.get('publication_number', '')
            primary_assignee = record.get('primary_assignee', '')
            assignee_type = record.get('assignee_type', '')
            
            if not pub_num or not primary_assignee:
                skipped += 1
                continue
            
            # Convert BigQuery publication_number format (US-12224364-B2) to our DB format (US-12224364)
            # Our patents table uses format from PatentsView: "US-{number}" without kind code
            # Strip the kind code suffix (last segment after the number)
            parts = pub_num.split('-')
            if len(parts) >= 3:
                # US-12224364-B2 → US-12224364
                patent_id = f"{parts[0]}-{parts[1]}"
            else:
                patent_id = pub_num
            
            batch.append((primary_assignee, assignee_type, patent_id))
            
            if len(batch) >= batch_size:
                count = flush_assignee_batch(cur, batch)
                updated += count
                conn.commit()
                batch = []
                
                if updated % 50000 == 0:
                    elapsed = time.time() - start_time
                    rate = updated / elapsed if elapsed > 0 else 0
                    log.info(f"  Updated {updated:,} patents ({rate:.0f}/sec, skipped {skipped:,})")
    
    # Final batch
    if batch:
        count = flush_assignee_batch(cur, batch)
        updated += count
        conn.commit()
    
    elapsed = time.time() - start_time
    log.info(f"Assignee load complete: {updated:,} updated, {skipped:,} skipped in {elapsed:.1f}s")
    
    cur.close()
    conn.close()
    return updated


def flush_assignee_batch(cur, batch):
    """Update assignee for a batch of patents."""
    # batch is list of (assignee, assignee_type, patent_id)
    query = """
        UPDATE patents 
        SET assignee = data.assignee, 
            assignee_type = data.assignee_type
        FROM (VALUES %s) AS data(assignee, assignee_type, patent_id)
        WHERE patents.patent_id = data.patent_id
    """
    execute_values(cur, query, batch, page_size=1000)
    return cur.rowcount


def load_full_metadata(filepath):
    """
    Load full patent metadata — INSERT new patents, UPDATE existing ones.
    Handles 130M+ patents from BigQuery.
    """
    log.info("=== Loading full patent metadata ===")
    
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    cur = conn.cursor()
    
    # Ensure columns exist
    cur.execute("""
        DO $$ BEGIN
            ALTER TABLE patents ADD COLUMN IF NOT EXISTS assignee_type TEXT;
            ALTER TABLE patents ADD COLUMN IF NOT EXISTS cpc_codes TEXT[];
            ALTER TABLE patents ADD COLUMN IF NOT EXISTS inventors TEXT[];
        EXCEPTION WHEN duplicate_column THEN NULL;
        END $$;
    """)
    conn.commit()
    
    batch_size = 2000
    batch = []
    inserted = 0
    updated = 0
    skipped = 0
    start_time = time.time()
    
    opener = gzip.open if str(filepath).endswith('.gz') else open
    
    with opener(filepath, 'rt', encoding='utf-8') as f:
        for line in f:
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                skipped += 1
                continue
            
            pub_num = record.get('publication_number', '')
            if not pub_num:
                skipped += 1
                continue
            
            title = (record.get('title') or '')[:2000]
            abstract = (record.get('abstract') or '')[:10000]
            primary_assignee = record.get('primary_assignee', '')
            assignee_type = record.get('assignee_type', '')
            country = record.get('country_code', '')
            filing_date = parse_bq_date(record.get('filing_date'))
            grant_date = parse_bq_date(record.get('grant_date'))
            ipc_codes = parse_array(record.get('ipc_codes', ''))
            cpc_codes = parse_array(record.get('cpc_codes', ''))
            inventors = parse_array(record.get('inventors', ''))
            
            batch.append((
                pub_num, title, abstract, primary_assignee, assignee_type,
                country, filing_date, grant_date, ipc_codes, cpc_codes, inventors
            ))
            
            if len(batch) >= batch_size:
                ins, upd = flush_full_batch(cur, batch)
                inserted += ins
                updated += upd
                conn.commit()
                batch = []
                
                total = inserted + updated
                if total % 50000 == 0:
                    elapsed = time.time() - start_time
                    rate = total / elapsed if elapsed > 0 else 0
                    log.info(f"  Processed {total:,} (ins={inserted:,} upd={updated:,}) at {rate:.0f}/sec")
    
    if batch:
        ins, upd = flush_full_batch(cur, batch)
        inserted += ins
        updated += upd
        conn.commit()
    
    elapsed = time.time() - start_time
    log.info(f"Full load complete: {inserted:,} inserted, {updated:,} updated in {elapsed:.1f}s")
    
    cur.close()
    conn.close()
    return inserted + updated


def flush_full_batch(cur, batch):
    """Upsert a batch of full patent records."""
    query = """
        INSERT INTO patents (patent_id, title, abstract, assignee, assignee_type,
                            jurisdiction, filing_date, grant_date, ipc_codes, cpc_codes, inventors)
        VALUES %s
        ON CONFLICT (patent_id) DO UPDATE SET
            assignee = COALESCE(EXCLUDED.assignee, patents.assignee),
            assignee_type = COALESCE(EXCLUDED.assignee_type, patents.assignee_type),
            ipc_codes = COALESCE(EXCLUDED.ipc_codes, patents.ipc_codes),
            cpc_codes = COALESCE(EXCLUDED.cpc_codes, patents.cpc_codes),
            inventors = COALESCE(EXCLUDED.inventors, patents.inventors),
            title = COALESCE(NULLIF(EXCLUDED.title, ''), patents.title),
            abstract = COALESCE(NULLIF(EXCLUDED.abstract, ''), patents.abstract)
    """
    template = "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    execute_values(cur, query, batch, template=template, page_size=500)
    
    # Approximate: all rows are either inserted or updated
    inserted = cur.rowcount
    return inserted, 0


def load_citations(filepath):
    """
    Load patent→paper citation links from BigQuery NPL extraction.
    Extracts DOIs from non-patent literature citations.
    """
    log.info("=== Loading patent→paper citations ===")
    
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    cur = conn.cursor()
    
    batch_size = 5000
    batch = []
    inserted = 0
    skipped = 0
    start_time = time.time()
    
    opener = gzip.open if str(filepath).endswith('.gz') else open
    
    with opener(filepath, 'rt', encoding='utf-8') as f:
        for line in f:
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                skipped += 1
                continue
            
            patent_id = record.get('citing_patent', '')
            doi = record.get('extracted_doi', '')
            npl_text = record.get('npl_citation_text', '')
            
            if not patent_id or (not doi and not npl_text):
                skipped += 1
                continue
            
            # Clean DOI — normalize to canonical lowercase form
            if doi:
                doi = doi.strip().rstrip('.').lower()
                if not doi.startswith('10.') or '/' not in doi:
                    doi = ''
            
            batch.append((patent_id, doi, npl_text[:2000] if npl_text else None))
            
            if len(batch) >= batch_size:
                count = flush_citation_batch(cur, batch)
                inserted += count
                conn.commit()
                batch = []
                
                if inserted % 100000 == 0:
                    elapsed = time.time() - start_time
                    rate = inserted / elapsed if elapsed > 0 else 0
                    log.info(f"  Inserted {inserted:,} citations ({rate:.0f}/sec, skipped {skipped:,})")
    
    if batch:
        count = flush_citation_batch(cur, batch)
        inserted += count
        conn.commit()
    
    elapsed = time.time() - start_time
    log.info(f"Citation load complete: {inserted:,} inserted, {skipped:,} skipped in {elapsed:.1f}s")
    
    cur.close()
    conn.close()
    return inserted


def flush_citation_batch(cur, batch):
    """Insert citation batch with conflict handling."""
    query = """
        INSERT INTO patent_paper_citations (patent_id, paper_doi, citation_context)
        VALUES %s
        ON CONFLICT (patent_id, paper_doi) DO NOTHING
    """
    # Filter out rows without DOIs for this table (we only store DOI-linked citations)
    doi_batch = [(pid, doi, ctx) for pid, doi, ctx in batch if doi]
    
    if not doi_batch:
        return 0
    
    execute_values(cur, query, doi_batch, page_size=1000)
    return cur.rowcount


def parse_bq_date(date_str):
    """Parse BigQuery date format (YYYYMMDD integer as string) to DATE."""
    if not date_str or date_str == '0' or date_str == 'None':
        return None
    try:
        date_str = str(date_str).strip()
        if len(date_str) == 8:
            return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
        return None
    except (ValueError, TypeError):
        return None


def parse_array(pipe_separated):
    """Parse pipe-separated string into PostgreSQL array."""
    if not pipe_separated or pipe_separated == 'None':
        return None
    items = [x.strip() for x in str(pipe_separated).split(' | ') if x.strip()]
    return items if items else None


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Load BigQuery patent exports into DB")
    parser.add_argument("--phase", choices=["assignees", "full", "citations"],
                       required=True, help="Which phase to load")
    parser.add_argument("--file", type=str, help="Direct file path (skip S3 download)")
    args = parser.parse_args()
    
    # Get the data file
    if args.file:
        filepath = Path(args.file)
        if not filepath.exists():
            log.error(f"File not found: {filepath}")
            sys.exit(1)
    else:
        filepath = download_from_s3(args.phase)
        if not filepath:
            log.error("Failed to download export file")
            sys.exit(1)
    
    log.info(f"Loading file: {filepath}")
    
    # Run the appropriate loader
    if args.phase == "assignees":
        load_assignees(filepath)
    elif args.phase == "full":
        load_full_metadata(filepath)
    elif args.phase == "citations":
        load_citations(filepath)
    
    log.info("Done!")


if __name__ == "__main__":
    main()
