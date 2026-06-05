#!/usr/bin/env python3
"""
BigQuery Patent Export — Google Patents Public Data

Exports patent data from Google's patents-public-data BigQuery dataset.
This contains 130M+ patents with full text, claims, citations, and metadata.

Prerequisites:
1. Google Cloud project with billing enabled (free tier = 1TB/mo queries)
2. `gcloud` CLI installed and authenticated
3. pip install google-cloud-bigquery google-cloud-storage

This script:
1. Queries patents-public-data for patent→paper citation links
2. Exports results to GCS (or local CSV)
3. Bulk loads into Paper DB

The key table: `patents-public-data.patents.publications`
Citation table: `patents-public-data.patents.publications_202401` (latest snapshot)
"""

import os
import sys
import json
import csv
import re
import time
import psycopg2
import psycopg2.extras
from datetime import datetime

# Try to import BigQuery — may not be installed yet
try:
    from google.cloud import bigquery
    HAS_BIGQUERY = True
except ImportError:
    HAS_BIGQUERY = False

DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_NAME = os.environ.get("DB_NAME", "paper_search")
DB_USER = os.environ.get("DB_USER", "nobleblocks")
DB_PASS = os.environ.get("DB_PASS", "")

GCP_PROJECT = os.environ.get("GCP_PROJECT", "")
EXPORT_DIR = "/tmp/bigquery_patent_export"
BATCH_SIZE = 5000

DOI_PATTERN = re.compile(r'10\.\d{4,9}/[^\s,;"\'>]+')

# Queries to extract the most valuable data
QUERIES = {
    # Patent→paper citation links (the KEY data for VC intelligence)
    "patent_paper_citations": """
        SELECT
            p.publication_number as patent_id,
            p.country_code as jurisdiction,
            p.title_localized[SAFE_OFFSET(0)].text as title,
            p.abstract_localized[SAFE_OFFSET(0)].text as abstract,
            p.filing_date,
            p.grant_date,
            p.assignee_harmonized[SAFE_OFFSET(0)].name as assignee,
            npl.text as npl_citation_text,
            npl.npl_text as npl_full_text
        FROM `patents-public-data.patents.publications` p,
            UNNEST(p.citation) as citation
            LEFT JOIN UNNEST(citation.npl_text) as npl
        WHERE citation.type = 'NON_PATENT_LITERATURE'
            AND p.grant_date >= 20200101
            AND npl.text IS NOT NULL
        LIMIT 10000000
    """,

    # Recent patents with most NPL citations (high academic connection)
    "high_academic_patents": """
        SELECT
            p.publication_number as patent_id,
            p.country_code as jurisdiction,
            p.title_localized[SAFE_OFFSET(0)].text as title,
            p.filing_date,
            p.grant_date,
            p.assignee_harmonized[SAFE_OFFSET(0)].name as assignee,
            ARRAY_LENGTH(p.citation) as citation_count,
            (SELECT COUNT(*) FROM UNNEST(p.citation) c WHERE c.type = 'NON_PATENT_LITERATURE') as npl_count
        FROM `patents-public-data.patents.publications` p
        WHERE p.grant_date >= 20230101
            AND (SELECT COUNT(*) FROM UNNEST(p.citation) c WHERE c.type = 'NON_PATENT_LITERATURE') > 5
        ORDER BY npl_count DESC
        LIMIT 1000000
    """,

    # CPC/IPC classification for technology domain mapping
    "patent_classifications": """
        SELECT
            p.publication_number as patent_id,
            cpc.code as cpc_code,
            cpc.inventive as is_inventive
        FROM `patents-public-data.patents.publications` p,
            UNNEST(p.cpc) as cpc
        WHERE p.grant_date >= 20200101
        LIMIT 50000000
    """,
}


def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS
    )


def run_bigquery_export(query_name, query_sql):
    """Run a BigQuery query and export results."""
    if not HAS_BIGQUERY:
        print("  ❌ google-cloud-bigquery not installed!")
        print("  Run: pip install google-cloud-bigquery google-cloud-storage")
        return None

    if not GCP_PROJECT:
        print("  ❌ GCP_PROJECT environment variable not set!")
        return None

    client = bigquery.Client(project=GCP_PROJECT)
    print(f"  Running query: {query_name}...")
    print(f"  (This may take 1-5 minutes depending on data volume)")

    job_config = bigquery.QueryJobConfig(
        destination=None,  # Write to temp table
        use_legacy_sql=False,
    )

    query_job = client.query(query_sql, job_config=job_config)

    # Wait for completion
    results = query_job.result()
    total_rows = query_job.total_bytes_processed
    print(f"  ✓ Query complete: {results.total_rows:,} rows, "
          f"{total_rows/1024/1024/1024:.2f} GB processed")

    # Export to local CSV
    os.makedirs(EXPORT_DIR, exist_ok=True)
    output_file = os.path.join(EXPORT_DIR, f"{query_name}.csv")

    print(f"  Exporting to {output_file}...")
    with open(output_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([field.name for field in results.schema])
        for row in results:
            writer.writerow(list(row.values()))

    print(f"  ✓ Exported {results.total_rows:,} rows")
    return output_file


def load_citations_csv(filepath, conn):
    """Load patent→paper citations from BigQuery CSV export."""
    if not filepath or not os.path.exists(filepath):
        return 0

    print(f"  Loading citations from {filepath}...")
    total_loaded = 0
    batch = []

    with open(filepath) as f:
        reader = csv.DictReader(f)
        for row in reader:
            npl_text = row.get("npl_citation_text") or row.get("npl_full_text") or ""

            # Extract DOI from NPL text
            doi_match = DOI_PATTERN.search(npl_text)
            paper_doi = doi_match.group(0).rstrip(".),") if doi_match else None

            if not paper_doi:
                continue  # Skip citations without DOIs

            batch.append({
                "patent_id": row.get("patent_id"),
                "paper_doi": paper_doi,
                "citation_context": npl_text[:500],
                "citation_type": "npl",
                "source": "bigquery",
                "jurisdiction": row.get("jurisdiction"),
                "title": row.get("title"),
                "assignee": row.get("assignee"),
                "filing_date": row.get("filing_date"),
                "grant_date": row.get("grant_date"),
            })

            if len(batch) >= BATCH_SIZE:
                loaded = insert_batch(conn, batch)
                total_loaded += loaded
                batch = []

                if total_loaded % 50000 == 0:
                    print(f"    Loaded {total_loaded:,} citation links...")

    # Flush
    if batch:
        loaded = insert_batch(conn, batch)
        total_loaded += loaded

    return total_loaded


def insert_batch(conn, batch):
    """Insert a batch of citations + ensure patent records exist."""
    if not batch:
        return 0

    cur = conn.cursor()

    # Ensure patents exist
    patent_sql = """
        INSERT INTO patents (patent_id, title, assignee, filing_date, grant_date, jurisdiction, source)
        VALUES %s
        ON CONFLICT (patent_id) DO NOTHING
    """
    patent_values = list(set(
        (b["patent_id"], b.get("title"), b.get("assignee"),
         b.get("filing_date"), b.get("grant_date"),
         b.get("jurisdiction"), "bigquery")
        for b in batch if b.get("patent_id")
    ))
    if patent_values:
        psycopg2.extras.execute_values(cur, patent_sql, patent_values,
            template="(%s, %s, %s, %s, %s, %s, %s)")

    # Insert citation links
    citation_sql = """
        INSERT INTO patent_paper_citations (patent_id, paper_doi, citation_context, citation_type, source)
        VALUES %s
        ON CONFLICT (patent_id, COALESCE(paper_doi, ''), COALESCE(paper_openalex_id, ''))
        DO NOTHING
    """
    citation_values = [
        (b["patent_id"], b["paper_doi"], b.get("citation_context"),
         b["citation_type"], b["source"])
        for b in batch
    ]
    psycopg2.extras.execute_values(cur, citation_sql, citation_values,
        template="(%s, %s, %s, %s, %s)")
    inserted = cur.rowcount
    conn.commit()
    cur.close()
    return inserted


def alternative_bulk_download():
    """
    Alternative: Download pre-exported patent data from Google Cloud Storage.
    The patents-public-data bucket has bulk exports available.

    This doesn't require BigQuery access — just gsutil/gcloud.
    """
    print("  Alternative: Using bulk export from GCS...")
    print("  Command: gsutil -m cp gs://patents-public-data/citations/*.jsonl.gz /tmp/patent_citations/")
    print("  This requires gcloud CLI. Running...")

    import subprocess
    os.makedirs("/tmp/patent_citations", exist_ok=True)

    # Try to download a sample file first
    result = subprocess.run(
        ["gsutil", "ls", "gs://patents-public-data/"],
        capture_output=True, text=True
    )

    if result.returncode != 0:
        print("  ❌ gsutil not available. Install: pip install gsutil")
        print("  Or: curl https://sdk.cloud.google.com | bash")
        return False

    print(f"  Available: {result.stdout[:500]}")
    return True


def main():
    print("=" * 60)
    print("  Google Patents BigQuery Export Pipeline")
    print("=" * 60)
    print(f"  DB: {DB_HOST}:{DB_PORT}/{DB_NAME}")
    print(f"  GCP Project: {GCP_PROJECT or '⚠ NOT SET'}")
    print(f"  BigQuery SDK: {'✓' if HAS_BIGQUERY else '❌ NOT INSTALLED'}")
    print()

    conn = get_db_connection()

    if HAS_BIGQUERY and GCP_PROJECT:
        # Full BigQuery pipeline
        for query_name, query_sql in QUERIES.items():
            if query_name == "patent_paper_citations":
                filepath = run_bigquery_export(query_name, query_sql)
                if filepath:
                    loaded = load_citations_csv(filepath, conn)
                    print(f"  ✓ Loaded {loaded:,} patent→paper citation links from BigQuery")
    else:
        print("  ⚠ BigQuery not available. Checking alternatives...")
        print()
        print("  To use BigQuery (FREE, 1TB/month):")
        print("  1. Create Google Cloud project: https://console.cloud.google.com")
        print("  2. Enable BigQuery API")
        print("  3. pip install google-cloud-bigquery")
        print("  4. gcloud auth application-default login")
        print("  5. Set GCP_PROJECT=your-project-id")
        print()
        print("  The query will scan ~50GB (well within free tier)")
        print()

        # Try GCS bulk download as alternative
        alternative_bulk_download()

    # Resolve DOIs to paper records regardless of source
    print("\n  Resolving DOIs to paper records...")
    cur = conn.cursor()
    cur.execute("""
        UPDATE patent_paper_citations ppc
        SET paper_id = p.id, paper_title = p.title
        FROM papers p
        WHERE ppc.paper_doi = p.doi
        AND ppc.paper_id IS NULL
        AND ppc.source = 'bigquery'
    """)
    resolved = cur.rowcount
    conn.commit()
    cur.close()
    print(f"  ✓ Resolved {resolved:,} DOI→paper links")

    conn.close()
    print("\n  Done!")


if __name__ == "__main__":
    main()
