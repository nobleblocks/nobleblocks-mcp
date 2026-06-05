#!/usr/bin/env python3
"""
BigQuery Patent Export Pipeline
================================
Queries Google's patents-public-data.patents.publications for:
1. Non-patent literature (NPL) citations → patent↔paper links (HIGH VALUE)
2. Patent metadata (title, abstract, assignees, IPC/CPC, dates)
3. Claims text (expensive — only with billing enabled)

Prerequisites:
- pip install google-cloud-bigquery psycopg2-binary
- gcloud auth application-default login (run once in terminal)
- Project: gen-lang-client-0004533848

Usage:
  python3 ingest_bigquery_patents.py --phase npl            # NPL citations only (~50GB scan)
  python3 ingest_bigquery_patents.py --phase metadata       # Patent metadata (~200GB/country)
  python3 ingest_bigquery_patents.py --phase claims         # Claims text (500GB+ — needs billing)
  python3 ingest_bigquery_patents.py --phase resolve        # Resolve DOIs to paper IDs
  python3 ingest_bigquery_patents.py --phase all            # Everything
  python3 ingest_bigquery_patents.py --dry-run              # Test auth + sample query

Data budget: ~1TB/month free (sandbox). Strategy:
- NPL citations query scans ~50GB total (just citation array + DOI filter)
- Metadata scans ~80GB per major country (US/EP/WO)
- Run NPL first (highest value for VC intelligence)

Sandbox mode (no billing): CAN query public datasets. Cannot export to GCS.
We stream results directly to PostgreSQL — no GCS needed.
"""

import os
import sys
import json
import re
import time
import argparse
import logging
from datetime import datetime

import psycopg2
from psycopg2.extras import execute_values

# Try to import BigQuery
try:
    from google.cloud import bigquery
    HAS_BIGQUERY = True
except ImportError:
    HAS_BIGQUERY = False

# --- Configuration ---
GCP_PROJECT = os.environ.get("GCP_PROJECT", "gen-lang-client-0004533848")

DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = int(os.environ.get("DB_PORT", "5432"))
DB_NAME = os.environ.get("DB_NAME", "paper_search")
DB_USER = os.environ.get("DB_USER", "nobleblocks")
DB_PASS = os.environ.get("DB_PASS", "nb_papers_2026_prod")

BATCH_SIZE = 5000
PROGRESS_FILE = "/tmp/bigquery_patent_progress.json"

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# DOI extraction
DOI_PATTERN = re.compile(r'(10\.\d{4,9}/[^\s,;"\'\]>]+)', re.IGNORECASE)


def get_db_conn():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS,
        options="-c statement_timeout=600000",  # 10 min for large UPDATEs
    )


def get_bq_client():
    """Get authenticated BigQuery client."""
    if not HAS_BIGQUERY:
        log.error("google-cloud-bigquery not installed! Run: pip install google-cloud-bigquery")
        sys.exit(1)
    return bigquery.Client(project=GCP_PROJECT)


def load_progress():
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE) as f:
            return json.load(f)
    return {
        "npl_countries_done": [],
        "npl_total_inserted": 0,
        "metadata_countries_done": [],
        "metadata_total_inserted": 0,
        "last_updated": None,
    }


def save_progress(progress):
    progress["last_updated"] = datetime.utcnow().isoformat()
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f, indent=2)


def extract_doi_from_npl(npl_text: str):
    """Extract DOI from non-patent literature citation text."""
    if not npl_text:
        return None
    # Try explicit doi: prefix first
    m = re.search(r'doi[:\s]+([^\s,;"\'>]+)', npl_text, re.IGNORECASE)
    if m:
        doi = m.group(1).rstrip('.)')
        if doi.startswith("10."):
            return doi
    # General DOI pattern
    m = DOI_PATTERN.search(npl_text)
    if m:
        doi = m.group(1).rstrip('.)')
        return doi
    return None


# ═══════════════════════════════════════════════════════════════
# Phase 1: NPL Citations (THE HIGH-VALUE DATA)
# ═══════════════════════════════════════════════════════════════

def ingest_npl_citations(client, conn, progress, countries=None):
    """
    Extract non-patent literature citations from BigQuery.
    These are academic papers cited by patents — the core VC intelligence signal.

    Strategy: Download to local CSV first (faster than row-by-row iteration
    through SSM tunnel), then bulk-load to DB.
    
    US alone has ~892K NPL citations with DOIs. Streams results to CSV
    then bulk loads to DB via COPY-style batch inserts.
    """
    import csv
    
    target_countries = countries or ["US", "EP", "WO", "CN", "JP", "KR", "DE", "GB", "FR", "CA", "AU", "IN"]
    done_countries = set(progress.get("npl_countries_done", []))

    for country in target_countries:
        if country in done_countries:
            log.info(f"  {country}: already done, skipping")
            continue

        log.info(f"  Querying NPL citations for country={country}...")

        query = f"""
        SELECT
            pub.publication_number AS patent_id,
            cit.npl_text,
            cit.category AS citation_category
        FROM
            `patents-public-data.patents.publications` pub,
            UNNEST(pub.citation) AS cit
        WHERE
            pub.country_code = '{country}'
            AND cit.npl_text IS NOT NULL
            AND LENGTH(cit.npl_text) > 20
            AND REGEXP_CONTAINS(cit.npl_text, r'10\\.\\d{{4,9}}/')
        """

        try:
            start_t = time.time()
            query_job = client.query(query)
            
            log.info(f"  {country}: Downloading results...")
            results = query_job.result()
            
            bytes_processed = query_job.total_bytes_processed or 0
            log.info(f"  {country}: Query scanned {bytes_processed / 1e9:.1f} GB")

            # Stream results to local CSV (extract DOIs on the fly)
            csv_path = f"/tmp/bigquery_npl_{country}.csv"
            total_rows = 0
            total_with_doi = 0
            
            with open(csv_path, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["patent_id", "paper_doi", "citation_context", "citation_type", "source"])
                
                for row in results:
                    total_rows += 1
                    doi = extract_doi_from_npl(row.npl_text)
                    if not doi:
                        continue
                    total_with_doi += 1
                    writer.writerow([
                        row.patent_id,
                        doi,
                        (row.npl_text or "")[:500],
                        "npl",
                        "bigquery",
                    ])
                    
                    if total_rows % 50000 == 0:
                        elapsed = time.time() - start_t
                        rate = total_rows / elapsed if elapsed > 0 else 0
                        log.info(f"    {country}: {total_rows:,} rows ({total_with_doi:,} DOIs) "
                                 f"@ {rate:.0f} rows/s...")

            download_elapsed = time.time() - start_t
            log.info(f"  {country}: Download complete — {total_rows:,} rows, "
                     f"{total_with_doi:,} with DOIs ({download_elapsed:.0f}s)")

            if total_with_doi == 0:
                log.info(f"  {country}: No DOIs found, skipping DB load")
                progress["npl_countries_done"].append(country)
                save_progress(progress)
                continue

            # Phase 2: Bulk load CSV into database
            log.info(f"  {country}: Loading {total_with_doi:,} citations into DB...")
            loaded = _bulk_load_csv(conn, csv_path)
            
            progress["npl_total_inserted"] += loaded
            progress["npl_countries_done"].append(country)
            save_progress(progress)
            
            total_elapsed = time.time() - start_t
            log.info(f"  {country}: Done — {loaded:,} new citations loaded ({total_elapsed:.0f}s total)")

            # Cleanup CSV
            os.remove(csv_path)

        except Exception as e:
            err_str = str(e).lower()
            log.error(f"  {country}: Failed — {e}")
            if "billing" in err_str or "quota" in err_str or "exceed" in err_str:
                log.error("BILLING/QUOTA ERROR — stopping.")
                return
            if "access denied" in err_str or "permission" in err_str:
                log.error("AUTH ERROR — run: gcloud auth application-default login")
                return
            continue

    log.info(f"NPL phase complete. Total inserted: {progress['npl_total_inserted']:,}")


def _bulk_load_csv(conn, csv_path):
    """Bulk load citations from CSV into patent_paper_citations."""
    import csv
    
    loaded = 0
    batch = []
    
    with open(csv_path) as f:
        reader = csv.reader(f)
        next(reader)  # skip header
        
        for row in reader:
            patent_id, paper_doi, citation_context, citation_type, source = row
            batch.append((patent_id, paper_doi, citation_context, citation_type, source))
            
            if len(batch) >= BATCH_SIZE:
                _insert_citations_batch(conn, batch)
                loaded += len(batch)
                batch = []
                if loaded % 50000 == 0:
                    log.info(f"    Loaded {loaded:,}...")
    
    if batch:
        _insert_citations_batch(conn, batch)
        loaded += len(batch)
    
    return loaded


def _insert_citations_batch(conn, batch):
    """Insert patent→paper citation links, skip duplicates.
    Unique index: (patent_id, COALESCE(paper_doi,''), COALESCE(paper_openalex_id,''))
    """
    with conn.cursor() as cur:
        execute_values(
            cur,
            """INSERT INTO patent_paper_citations
               (patent_id, paper_doi, citation_context, citation_type, source)
               VALUES %s
               ON CONFLICT (patent_id, COALESCE(paper_doi, ''::text), COALESCE(paper_openalex_id, ''::text))
               DO NOTHING""",
            batch,
            page_size=1000,
        )
    conn.commit()


# ═══════════════════════════════════════════════════════════════
# Phase 2: Patent Metadata
# ═══════════════════════════════════════════════════════════════

def ingest_patent_metadata(client, conn, progress, countries=None):
    """
    Import patent metadata: title, abstract, assignees, IPC/CPC, dates.
    Enriches existing patents (from PatentsView) and adds new ones.

    US alone is ~80GB to scan. Process one country at a time.
    Only imports patents that have NPL citations (i.e., cite academic papers).
    """
    target_countries = countries or ["US", "EP", "WO"]
    done_countries = set(progress.get("metadata_countries_done", []))

    for country in target_countries:
        if country in done_countries:
            log.info(f"  {country}: metadata already done, skipping")
            continue

        log.info(f"  Querying patent metadata for country={country}...")
        log.info(f"  (Only patents that cite academic papers — limits scan)")

        # Only fetch metadata for patents we have citations for (reduces scan dramatically)
        # First, get the list of patent_ids we already have from NPL phase
        query = f"""
        SELECT
            pub.publication_number AS patent_id,
            pub.title_localized[SAFE_OFFSET(0)].text AS title,
            pub.abstract_localized[SAFE_OFFSET(0)].text AS abstract,
            pub.filing_date AS filing_date_int,
            pub.grant_date AS grant_date_int,
            pub.assignee_harmonized[SAFE_OFFSET(0)].name AS assignee,
            ARRAY_TO_STRING(
                ARRAY(SELECT name FROM UNNEST(pub.inventor_harmonized)), '|'
            ) AS inventors_str,
            ARRAY_TO_STRING(
                ARRAY(SELECT code FROM UNNEST(pub.ipc) LIMIT 20), ','
            ) AS ipc_codes_str,
            ARRAY_TO_STRING(
                ARRAY(SELECT code FROM UNNEST(pub.cpc) LIMIT 20), ','
            ) AS cpc_codes_str,
            CAST(pub.family_id AS STRING) AS family_id
        FROM
            `patents-public-data.patents.publications` pub
        WHERE
            pub.country_code = '{country}'
            AND pub.grant_date > 0
            AND EXISTS (
                SELECT 1 FROM UNNEST(pub.citation) cit
                WHERE cit.npl_text IS NOT NULL AND LENGTH(cit.npl_text) > 20
            )
        """

        try:
            start_t = time.time()
            query_job = client.query(query)
            results = query_job.result()

            bytes_processed = query_job.total_bytes_processed or 0
            elapsed = time.time() - start_t
            log.info(f"  {country}: Query done in {elapsed:.0f}s, scanned {bytes_processed / 1e9:.1f} GB")

            batch = []
            total_this_country = 0

            for row in results:
                filing_date = _parse_bq_date(row.filing_date_int)
                grant_date = _parse_bq_date(row.grant_date_int)
                ipc_codes = [c.strip() for c in row.ipc_codes_str.split(",") if c.strip()] if row.ipc_codes_str else None
                cpc_codes = [c.strip() for c in row.cpc_codes_str.split(",") if c.strip()] if row.cpc_codes_str else None
                inventors = [i.strip() for i in row.inventors_str.split("|") if i.strip()] if row.inventors_str else None

                batch.append((
                    row.patent_id,
                    row.title,
                    row.abstract,
                    None,           # claims_text (expensive, Phase 3)
                    filing_date,
                    grant_date,
                    row.assignee,
                    None,           # assignee_type
                    inventors,
                    ipc_codes,
                    cpc_codes,
                    country,
                    None,           # legal_status
                    row.family_id,
                    "bigquery",
                ))

                if len(batch) >= BATCH_SIZE:
                    _insert_patents_batch(conn, batch)
                    total_this_country += len(batch)
                    progress["metadata_total_inserted"] += len(batch)
                    batch = []
                    if total_this_country % 100000 == 0:
                        log.info(f"    {country}: {total_this_country:,} patents inserted...")
                        save_progress(progress)

            if batch:
                _insert_patents_batch(conn, batch)
                total_this_country += len(batch)
                progress["metadata_total_inserted"] += len(batch)

            log.info(f"  {country}: Done — {total_this_country:,} patents")
            progress["metadata_countries_done"].append(country)
            save_progress(progress)

        except Exception as e:
            err_str = str(e).lower()
            log.error(f"  {country}: Query failed — {e}")
            if "billing" in err_str or "quota" in err_str:
                log.error("BILLING/QUOTA ERROR — stopping.")
                return
            continue

    log.info(f"Metadata phase complete. Total: {progress['metadata_total_inserted']:,}")


def _parse_bq_date(date_int):
    """Parse BigQuery integer date (YYYYMMDD) to ISO date string."""
    if not date_int or date_int <= 0:
        return None
    try:
        s = str(int(date_int))
        if len(s) == 8:
            return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    except (ValueError, TypeError):
        pass
    return None


def _insert_patents_batch(conn, batch):
    """Insert/update patent metadata."""
    with conn.cursor() as cur:
        execute_values(
            cur,
            """INSERT INTO patents
               (patent_id, title, abstract, claims_text, filing_date, grant_date,
                assignee, assignee_type, inventors, ipc_codes, cpc_codes,
                jurisdiction, legal_status, patent_family_id, source)
               VALUES %s
               ON CONFLICT (patent_id) DO UPDATE SET
                   title = COALESCE(EXCLUDED.title, patents.title),
                   abstract = COALESCE(EXCLUDED.abstract, patents.abstract),
                   filing_date = COALESCE(EXCLUDED.filing_date, patents.filing_date),
                   grant_date = COALESCE(EXCLUDED.grant_date, patents.grant_date),
                   assignee = COALESCE(EXCLUDED.assignee, patents.assignee),
                   inventors = COALESCE(EXCLUDED.inventors, patents.inventors),
                   ipc_codes = COALESCE(EXCLUDED.ipc_codes, patents.ipc_codes),
                   cpc_codes = COALESCE(EXCLUDED.cpc_codes, patents.cpc_codes),
                   patent_family_id = COALESCE(EXCLUDED.patent_family_id, patents.patent_family_id),
                   source = CASE WHEN patents.source = 'patentsview'
                            THEN 'bigquery' ELSE patents.source END,
                   updated_at = NOW()""",
            batch,
            template="(%s, %s, %s, %s, %s::date, %s::date, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            page_size=1000,
        )
    conn.commit()


# ═══════════════════════════════════════════════════════════════
# Phase 3: Claims Text (EXPENSIVE — requires billing)
# ═══════════════════════════════════════════════════════════════

def ingest_claims_text(client, conn, country="US", year_start=2020):
    """
    Import full claims text for recent patents.
    WARNING: Scans ~500GB+ for all US patents. Only run with billing enabled.
    Partitions by filing year to control costs.
    """
    log.warning("Claims text ingestion scans 500GB+. Requires billing account.")

    for year in range(year_start, 2027):
        log.info(f"  Querying claims for {country} patents filed in {year}...")

        query = f"""
        SELECT
            pub.publication_number AS patent_id,
            pub.claims_localized[SAFE_OFFSET(0)].text AS claims_text
        FROM
            `patents-public-data.patents.publications` pub
        WHERE
            pub.country_code = '{country}'
            AND CAST(FLOOR(pub.filing_date / 10000) AS INT64) = {year}
            AND ARRAY_LENGTH(pub.claims_localized) > 0
        """

        try:
            query_job = client.query(query)
            results = query_job.result()

            bytes_proc = query_job.total_bytes_processed or 0
            log.info(f"  {year}: Scanned {bytes_proc / 1e9:.1f} GB")

            count = 0
            with conn.cursor() as cur:
                for row in results:
                    if row.claims_text:
                        cur.execute(
                            """UPDATE patents SET claims_text = %s, updated_at = NOW()
                               WHERE patent_id = %s AND claims_text IS NULL""",
                            (row.claims_text[:50000], row.patent_id),
                        )
                        count += 1
                        if count % 10000 == 0:
                            conn.commit()
                            log.info(f"    {year}: {count:,} claims updated...")
                conn.commit()

            log.info(f"  {year}: Done — {count:,} claims")

        except Exception as e:
            log.error(f"  {year}: Failed — {e}")
            if "billing" in str(e).lower():
                log.error("Need billing enabled. Stop.")
                return


# ═══════════════════════════════════════════════════════════════
# DOI Resolution
# ═══════════════════════════════════════════════════════════════

def resolve_dois(conn):
    """Resolve paper_doi → paper_id for BigQuery citations."""
    log.info("Resolving BigQuery citation DOIs to paper IDs...")

    with conn.cursor() as cur:
        cur.execute("""
            UPDATE patent_paper_citations ppc
            SET paper_id = p.id
            FROM papers p
            WHERE ppc.paper_doi = p.doi
              AND ppc.paper_id IS NULL
              AND ppc.source = 'bigquery'
        """)
        resolved = cur.rowcount
    conn.commit()

    log.info(f"Resolved {resolved:,} BigQuery citations to paper IDs")
    return resolved


# ═══════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="BigQuery Patent Export Pipeline")
    parser.add_argument("--phase", choices=["npl", "metadata", "claims", "resolve", "all"],
                        default="npl", help="Which phase to run (default: npl)")
    parser.add_argument("--country", type=str, help="Single country code (e.g., US, EP, WO)")
    parser.add_argument("--dry-run", action="store_true", help="Test auth + sample query only")
    args = parser.parse_args()

    progress = load_progress()
    countries = [args.country] if args.country else None

    log.info("=" * 60)
    log.info("  BigQuery Patent Export Pipeline")
    log.info(f"  Project: {GCP_PROJECT}")
    log.info(f"  Phase: {args.phase}")
    log.info(f"  BigQuery SDK: {'OK' if HAS_BIGQUERY else 'NOT INSTALLED'}")
    log.info("=" * 60)

    # Auth check
    try:
        client = get_bq_client()
        test = client.query("SELECT 1 AS ok").result()
        for row in test:
            assert row.ok == 1
        log.info("  BigQuery: authenticated OK")
    except Exception as e:
        log.error(f"  BigQuery auth FAILED: {e}")
        log.error("  Fix: run in terminal → gcloud auth application-default login")
        sys.exit(1)

    if args.dry_run:
        log.info("  Running test query...")
        q = """
        SELECT COUNT(*) AS total
        FROM `patents-public-data.patents.publications`
        WHERE country_code = 'US' AND grant_date > 20200101
        """
        result = client.query(q).result()
        for row in result:
            log.info(f"  US patents granted since 2020: {row.total:,}")
        bytes_proc = client.query(q).total_bytes_processed
        log.info(f"  Test scan: {(bytes_proc or 0) / 1e9:.2f} GB")
        log.info("  Dry run complete — auth works, ready to ingest.")
        return

    # Database
    conn = get_db_conn()
    log.info(f"  Database: {DB_HOST}:{DB_PORT}/{DB_NAME} OK")

    try:
        if args.phase in ("npl", "all"):
            log.info("\n--- Phase 1: NPL Citations (patent→paper links) ---")
            ingest_npl_citations(client, conn, progress, countries)

        if args.phase in ("metadata", "all"):
            log.info("\n--- Phase 2: Patent Metadata ---")
            ingest_patent_metadata(client, conn, progress, countries)

        if args.phase == "claims":
            log.info("\n--- Phase 3: Claims Text (requires billing) ---")
            country = args.country or "US"
            ingest_claims_text(client, conn, country=country)

        if args.phase in ("resolve", "all"):
            log.info("\n--- DOI Resolution ---")
            resolve_dois(conn)

    finally:
        conn.close()
        save_progress(progress)

    log.info("\n" + "=" * 60)
    log.info(f"  NPL citations inserted: {progress['npl_total_inserted']:,}")
    log.info(f"  Patents inserted: {progress['metadata_total_inserted']:,}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
