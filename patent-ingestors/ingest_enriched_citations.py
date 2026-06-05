#!/usr/bin/env python3
"""
USPTO Enriched Citations Ingestor — Office Action Citation Context

Ingests the `enriched_cited_reference_metadata` dataset from the USPTO Developer
Hub API (developer.uspto.gov). This gives us RICH citation context that PatentsView
doesn't provide:
  - Which specific claims in the patent application the citation relates to
  - Exact passage locations in cited documents (paragraphs, columns, lines, figures)
  - Whether NPL (non-patent literature = academic paper) was used in the rejection
  - Examiner vs. applicant cited flags
  - Office action category (final/non-final rejection)
  - Citation category codes (X=novelty destroying, Y=inventive step, A=background)

Data flows into patent_paper_citations and a new enriched_citation_context table.

API: POST https://developer.uspto.gov/ds-api/enriched_cited_reference_metadata/v3/records
Format: form-data with Lucene query syntax
Total records: ~44M (170K+ are NPL)

WARNING: Legacy Developer Hub being decommissioned May 29, 2026. After that,
use data.uspto.gov/apis/enriched-citations (requires ODP login).

Run on paper-db server:
  DB_PASS=nb_papers_2026_prod python3 ingest_enriched_citations.py
"""

import gzip
import io
import json
import os
import re
import signal
import sys
import time
from datetime import datetime

import psycopg2
import psycopg2.extras
import requests

# Config
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_NAME = os.environ.get("DB_NAME", "paper_search")
DB_USER = os.environ.get("DB_USER", "nobleblocks")
DB_PASS = os.environ.get("DB_PASS", "nb_papers_2026_prod")

API_BASE = "https://developer.uspto.gov/ds-api/enriched_cited_reference_metadata/v3/records"
BATCH_SIZE = 1000  # API allows up to 1000 per request
PAGE_SIZE = 1000
REQUEST_DELAY = 0.35  # ~3 req/s
PROGRESS_FILE = "/tmp/enriched_citations_progress.json"
DB_BATCH_SIZE = 1000

# Graceful shutdown
shutdown_requested = False


def handle_signal(sig, frame):
    global shutdown_requested
    shutdown_requested = True
    print("\n⚠ Shutdown requested, finishing current batch...")


signal.signal(signal.SIGTERM, handle_signal)
signal.signal(signal.SIGINT, handle_signal)


def load_progress():
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE) as f:
            return json.load(f)
    return {
        "schema_created": False,
        "npl_offset": 0,
        "patent_offset": 0,
        "total_npl_ingested": 0,
        "total_patent_ingested": 0,
        "last_updated": None,
    }


def save_progress(progress):
    progress["last_updated"] = datetime.utcnow().isoformat()
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f)


def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS
    )


def create_schema(conn):
    """Create the enriched_citation_context table for passage-level data."""
    cur = conn.cursor()
    # Drop old broken schema if exists (had wrong unique constraint)
    cur.execute("DROP TABLE IF EXISTS enriched_citation_context CASCADE")
    cur.execute("""
        CREATE TABLE enriched_citation_context (
            id VARCHAR(40) PRIMARY KEY,
            patent_application_number VARCHAR(20) NOT NULL,
            patent_id VARCHAR(30),
            publication_number VARCHAR(20),
            cited_document_identifier TEXT,
            inventor_name TEXT,
            country_code VARCHAR(5),
            kind_code VARCHAR(5),
            npl_indicator BOOLEAN DEFAULT FALSE,
            office_action_date DATE,
            office_action_category VARCHAR(10),
            citation_category_code VARCHAR(5),
            related_claims TEXT,
            passage_locations TEXT[],
            examiner_cited BOOLEAN DEFAULT FALSE,
            applicant_cited BOOLEAN DEFAULT FALSE,
            tech_center VARCHAR(10),
            work_group VARCHAR(10),
            group_art_unit VARCHAR(10),
            quality_summary TEXT,
            source VARCHAR(50) DEFAULT 'uspto_enriched',
            created_at TIMESTAMP DEFAULT NOW()
        );
        
        CREATE INDEX idx_ecc_patent_app ON enriched_citation_context(patent_application_number);
        CREATE INDEX idx_ecc_npl ON enriched_citation_context(npl_indicator) WHERE npl_indicator = TRUE;
        CREATE INDEX idx_ecc_pub_num ON enriched_citation_context(publication_number) WHERE publication_number != '';
        CREATE INDEX idx_ecc_office_action_date ON enriched_citation_context(office_action_date);
        CREATE INDEX idx_ecc_category ON enriched_citation_context(citation_category_code);
        CREATE INDEX idx_ecc_examiner ON enriched_citation_context(examiner_cited) WHERE examiner_cited = TRUE;
    """)
    conn.commit()
    cur.close()
    print("  ✓ enriched_citation_context table ready")


def fetch_page(criteria, start, rows=PAGE_SIZE, max_retries=3):
    """Fetch a page from the Enriched Citations API."""
    for attempt in range(max_retries):
        try:
            resp = requests.post(
                API_BASE,
                data={"criteria": criteria, "start": start, "rows": rows},
                headers={"Accept-Encoding": "gzip"},
                timeout=60,
            )
            resp.raise_for_status()

            # Response is always gzip-compressed regardless of header
            try:
                content = gzip.decompress(resp.content)
            except (gzip.BadGzipFile, OSError):
                content = resp.content

            data = json.loads(content)
            response = data.get("response", {})
            return response.get("numFound", 0), response.get("docs", [])

        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                wait = 2 ** (attempt + 1)
                print(f"    ⚠ API error (attempt {attempt+1}): {e}. Retrying in {wait}s...")
                time.sleep(wait)
            else:
                print(f"    ✗ API failed after {max_retries} attempts: {e}")
                return 0, []
        except json.JSONDecodeError as e:
            print(f"    ✗ JSON decode error: {e}")
            return 0, []


def ingest_npl_citations(conn, progress):
    """
    Ingest NPL (non-patent literature) citations — the most valuable ones.
    These are cases where academic papers were cited in patent office actions.
    
    API has a 10K record pagination limit (Solr default). We work around this
    by splitting into monthly date ranges (each month has < 10K NPL records).
    """
    print("\n  Phase 1: Ingesting NPL citations (academic papers cited in patents)...")

    total_ingested = progress.get("total_npl_ingested", 0)
    done_months = set(progress.get("npl_months_done", []))

    # Generate monthly ranges from 2008-01 to 2026-12
    months = []
    for year in range(2008, 2027):
        for month in range(1, 13):
            months.append(f"{year}-{month:02d}")

    for month_str in months:
        if month_str in done_months:
            continue
        if shutdown_requested:
            break

        year, month = int(month_str[:4]), int(month_str[5:])
        # Calculate end of month
        if month == 12:
            end_date = f"{year+1}-01-01T00:00:00"
        else:
            end_date = f"{year}-{month+1:02d}-01T00:00:00"
        start_date = f"{year}-{month:02d}-01T00:00:00"

        criteria = f"nplIndicator:true AND officeActionDate:[{start_date} TO {end_date}]"

        # Get count for this month
        total, docs = fetch_page(criteria, 0, rows=1)
        if total == 0:
            done_months.add(month_str)
            continue

        # Paginate within this month (should be < 10K per month)
        month_ingested = 0
        offset = 0
        while offset < total:
            if offset == 0 and docs and len(docs) == 1:
                # Re-fetch with full page
                _, docs = fetch_page(criteria, 0)
            elif offset > 0:
                _, docs = fetch_page(criteria, offset)

            if not docs:
                break

            batch = []
            for doc in docs:
                batch.append({
                    "id": doc.get("id", ""),
                    "patent_application_number": doc.get("patentApplicationNumber", ""),
                    "publication_number": doc.get("publicationNumber", ""),
                    "cited_document_identifier": doc.get("citedDocumentIdentifier", ""),
                    "inventor_name": doc.get("inventorNameText", ""),
                    "country_code": doc.get("countryCode", ""),
                    "kind_code": doc.get("kindCode", ""),
                    "npl_indicator": True,
                    "office_action_date": parse_date(doc.get("officeActionDate")),
                    "office_action_category": doc.get("officeActionCategory", ""),
                    "citation_category_code": doc.get("citationCategoryCode", ""),
                    "related_claims": doc.get("relatedClaimNumberText", ""),
                    "passage_locations": doc.get("passageLocationText", []),
                    "examiner_cited": doc.get("examinerCitedReferenceIndicator", False),
                    "applicant_cited": doc.get("applicantCitedExaminerReferenceIndicator", False),
                    "tech_center": doc.get("techCenter", ""),
                    "work_group": doc.get("workGroupNumber", ""),
                    "group_art_unit": doc.get("groupArtUnitNumber", ""),
                    "quality_summary": doc.get("qualitySummaryText", ""),
                })

            if batch:
                insert_enriched_batch(conn, batch)
                month_ingested += len(batch)

            offset += PAGE_SIZE
            time.sleep(REQUEST_DELAY)

        total_ingested += month_ingested
        done_months.add(month_str)
        progress["npl_months_done"] = list(done_months)
        progress["total_npl_ingested"] = total_ingested
        save_progress(progress)

        if month_ingested > 0:
            print(f"    {month_str}: {month_ingested:,} records (total: {total_ingested:,})")

    progress["total_npl_ingested"] = total_ingested
    save_progress(progress)
    print(f"  ✓ Ingested {total_ingested:,} NPL citation records")


def ingest_patent_citations_with_context(conn, progress):
    """
    Ingest patent-to-patent citations that have passage location context.
    We already have patent→paper links from PatentsView, but these add:
    - Specific claim relationships
    - Passage locations (which paragraph/column/figure was cited)
    - Citation category (X/Y/A significance)
    """
    print("\n  Phase 2: Ingesting patent citations with passage context...")

    offset = progress.get("patent_offset", 0)
    total_ingested = progress.get("total_patent_ingested", 0)
    batch = []

    # Get citations where passageLocationText is non-empty and NPL is false
    criteria = "nplIndicator:false AND citationCategoryCode:[X TO Z]"
    total, docs = fetch_page(criteria, offset)

    if total == 0:
        print("    No patent citation records with context found.")
        return

    print(f"    Total patent citations with category: {total:,}")
    print(f"    Resuming from offset: {offset:,}")

    # Only ingest first 5M to be reasonable (most valuable = X and Y categories)
    max_records = min(total, 5_000_000)

    while offset < max_records and not shutdown_requested:
        if offset > 0 and not docs:
            _, docs = fetch_page(criteria, offset)

        if not docs:
            print(f"    ⚠ Empty response at offset {offset}, stopping.")
            break

        for doc in docs:
            record = {
                "id": doc.get("id", ""),
                "patent_application_number": doc.get("patentApplicationNumber", ""),
                "publication_number": doc.get("publicationNumber", ""),
                "cited_document_identifier": doc.get("citedDocumentIdentifier", ""),
                "inventor_name": doc.get("inventorNameText", ""),
                "country_code": doc.get("countryCode", ""),
                "kind_code": doc.get("kindCode", ""),
                "npl_indicator": False,
                "office_action_date": parse_date(doc.get("officeActionDate")),
                "office_action_category": doc.get("officeActionCategory", ""),
                "citation_category_code": doc.get("citationCategoryCode", ""),
                "related_claims": doc.get("relatedClaimNumberText", ""),
                "passage_locations": doc.get("passageLocationText", []),
                "examiner_cited": doc.get("examinerCitedReferenceIndicator", False),
                "applicant_cited": doc.get("applicantCitedExaminerReferenceIndicator", False),
                "tech_center": doc.get("techCenter", ""),
                "work_group": doc.get("workGroupNumber", ""),
                "group_art_unit": doc.get("groupArtUnitNumber", ""),
                "quality_summary": doc.get("qualitySummaryText", ""),
            }
            batch.append(record)

        if len(batch) >= DB_BATCH_SIZE:
            insert_enriched_batch(conn, batch)
            total_ingested += len(batch)
            batch = []

        offset += PAGE_SIZE

        if (offset // PAGE_SIZE) % 50 == 0:
            progress["patent_offset"] = offset
            progress["total_patent_ingested"] = total_ingested
            save_progress(progress)
            print(f"    {offset:,}/{max_records:,} ({offset*100//max_records}%) — {total_ingested:,} ingested")

        # Rate limit (3 req/s)
        time.sleep(0.35)

        _, docs = fetch_page(criteria, offset)

    # Flush remaining
    if batch:
        insert_enriched_batch(conn, batch)
        total_ingested += len(batch)

    progress["patent_offset"] = offset
    progress["total_patent_ingested"] = total_ingested
    save_progress(progress)
    print(f"  ✓ Ingested {total_ingested:,} patent citation context records")


def insert_enriched_batch(conn, batch):
    """Bulk insert enriched citation context records using API id as PK."""
    sql = """
        INSERT INTO enriched_citation_context (
            id, patent_application_number, publication_number, cited_document_identifier,
            inventor_name, country_code, kind_code,
            npl_indicator, office_action_date, office_action_category,
            citation_category_code, related_claims, passage_locations,
            examiner_cited, applicant_cited, tech_center, work_group,
            group_art_unit, quality_summary
        ) VALUES %s
        ON CONFLICT (id) DO NOTHING
    """
    values = []
    for r in batch:
        values.append((
            r["id"],
            r["patent_application_number"],
            r["publication_number"],
            r["cited_document_identifier"] or "",
            r.get("inventor_name", ""),
            r.get("country_code", ""),
            r.get("kind_code", ""),
            r["npl_indicator"],
            r["office_action_date"],
            r["office_action_category"],
            r["citation_category_code"],
            r["related_claims"],
            r["passage_locations"] if r["passage_locations"] else None,
            r["examiner_cited"],
            r["applicant_cited"],
            r["tech_center"],
            r.get("work_group", ""),
            r["group_art_unit"],
            r["quality_summary"],
        ))

    cur = conn.cursor()
    try:
        psycopg2.extras.execute_values(cur, sql, values, page_size=500)
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"    ⚠ Batch insert error: {e}")
    cur.close()


def link_enriched_to_patents(conn):
    """
    Cross-reference enriched citations with our patents table.
    Match publication_number → patent_id for further linking.
    """
    print("\n  Linking enriched citations to patent records...")
    cur = conn.cursor()

    # Match publication numbers to our patents table
    # Publication numbers like "20130341529" → US-20130341529
    cur.execute("""
        UPDATE enriched_citation_context ecc
        SET patent_id = p.patent_id
        FROM patents p
        WHERE ecc.patent_id IS NULL
        AND ecc.publication_number != ''
        AND p.patent_id = 'US-' || ecc.publication_number
    """)
    linked = cur.rowcount
    conn.commit()
    cur.close()

    print(f"  ✓ Linked {linked:,} enriched citations to patent records")
    return linked


def parse_date(date_str):
    """Parse ISO date string from API."""
    if not date_str:
        return None
    try:
        # Format: "2017-03-13T00:00:00"
        return datetime.fromisoformat(date_str.replace("Z", "+00:00")).date()
    except (ValueError, TypeError):
        return None


def main():
    print("=" * 60)
    print("  USPTO Enriched Citations Ingestor")
    print("  API: developer.uspto.gov/ds-api/enriched_cited_reference_metadata/v3")
    print("=" * 60)
    print(f"  DB: {DB_HOST}:{DB_PORT}/{DB_NAME}")
    print()

    conn = get_db_connection()
    progress = load_progress()
    start_time = time.time()

    # Create schema
    if not progress.get("schema_created"):
        create_schema(conn)
        progress["schema_created"] = True
        save_progress(progress)
    else:
        print("  Schema already exists")

    # Phase 1: NPL citations (most valuable — papers cited in patents)
    if not progress.get("npl_complete"):
        ingest_npl_citations(conn, progress)
        progress["npl_complete"] = True
        save_progress(progress)

    if shutdown_requested:
        print("\n  Stopped early. Progress saved.")
        conn.close()
        return

    # Phase 2: Patent citations with high-value context (X/Y categories)
    ingest_patent_citations_with_context(conn, progress)

    if shutdown_requested:
        print("\n  Stopped early. Progress saved.")
        conn.close()
        return

    # Phase 3: Link to patent records
    link_enriched_to_patents(conn)

    elapsed = time.time() - start_time
    print(f"\n{'=' * 60}")
    print(f"  COMPLETE in {elapsed/60:.1f} minutes")
    print(f"  NPL citations ingested: {progress.get('total_npl_ingested', 0):,}")
    print(f"  Patent citations ingested: {progress.get('total_patent_ingested', 0):,}")
    print(f"{'=' * 60}")

    conn.close()


if __name__ == "__main__":
    main()
