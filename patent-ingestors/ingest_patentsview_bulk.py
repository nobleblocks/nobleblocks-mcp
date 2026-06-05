#!/usr/bin/env python3
"""
PatentsView Bulk Data Ingestor — Patent Metadata + NPL Citations

Downloads TSV files from PatentsView's S3-hosted bulk data:
- g_patent.tsv.zip (230MB) → patent metadata
- g_other_reference.tsv.zip (4.3GB) → Non-Patent Literature citations (academic papers)

The NPL citations contain free-text references that often include DOIs.
We extract DOIs and link patents to papers in our database.

Source: https://s3.amazonaws.com/data.patentsview.org/download/
Updated weekly by USPTO.

Run on paper-db server:
  DB_PASS=nb_papers_2026_prod python3 ingest_patentsview_bulk.py
"""

import csv
import gzip
import io
import json
import os
import re
import signal
import sys
import time
import zipfile
from datetime import datetime
from pathlib import Path

import psycopg2
import psycopg2.extras
import requests

# Config
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_NAME = os.environ.get("DB_NAME", "paper_search")
DB_USER = os.environ.get("DB_USER", "nobleblocks")
DB_PASS = os.environ.get("DB_PASS", "")

S3_BASE = "https://s3.amazonaws.com/data.patentsview.org/download"
DOWNLOAD_DIR = "/tmp/patentsview_bulk"
BATCH_SIZE = 5000
PROGRESS_FILE = "/tmp/patentsview_bulk_progress.json"

# DOI regex — matches 10.XXXX/... patterns in NPL text
DOI_PATTERN = re.compile(r'\b(10\.\d{4,9}/[^\s,;"\'\)>\]]+)')

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
        "patents_loaded": False,
        "npl_rows_processed": 0,
        "total_patents": 0,
        "total_npl_with_doi": 0,
        "total_npl_no_doi": 0,
    }


def save_progress(progress):
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f)


def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS
    )


def download_file(filename):
    """Download a file from PatentsView S3 to local disk."""
    url = f"{S3_BASE}/{filename}"
    dest = os.path.join(DOWNLOAD_DIR, filename)
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    if os.path.exists(dest):
        print(f"  Already downloaded: {filename}")
        return dest

    print(f"  Downloading {filename}...")
    resp = requests.get(url, stream=True, timeout=60)
    resp.raise_for_status()

    total = int(resp.headers.get("content-length", 0))
    downloaded = 0
    start = time.time()

    with open(dest + ".partial", "wb") as f:
        for chunk in resp.iter_content(chunk_size=1024 * 1024):
            f.write(chunk)
            downloaded += len(chunk)
            if total > 0:
                pct = downloaded * 100 // total
                speed = downloaded / (time.time() - start + 0.001) / 1024 / 1024
                print(f"\r    {pct}% ({downloaded // 1048576}MB / {total // 1048576}MB) @ {speed:.1f} MB/s", end="", flush=True)

    os.rename(dest + ".partial", dest)
    print(f"\n  ✓ Downloaded: {filename} ({downloaded // 1048576}MB)")
    return dest


def extract_doi_from_npl(text):
    """Extract DOI from NPL citation text. Returns cleaned DOI or None."""
    if not text:
        return None
    match = DOI_PATTERN.search(text)
    if match:
        doi = match.group(1)
        # Clean trailing punctuation
        doi = doi.rstrip(".,;:)]}>")
        # Validate basic structure
        if len(doi) > 8 and "/" in doi:
            return doi.lower()
    return None


def ingest_patents(conn):
    """Load patent metadata from g_patent.tsv.zip."""
    filepath = download_file("g_patent.tsv.zip")

    print("\n  Processing patent metadata...")
    count = 0
    batch = []

    with zipfile.ZipFile(filepath) as zf:
        # Find the TSV file inside
        tsv_name = [n for n in zf.namelist() if n.endswith(".tsv")][0]
        print(f"    Reading {tsv_name}...")

        with zf.open(tsv_name) as f:
            reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8", errors="replace"), delimiter="\t")

            for row in reader:
                if shutdown_requested:
                    break

                patent_id = row.get("patent_id", "").strip()
                if not patent_id:
                    continue

                batch.append({
                    "patent_id": f"US-{patent_id}",
                    "title": (row.get("patent_title") or "")[:500],
                    "abstract": "",  # Not in g_patent.tsv base table
                    "grant_date": row.get("patent_date") or None,
                    "patent_type": row.get("patent_type") or None,
                    "num_claims": int(row.get("num_claims") or 0) or None,
                })
                count += 1

                if len(batch) >= BATCH_SIZE:
                    insert_patents_batch(conn, batch)
                    batch = []
                    if count % 100000 == 0:
                        print(f"    {count:,} patents loaded...")

    if batch:
        insert_patents_batch(conn, batch)

    print(f"  ✓ Loaded {count:,} patents")
    return count


def insert_patents_batch(conn, batch):
    """Bulk insert/update patent metadata."""
    sql = """
        INSERT INTO patents (patent_id, title, abstract, grant_date, jurisdiction, source)
        VALUES %s
        ON CONFLICT (patent_id) DO UPDATE SET
            title = COALESCE(EXCLUDED.title, patents.title),
            abstract = COALESCE(EXCLUDED.abstract, patents.abstract),
            grant_date = COALESCE(EXCLUDED.grant_date, patents.grant_date),
            updated_at = NOW()
    """
    values = [
        (p["patent_id"], p["title"], p["abstract"], p["grant_date"], "US", "patentsview")
        for p in batch
    ]
    cur = conn.cursor()
    psycopg2.extras.execute_values(cur, sql, values, template="(%s, %s, %s, %s, %s, %s)")
    conn.commit()
    cur.close()


def ingest_npl_citations(conn, skip_rows=0):
    """Load NPL citations from g_other_reference.tsv.zip and extract DOIs."""
    filepath = download_file("g_other_reference.tsv.zip")

    print("\n  Processing NPL citations (patent→paper links)...")
    print(f"    Skipping first {skip_rows:,} rows (already processed)")

    count = 0
    doi_count = 0
    no_doi_count = 0
    batch = []
    row_num = 0

    with zipfile.ZipFile(filepath) as zf:
        tsv_name = [n for n in zf.namelist() if n.endswith(".tsv")][0]
        print(f"    Reading {tsv_name}...")

        with zf.open(tsv_name) as f:
            reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8", errors="replace"), delimiter="\t")

            for row in reader:
                row_num += 1
                if row_num <= skip_rows:
                    continue

                if shutdown_requested:
                    break

                patent_id = row.get("patent_id", "").strip()
                npl_text = row.get("other_reference_text", "").strip()
                if not patent_id or not npl_text:
                    continue

                # Try to extract DOI
                doi = extract_doi_from_npl(npl_text)
                count += 1

                if doi:
                    doi_count += 1
                    batch.append({
                        "patent_id": f"US-{patent_id}",
                        "paper_doi": doi,
                        "citation_context": npl_text[:500],
                        "citation_type": "npl",
                        "source": "patentsview",
                    })
                else:
                    no_doi_count += 1
                    # Still store these — can resolve via title matching later
                    if len(npl_text) > 20:  # Skip very short/useless entries
                        batch.append({
                            "patent_id": f"US-{patent_id}",
                            "paper_doi": None,
                            "citation_context": npl_text[:500],
                            "citation_type": "npl",
                            "source": "patentsview",
                        })

                if len(batch) >= BATCH_SIZE:
                    insert_citations_batch(conn, batch)
                    batch = []
                    if count % 500000 == 0:
                        print(f"    {count:,} NPL citations processed ({doi_count:,} with DOI, {no_doi_count:,} without)")
                        save_progress({
                            "patents_loaded": True,
                            "npl_rows_processed": row_num,
                            "total_patents": 0,
                            "total_npl_with_doi": doi_count,
                            "total_npl_no_doi": no_doi_count,
                        })

    if batch:
        insert_citations_batch(conn, batch)

    print(f"  ✓ {count:,} NPL citations: {doi_count:,} with DOI, {no_doi_count:,} without")
    return count, doi_count


def insert_citations_batch(conn, batch):
    """Bulk insert patent→paper citation links."""
    # Citations with DOIs
    doi_citations = [c for c in batch if c.get("paper_doi")]
    no_doi_citations = [c for c in batch if not c.get("paper_doi")]

    if doi_citations:
        sql = """
            INSERT INTO patent_paper_citations (patent_id, paper_doi, citation_context, citation_type, source)
            VALUES %s
            ON CONFLICT (patent_id, COALESCE(paper_doi, ''), COALESCE(paper_openalex_id, ''))
            DO NOTHING
        """
        values = [(c["patent_id"], c["paper_doi"], c["citation_context"],
                   c["citation_type"], c["source"]) for c in doi_citations]
        cur = conn.cursor()
        try:
            psycopg2.extras.execute_values(cur, sql, values, template="(%s, %s, %s, %s, %s)")
            conn.commit()
        except Exception as e:
            conn.rollback()
            # Fallback: insert one by one, skip conflicts
            for v in values:
                try:
                    cur.execute(
                        "INSERT INTO patent_paper_citations (patent_id, paper_doi, citation_context, citation_type, source) "
                        "VALUES (%s, %s, %s, %s, %s) "
                        "ON CONFLICT (patent_id, COALESCE(paper_doi, ''), COALESCE(paper_openalex_id, '')) DO NOTHING",
                        v
                    )
                    conn.commit()
                except Exception:
                    conn.rollback()
        cur.close()

    # Store no-DOI citations too (for future title-matching)
    if no_doi_citations:
        sql = """
            INSERT INTO patent_paper_citations (patent_id, citation_context, citation_type, source)
            VALUES %s
            ON CONFLICT DO NOTHING
        """
        values = [(c["patent_id"], c["citation_context"],
                   c["citation_type"], c["source"]) for c in no_doi_citations]
        cur = conn.cursor()
        try:
            psycopg2.extras.execute_values(cur, sql, values, template="(%s, %s, %s, %s)")
            conn.commit()
        except Exception:
            conn.rollback()
        cur.close()


def resolve_dois_to_papers(conn):
    """Match extracted DOIs to papers in our database."""
    print("\n  Resolving DOIs to paper records...")
    cur = conn.cursor()

    # Match DOIs to papers table
    cur.execute("""
        UPDATE patent_paper_citations ppc
        SET paper_id = p.id,
            paper_title = p.title
        FROM papers p
        WHERE LOWER(ppc.paper_doi) = LOWER(p.doi)
        AND ppc.paper_id IS NULL
        AND ppc.paper_doi IS NOT NULL
        AND ppc.id IN (
            SELECT id FROM patent_paper_citations
            WHERE paper_id IS NULL AND paper_doi IS NOT NULL
            LIMIT 500000
        )
    """)
    resolved = cur.rowcount
    conn.commit()
    cur.close()

    print(f"  ✓ Resolved {resolved:,} DOI→paper links")
    return resolved


def compute_citation_signals(conn):
    """Compute patent citation signal scores for papers."""
    print("\n  Computing patent citation signals...")
    cur = conn.cursor()

    # Use the actual schema: patent_citations_count, velocity_score, window_start/end
    cur.execute("""
        INSERT INTO patent_citation_signals (paper_id, paper_doi, patent_citations_count, velocity_score, window_start, window_end)
        SELECT
            ppc.paper_id,
            ppc.paper_doi,
            COUNT(DISTINCT ppc.patent_id) AS patent_citations_count,
            -- Velocity: more citations from recent patents = higher score
            (COUNT(DISTINCT ppc.patent_id) * 10.0 +
             CASE WHEN MAX(pat.grant_date) > CURRENT_DATE - INTERVAL '2 years' THEN 20 ELSE 0 END
            ) AS velocity_score,
            MIN(pat.grant_date) AS window_start,
            MAX(pat.grant_date) AS window_end
        FROM patent_paper_citations ppc
        JOIN patents pat ON ppc.patent_id = pat.patent_id
        WHERE ppc.paper_id IS NOT NULL
        GROUP BY ppc.paper_id, ppc.paper_doi
    """)
    signals = cur.rowcount
    conn.commit()
    cur.close()

    print(f"  ✓ Computed signals for {signals:,} papers")
    return signals


def main():
    print("=" * 60)
    print("  PatentsView Bulk Data Ingestor")
    print("  Source: s3://data.patentsview.org/download/")
    print("=" * 60)
    print(f"  DB: {DB_HOST}:{DB_PORT}/{DB_NAME}")
    print(f"  Download dir: {DOWNLOAD_DIR}")
    print()

    conn = get_db_connection()
    progress = load_progress()
    start_time = time.time()

    # Phase 1: Patent metadata
    if not progress.get("patents_loaded"):
        total_patents = ingest_patents(conn)
        progress["patents_loaded"] = True
        progress["total_patents"] = total_patents
        save_progress(progress)
    else:
        print(f"  Skipping patents (already loaded: {progress.get('total_patents', 0):,})")

    if shutdown_requested:
        print("\n  Stopped early. Progress saved.")
        conn.close()
        return

    # Phase 2: NPL citations (the main event — patent→paper links)
    skip_rows = progress.get("npl_rows_processed", 0)
    total_npl, doi_count = ingest_npl_citations(conn, skip_rows=skip_rows)

    progress["total_npl_with_doi"] = progress.get("total_npl_with_doi", 0) + doi_count
    save_progress(progress)

    if shutdown_requested:
        print("\n  Stopped early. Progress saved.")
        conn.close()
        return

    # Phase 3: Resolve DOIs to papers
    resolved = resolve_dois_to_papers(conn)

    # Phase 4: Compute citation signals
    signals = compute_citation_signals(conn)

    elapsed = time.time() - start_time
    print(f"\n{'=' * 60}")
    print(f"  DONE in {elapsed:.0f}s ({elapsed/3600:.1f}h)")
    print(f"  Patents: {progress.get('total_patents', 0):,}")
    print(f"  NPL citations: {total_npl:,}")
    print(f"  With DOI: {doi_count:,}")
    print(f"  Resolved to papers: {resolved:,}")
    print(f"  Signal scores computed: {signals:,}")
    print(f"{'=' * 60}")

    conn.close()


if __name__ == "__main__":
    main()
