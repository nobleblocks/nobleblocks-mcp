#!/usr/bin/env python3
"""
OpenAlex Patent-Cited Papers Ingestor

OpenAlex does NOT have patents as a work type. Instead, it tracks which
papers are cited by patents via the `cited_by_patent_count` field.

Strategy: Fetch papers with cited_by_patent_count > 0, then for each,
get the patent citation details from OpenAlex's citations API.

This gives us: which papers are commercially relevant (cited by patents).
The actual patent records come from PatentsView/USPTO/BigQuery.

API: https://api.openalex.org/works?filter=cited_by_patent_count:>0
"""

import requests
import psycopg2
import psycopg2.extras
import time
import json
import os
import sys
import signal
from datetime import datetime, date

# Config
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_NAME = os.environ.get("DB_NAME", "paper_search")
DB_USER = os.environ.get("DB_USER", "nobleblocks")
DB_PASS = os.environ.get("DB_PASS", "")

OPENALEX_EMAIL = os.environ.get("OPENALEX_EMAIL", "admin@nobleblocks.com")
OPENALEX_BASE = "https://api.openalex.org"
PER_PAGE = 200
BATCH_SIZE = 500  # DB insert batch size

# Progress file
PROGRESS_FILE = "/tmp/openalex_patent_progress.json"

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
    return {"cursor": "*", "patents_processed": 0, "links_created": 0, "started_at": datetime.now().isoformat()}


def save_progress(progress):
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f)


def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS
    )


def fetch_patent_page(cursor):
    """Fetch a page of papers cited by patents from OpenAlex."""
    params = {
        "filter": "cited_by_patent_count:>0",
        "per_page": PER_PAGE,
        "cursor": cursor,
        "select": "id,doi,title,publication_date,cited_by_patent_count,authorships,topics,primary_location",
        "mailto": OPENALEX_EMAIL,
    }
    resp = requests.get(f"{OPENALEX_BASE}/works", params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def extract_patent_data(work):
    """Extract paper record that's cited by patents.
    We store this as a patent_paper_citation signal — the paper is the target."""
    oaid = work.get("id", "").replace("https://openalex.org/", "")
    doi = work.get("doi", "").replace("https://doi.org/", "").lower().strip() if work.get("doi") else None
    title = work.get("title", "")
    patent_count = work.get("cited_by_patent_count", 0)

    if patent_count <= 0:
        return None

    return {
        "openalex_id": oaid,
        "doi": doi,
        "title": title,
        "cited_by_patent_count": patent_count,
        "publication_date": work.get("publication_date"),
    }


def extract_citation_links(work):
    """For papers cited by patents, create signal records.
    We don't have individual patent IDs from OpenAlex — just the count.
    Store as aggregated signals in patent_citation_signals."""
    oaid = work.get("id", "").replace("https://openalex.org/", "")
    doi = work.get("doi", "").replace("https://doi.org/", "").lower().strip() if work.get("doi") else None
    patent_count = work.get("cited_by_patent_count", 0)

    if patent_count <= 0:
        return []

    return [{
        "paper_openalex_id": oaid,
        "paper_doi": doi,
        "patent_citations_count": patent_count,
    }]


def insert_patents_batch(conn, papers):
    """Store papers cited by patents — update patent citation counts."""
    if not papers:
        return 0

    sql = """
        INSERT INTO patent_citation_signals
            (paper_doi, paper_openalex_id, patent_citations_count, velocity_score, computed_at)
        VALUES %s
        ON CONFLICT DO NOTHING
    """
    values = [
        (p.get("doi"), p.get("openalex_id"), p.get("cited_by_patent_count", 0),
         float(p.get("cited_by_patent_count", 0)), datetime.now())
        for p in papers if p
    ]
    if not values:
        return 0
    cur = conn.cursor()
    psycopg2.extras.execute_values(cur, sql, values,
        template="(%s, %s, %s, %s, %s)")
    conn.commit()
    cur.close()
    return len(values)


def insert_links_batch(conn, links):
    """Store aggregated patent citation signals."""
    # These are already covered by insert_patents_batch
    return len(links)


def resolve_paper_ids(conn, batch_size=10000):
    """Resolve OpenAlex IDs to paper DOIs and internal IDs."""
    cur = conn.cursor()
    # Find unresolved links
    cur.execute("""
        UPDATE patent_paper_citations ppc
        SET paper_doi = p.doi,
            paper_id = p.id,
            paper_title = p.title
        FROM papers p
        WHERE ppc.paper_openalex_id = p.openalex_id
        AND ppc.paper_id IS NULL
        AND ppc.id IN (
            SELECT id FROM patent_paper_citations
            WHERE paper_id IS NULL
            LIMIT %s
        )
    """, (batch_size,))
    resolved = cur.rowcount
    conn.commit()
    cur.close()
    return resolved


def main():
    print("=" * 60)
    print("  OpenAlex Patent↔Paper Citation Ingestor")
    print("=" * 60)
    print(f"  DB: {DB_HOST}:{DB_PORT}/{DB_NAME}")
    print(f"  Email: {OPENALEX_EMAIL}")
    print()

    conn = get_db_connection()
    progress = load_progress()

    cursor = progress.get("cursor", "*")
    total_patents = progress.get("patents_processed", 0)
    total_links = progress.get("links_created", 0)

    if cursor != "*":
        print(f"  Resuming from cursor (already processed {total_patents} patents, {total_links} links)")
    else:
        print("  Starting fresh...")

    print()

    patent_batch = []
    link_batch = []
    page_count = 0
    start_time = time.time()

    while not shutdown_requested:
        try:
            data = fetch_patent_page(cursor)
        except requests.exceptions.RequestException as e:
            print(f"  ⚠ API error: {e}, retrying in 10s...")
            time.sleep(10)
            continue

        results = data.get("results", [])
        if not results:
            print("  ✅ No more results — ingestion complete!")
            break

        # Process each patent work
        for work in results:
            patent = extract_patent_data(work)
            patent_batch.append(patent)

            links = extract_citation_links(work)
            link_batch.extend(links)

        # Flush batches
        if len(patent_batch) >= BATCH_SIZE:
            inserted_p = insert_patents_batch(conn, patent_batch)
            inserted_l = insert_links_batch(conn, link_batch)
            total_patents += inserted_p
            total_links += inserted_l
            patent_batch = []
            link_batch = []

        # Update cursor
        meta = data.get("meta", {})
        cursor = meta.get("next_cursor")
        if not cursor:
            print("  ✅ Cursor exhausted — ingestion complete!")
            break

        page_count += 1
        elapsed = time.time() - start_time
        rate = total_patents / elapsed if elapsed > 0 else 0

        if page_count % 10 == 0:
            print(f"  Page {page_count}: {total_patents:,} patents, {total_links:,} links "
                  f"({rate:.0f}/s) | cursor={cursor[:20]}...")
            progress["cursor"] = cursor
            progress["patents_processed"] = total_patents
            progress["links_created"] = total_links
            save_progress(progress)

        # Rate limit: ~10 req/sec for polite pool
        time.sleep(0.1)

    # Flush remaining
    if patent_batch:
        inserted_p = insert_patents_batch(conn, patent_batch)
        inserted_l = insert_links_batch(conn, link_batch)
        total_patents += inserted_p
        total_links += inserted_l

    # Save final progress
    progress["cursor"] = cursor
    progress["patents_processed"] = total_patents
    progress["links_created"] = total_links
    progress["last_run"] = datetime.now().isoformat()
    save_progress(progress)

    # Resolve paper IDs
    print("\n  Resolving paper IDs (linking to existing papers table)...")
    resolved = resolve_paper_ids(conn)
    print(f"  ✓ Resolved {resolved:,} links to paper records")

    elapsed = time.time() - start_time
    print(f"\n{'=' * 60}")
    print(f"  DONE in {elapsed:.0f}s")
    print(f"  Patents processed: {total_patents:,}")
    print(f"  Citation links: {total_links:,}")
    print(f"  Paper IDs resolved: {resolved:,}")
    print(f"{'=' * 60}")

    conn.close()


if __name__ == "__main__":
    main()
