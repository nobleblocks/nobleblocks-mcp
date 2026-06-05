#!/usr/bin/env python3
"""
Crossref Event Data — Patent Citation Ingestor

This is THE canonical source for "which patents cite which papers."
Crossref Event Data tracks citation events including patent→DOI links.

Source: https://api.eventdata.crossref.org/v1/events
Filter: source=patent-citations

This gives us EXACTLY what we need:
- Which patent cited which DOI
- When the citation was first detected
- The patent identifier

FREE, no registration, no API key needed.
Rate limit: 1 request/second recommended.
"""

import requests
import psycopg2
import psycopg2.extras
import time
import json
import os
import sys
import signal
from datetime import datetime, timedelta

# Config
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_NAME = os.environ.get("DB_NAME", "paper_search")
DB_USER = os.environ.get("DB_USER", "nobleblocks")
DB_PASS = os.environ.get("DB_PASS", "")

CROSSREF_BASE = "https://api.eventdata.crossref.org/v1/events"
CROSSREF_EMAIL = os.environ.get("CROSSREF_EMAIL", "admin@nobleblocks.com")
BATCH_SIZE = 500
PROGRESS_FILE = "/tmp/crossref_patent_events_progress.json"

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
    return {"cursor": None, "from_date": "2020-01-01", "total_events": 0, "total_links": 0}


def save_progress(progress):
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f)


def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS
    )


def fetch_events(from_date, cursor=None, rows=1000):
    """Fetch patent citation events from Crossref Event Data."""
    params = {
        "source": "patent-citations",
        "from-collected-date": from_date,
        "rows": rows,
        "mailto": CROSSREF_EMAIL,
    }
    if cursor:
        params["cursor"] = cursor

    resp = requests.get(CROSSREF_BASE, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def parse_event(event):
    """Parse a single patent citation event."""
    # Subject = the patent that cites
    # Object = the paper being cited (DOI)
    subj_id = event.get("subj_id", "")
    obj_id = event.get("obj_id", "")

    # Extract patent ID from subject
    patent_id = subj_id.replace("https://doi.org/", "").replace("http://doi.org/", "")
    if not patent_id:
        return None

    # Extract paper DOI from object
    paper_doi = obj_id.replace("https://doi.org/", "").replace("http://doi.org/", "").lower().strip()
    if not paper_doi:
        return None

    # Determine jurisdiction from patent DOI/ID
    jurisdiction = None
    if "US" in patent_id.upper()[:5]:
        jurisdiction = "US"
    elif "EP" in patent_id.upper()[:5]:
        jurisdiction = "EP"
    elif "WO" in patent_id.upper()[:5]:
        jurisdiction = "WO"

    return {
        "patent_id": patent_id,
        "paper_doi": paper_doi,
        "jurisdiction": jurisdiction,
        "occurred_at": event.get("occurred_at"),
        "source_id": event.get("source_id"),
    }


def insert_events_batch(conn, events):
    """Insert patent citation events."""
    if not events:
        return 0

    # Ensure patent records exist
    patent_sql = """
        INSERT INTO patents (patent_id, jurisdiction, source)
        VALUES %s
        ON CONFLICT (patent_id) DO NOTHING
    """
    patent_values = list(set(
        (e["patent_id"], e.get("jurisdiction"), "crossref_events")
        for e in events if e.get("patent_id")
    ))
    if patent_values:
        cur = conn.cursor()
        psycopg2.extras.execute_values(cur, patent_sql, patent_values,
            template="(%s, %s, %s)")
        conn.commit()
        cur.close()

    # Insert citation links
    citation_sql = """
        INSERT INTO patent_paper_citations (patent_id, paper_doi, citation_type, source)
        VALUES %s
        ON CONFLICT (patent_id, COALESCE(paper_doi, ''), COALESCE(paper_openalex_id, ''))
        DO NOTHING
    """
    citation_values = [
        (e["patent_id"], e["paper_doi"], "crossref_event", "crossref_events")
        for e in events
    ]
    cur = conn.cursor()
    psycopg2.extras.execute_values(cur, citation_sql, citation_values,
        template="(%s, %s, %s, %s)")
    inserted = cur.rowcount
    conn.commit()
    cur.close()
    return inserted


def resolve_paper_ids(conn):
    """Resolve DOIs to internal paper IDs."""
    cur = conn.cursor()
    cur.execute("""
        UPDATE patent_paper_citations ppc
        SET paper_id = p.id,
            paper_title = p.title
        FROM papers p
        WHERE ppc.paper_doi = p.doi
        AND ppc.paper_id IS NULL
        AND ppc.source = 'crossref_events'
        AND ppc.id IN (
            SELECT id FROM patent_paper_citations
            WHERE paper_id IS NULL AND source = 'crossref_events'
            LIMIT 100000
        )
    """)
    resolved = cur.rowcount
    conn.commit()
    cur.close()
    return resolved


def main():
    print("=" * 60)
    print("  Crossref Event Data — Patent Citation Ingestor")
    print("=" * 60)
    print(f"  DB: {DB_HOST}:{DB_PORT}/{DB_NAME}")
    print(f"  Source: {CROSSREF_BASE}")
    print()

    conn = get_db_connection()
    progress = load_progress()

    from_date = progress.get("from_date", "2020-01-01")
    cursor = progress.get("cursor")
    total_events = progress.get("total_events", 0)
    total_links = progress.get("total_links", 0)

    print(f"  From date: {from_date}")
    if cursor:
        print(f"  Resuming from cursor...")
    print()

    start_time = time.time()
    batch = []
    page_count = 0

    while not shutdown_requested:
        try:
            data = fetch_events(from_date, cursor)
        except requests.exceptions.RequestException as e:
            print(f"  ⚠ API error: {e}, retrying in 10s...")
            time.sleep(10)
            continue

        message = data.get("message", {})
        events = message.get("events", [])
        next_cursor = message.get("next-cursor")
        total_results = message.get("total-results", 0)

        if not events:
            print("  ✅ No more events — ingestion complete!")
            break

        # Parse events
        for event in events:
            parsed = parse_event(event)
            if parsed:
                batch.append(parsed)
                total_events += 1

        # Flush batch
        if len(batch) >= BATCH_SIZE:
            inserted = insert_events_batch(conn, batch)
            total_links += inserted
            batch = []

        cursor = next_cursor
        page_count += 1

        if page_count % 10 == 0:
            elapsed = time.time() - start_time
            rate = total_events / elapsed if elapsed > 0 else 0
            print(f"  Page {page_count}: {total_events:,} events, {total_links:,} new links "
                  f"({rate:.0f}/s) | total available: {total_results:,}")

            progress["cursor"] = cursor
            progress["total_events"] = total_events
            progress["total_links"] = total_links
            save_progress(progress)

        if not next_cursor:
            print("  ✅ Cursor exhausted — all events processed!")
            break

        # Rate limit
        time.sleep(1.0)

    # Flush remaining
    if batch:
        inserted = insert_events_batch(conn, batch)
        total_links += inserted

    # Save progress
    progress["cursor"] = cursor
    progress["total_events"] = total_events
    progress["total_links"] = total_links
    progress["last_run"] = datetime.now().isoformat()
    save_progress(progress)

    # Resolve paper IDs
    print("\n  Resolving DOIs to paper records...")
    resolved = resolve_paper_ids(conn)
    print(f"  ✓ Resolved {resolved:,} DOI→paper links")

    elapsed = time.time() - start_time
    print(f"\n{'=' * 60}")
    print(f"  DONE in {elapsed:.0f}s")
    print(f"  Events processed: {total_events:,}")
    print(f"  Citation links created: {total_links:,}")
    print(f"  Paper IDs resolved: {resolved:,}")
    print(f"{'=' * 60}")

    conn.close()


if __name__ == "__main__":
    main()
