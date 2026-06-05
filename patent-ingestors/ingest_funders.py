#!/usr/bin/env python3
"""
OpenAlex Funders Ingestor — Populates funders table + funding_edges

Sources funder data from OpenAlex API (api.openalex.org/funders) and links
funders to papers via the works endpoint's `grants` field.

The funders table has a `noble_id` column ready for NobleID assignment.

Run on paper-db server:
  DB_PASS=nb_papers_2026_prod python3 ingest_funders.py
"""

import json
import os
import signal
import sys
import time
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
import requests

# Config
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_NAME = os.environ.get("DB_NAME", "paper_search")
DB_USER = os.environ.get("DB_USER", "nobleblocks")
DB_PASS = os.environ.get("DB_PASS", "")

OPENALEX_BASE = "https://api.openalex.org"
OPENALEX_EMAIL = "tech@nobleblocks.com"  # polite pool
BATCH_SIZE = 200  # API max per_page
PROGRESS_FILE = "/tmp/funders_ingest_progress.json"

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
        "funders_cursor": "*",
        "funders_ingested": 0,
        "edges_phase_cursor": "*",
        "edges_ingested": 0,
        "phase": "funders",  # funders → edges
        "last_updated": None,
    }


def save_progress(progress):
    progress["last_updated"] = datetime.now(timezone.utc).isoformat()
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f, indent=2)


def get_db():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS
    )


def ingest_funders(conn, progress):
    """Phase 1: Ingest all funders from OpenAlex."""
    print("═══ Phase 1: Ingesting Funders from OpenAlex ═══")
    cursor_val = progress.get("funders_cursor", "*")
    total = progress.get("funders_ingested", 0)

    while not shutdown_requested:
        url = f"{OPENALEX_BASE}/funders"
        params = {
            "per_page": BATCH_SIZE,
            "cursor": cursor_val,
            "mailto": OPENALEX_EMAIL,
        }

        try:
            resp = requests.get(url, params=params, timeout=30)
            if resp.status_code == 429:
                print("  Rate limited, sleeping 10s...")
                time.sleep(10)
                continue
            if resp.status_code != 200:
                print(f"  API error {resp.status_code}: {resp.text[:200]}")
                time.sleep(5)
                continue

            data = resp.json()
            results = data.get("results", [])

            if not results:
                print(f"  ✓ All funders ingested! Total: {total}")
                progress["phase"] = "edges"
                save_progress(progress)
                return True

            # Prepare batch
            batch = []
            for funder in results:
                openalex_id = funder.get("id", "").replace("https://openalex.org/", "")
                if not openalex_id:
                    continue

                # Extract CrossRef funder ID from ids field
                ids = funder.get("ids", {})
                crossref_id = ids.get("crossref")
                ror_id = ids.get("ror")

                batch.append((
                    openalex_id,
                    funder.get("display_name", ""),
                    funder.get("alternate_titles", []),
                    funder.get("country_code"),
                    funder.get("grants_count", 0),
                    funder.get("works_count", 0),
                    funder.get("cited_by_count", 0),
                    funder.get("homepage_url"),
                    ror_id,
                    crossref_id,
                ))

            if batch:
                cur = conn.cursor()
                psycopg2.extras.execute_values(
                    cur,
                    """INSERT INTO funders (openalex_id, name, alternate_names, country_code,
                       grants_count, works_count, citation_count, homepage_url, ror_id, crossref_id)
                       VALUES %s
                       ON CONFLICT (openalex_id) DO UPDATE SET
                         name = EXCLUDED.name,
                         alternate_names = EXCLUDED.alternate_names,
                         grants_count = EXCLUDED.grants_count,
                         works_count = EXCLUDED.works_count,
                         citation_count = EXCLUDED.citation_count,
                         updated_at = NOW()""",
                    batch,
                )
                conn.commit()
                cur.close()
                total += len(batch)

            # Next cursor
            meta = data.get("meta", {})
            cursor_val = meta.get("next_cursor")
            if not cursor_val:
                print(f"  ✓ All funders ingested! Total: {total}")
                progress["phase"] = "edges"
                save_progress(progress)
                return True

            progress["funders_cursor"] = cursor_val
            progress["funders_ingested"] = total

            if total % 1000 == 0:
                print(f"  Funders ingested: {total}")
                save_progress(progress)

            time.sleep(0.1)  # Be nice to OpenAlex

        except requests.exceptions.RequestException as e:
            print(f"  Network error: {e}, retrying in 10s...")
            time.sleep(10)
            continue

    save_progress(progress)
    return False


def ingest_funding_edges(conn, progress):
    """Phase 2: Link funders to papers via OpenAlex works grants field.

    Strategy: Query papers that have grants, extract funder→paper links.
    OpenAlex works have a `grants` field: [{funder, award_id}]
    """
    print("═══ Phase 2: Ingesting Funding Edges ═══")
    cursor_val = progress.get("edges_phase_cursor", "*")
    total = progress.get("edges_ingested", 0)

    while not shutdown_requested:
        url = f"{OPENALEX_BASE}/works"
        params = {
            "filter": "has_grant:true",
            "per_page": BATCH_SIZE,
            "cursor": cursor_val,
            "select": "id,doi,grants",
            "mailto": OPENALEX_EMAIL,
        }

        try:
            resp = requests.get(url, params=params, timeout=30)
            if resp.status_code == 429:
                print("  Rate limited, sleeping 10s...")
                time.sleep(10)
                continue
            if resp.status_code != 200:
                print(f"  API error {resp.status_code}: {resp.text[:200]}")
                time.sleep(5)
                continue

            data = resp.json()
            results = data.get("results", [])

            if not results:
                print(f"  ✓ All funding edges ingested! Total: {total}")
                save_progress(progress)
                return True

            # Collect edges
            edges = []
            for work in results:
                doi = (work.get("doi") or "").replace("https://doi.org/", "")
                grants = work.get("grants", []) or []

                for grant in grants:
                    funder_id = (grant.get("funder") or "").replace("https://openalex.org/", "")
                    award_id = grant.get("award_id")
                    if funder_id and doi:
                        edges.append((funder_id, doi, award_id))

            if edges:
                cur = conn.cursor()
                # Link via DOI → paper_id lookup
                psycopg2.extras.execute_values(
                    cur,
                    """INSERT INTO funding_edges (funder_id, paper_id, award_id)
                       SELECT f.id, p.id, v.award_id
                       FROM (VALUES %s) AS v(funder_openalex_id, paper_doi, award_id)
                       JOIN funders f ON f.openalex_id = v.funder_openalex_id
                       JOIN papers p ON p.doi = v.paper_doi
                       ON CONFLICT (funder_id, paper_id) DO NOTHING""",
                    edges,
                )
                inserted = cur.rowcount
                conn.commit()
                cur.close()
                total += inserted

            # Next cursor
            meta = data.get("meta", {})
            cursor_val = meta.get("next_cursor")
            if not cursor_val:
                print(f"  ✓ All funding edges done! Total: {total}")
                save_progress(progress)
                return True

            progress["edges_phase_cursor"] = cursor_val
            progress["edges_ingested"] = total

            if total % 5000 == 0:
                print(f"  Funding edges inserted: {total}")
                save_progress(progress)

            time.sleep(0.1)

        except requests.exceptions.RequestException as e:
            print(f"  Network error: {e}, retrying in 10s...")
            time.sleep(10)
            continue

    save_progress(progress)
    return False


def main():
    print("=" * 60)
    print("  OpenAlex Funders Ingestor")
    print(f"  Started: {datetime.now(timezone.utc).isoformat()}")
    print("=" * 60)

    progress = load_progress()
    conn = get_db()

    try:
        if progress["phase"] == "funders":
            if not ingest_funders(conn, progress):
                print("Interrupted during funders phase")
                return

        if progress["phase"] == "edges":
            if not ingest_funding_edges(conn, progress):
                print("Interrupted during edges phase")
                return

        print("\n✓ All phases complete!")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
