#!/usr/bin/env python3
"""
Open Targets Connector v2 — literatureOcurrences approach
==========================================================
Instead of checking OUR PMIDs against OT (0.03% hit rate, 16+ hours),
this version pulls ALL literature references from Open Targets drug targets
and matches them against our papers.

Validated API patterns (OT Platform v26.03):
  - search(queryString:"*", entityNames:["target"], page:{index,size}) → 78,691 targets
  - target(ensemblId).literatureOcurrences(cursor) → paginated PMIDs (size via cursor JSON)
  - Cursor format: base64({"index": N, "size": M}), max size 500

Strategy:
  1. Get top drug targets from OT search API (sorted by relevance = clinical importance)
  2. For each target, fetch literatureOcurrences (up to 5000 PMIDs per target)
  3. Bulk-match PMIDs against our papers table
  4. Store: target metadata + paper links

At 3 req/s: 5000 targets × ~10 pages × 0.35s = ~5 hours for comprehensive coverage.
Expected yield: millions of paper links.

Run:
  python3 kg_connector_open_targets_v2.py
  python3 kg_connector_open_targets_v2.py --targets 500  # test with fewer
"""

import argparse
import base64
import json
import logging
import os
import sys
import time
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError

import psycopg2
import psycopg2.extras

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("open-targets-v2")

# ── Config ────────────────────────────────────────────────────────────────────

DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = int(os.environ.get("DB_PORT", "5432"))
DB_NAME = os.environ.get("DB_NAME", "paper_search")
DB_USER = os.environ.get("DB_USER", "nobleblocks")
DB_PASS = os.environ.get("DB_PASS", "nb_papers_2026_prod")

OT_API = "https://api.platform.opentargets.org/api/v4/graphql"
RATE_LIMIT_DELAY = 0.35  # seconds between API calls (~3/s)
PAGE_SIZE = 500           # max literatureOcurrences per page
MAX_PAGES_PER_TARGET = 10 # cap at 5000 PMIDs per target
PMID_BATCH_SIZE = 1000    # PMIDs to match per DB query
PROGRESS_FILE = "/tmp/ot_v2_progress.json"


def get_conn():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS,
    )


def ensure_tables(conn):
    """Create/verify tables for OT target-paper links."""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS ot_targets (
                ensembl_id TEXT PRIMARY KEY,
                symbol TEXT,
                name TEXT,
                literature_count INT DEFAULT 0,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE TABLE IF NOT EXISTS ot_paper_targets (
                paper_id BIGINT NOT NULL,
                ensembl_id TEXT NOT NULL,
                pmid TEXT NOT NULL,
                PRIMARY KEY (paper_id, ensembl_id)
            );
            CREATE INDEX IF NOT EXISTS idx_ot_paper_targets_target
                ON ot_paper_targets(ensembl_id);
            CREATE INDEX IF NOT EXISTS idx_ot_paper_targets_paper
                ON ot_paper_targets(paper_id);
        """)
        conn.commit()


def load_progress():
    try:
        with open(PROGRESS_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {
            "targets_processed": 0,
            "targets_fetched": 0,
            "total_pmids": 0,
            "paper_links": 0,
            "processed_ids": [],
        }


def save_progress(prog):
    with open(PROGRESS_FILE, "w") as f:
        json.dump(prog, f)


def query_ot(query_str, variables=None, retries=3):
    """Execute GraphQL query against Open Targets API."""
    payload = {"query": query_str}
    if variables:
        payload["variables"] = variables

    data = json.dumps(payload).encode("utf-8")
    req = Request(OT_API, data=data, headers={
        "Content-Type": "application/json",
        "User-Agent": "NobleBlocks/1.0 (research platform)",
    })

    for attempt in range(retries):
        try:
            with urlopen(req, timeout=30) as resp:
                result = json.loads(resp.read().decode("utf-8"))
                if "errors" in result:
                    log.warning("GraphQL error: %s",
                                result["errors"][0].get("message", "")[:120])
                    return None
                return result.get("data")
        except (HTTPError, URLError, TimeoutError) as e:
            if attempt < retries - 1:
                wait = 2 ** (attempt + 1)
                log.debug("Retry %d after %ds: %s", attempt + 1, wait, e)
                time.sleep(wait)
                continue
            log.error("OT API failed after %d retries: %s", retries, e)
            return None
    return None


def make_cursor(index, size=PAGE_SIZE):
    """Create OT-compatible cursor from page index and size."""
    return base64.b64encode(
        json.dumps({"index": index, "size": size}).encode()
    ).decode()


def fetch_targets(page_index, page_size=500):
    """Fetch a page of targets from OT search API."""
    query = """
    query($page: Pagination!) {
        search(queryString: "*", entityNames: ["target"], page: $page) {
            total
            hits { id name }
        }
    }
    """
    return query_ot(query, {"page": {"index": page_index, "size": page_size}})


def fetch_literature(ensembl_id, page_index=0):
    """Fetch literatureOcurrences for a target (500 PMIDs per page)."""
    cursor = make_cursor(page_index, PAGE_SIZE)
    query = """
    query($eid: String!, $cursor: String) {
        target(ensemblId: $eid) {
            approvedSymbol
            literatureOcurrences(cursor: $cursor) {
                count
                cursor
                rows { pmid }
            }
        }
    }
    """
    return query_ot(query, {"eid": ensembl_id, "cursor": cursor})


def match_pmids_to_papers(conn, pmids):
    """Bulk-match PMIDs against our papers table. Returns {pmid: paper_id}."""
    if not pmids:
        return {}
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id, pmid FROM papers WHERE pmid = ANY(%s)",
            (list(pmids),)
        )
        return {str(row[1]): row[0] for row in cur.fetchall()}


def run(max_targets=5000, resume=True):
    log.info("Open Targets Connector v2 (literatureOcurrences approach)")
    log.info("Strategy: Pull literature FROM OT targets, match PMIDs to our papers")
    log.info("Max targets: %d", max_targets)

    conn = get_conn()
    ensure_tables(conn)

    progress = load_progress() if resume else {
        "targets_processed": 0, "targets_fetched": 0,
        "total_pmids": 0, "paper_links": 0, "processed_ids": [],
    }
    processed_set = set(progress.get("processed_ids", []))

    t0 = time.time()
    total_paper_links = progress["paper_links"]
    total_pmids_seen = progress["total_pmids"]

    # Phase 1: Get target list from OT search API
    log.info("Phase 1: Fetching target list from OT...")
    all_targets = []
    page_idx = 0

    while len(all_targets) < max_targets:
        time.sleep(RATE_LIMIT_DELAY)
        data = fetch_targets(page_idx, page_size=500)
        if not data or not data.get("search"):
            break

        hits = data["search"].get("hits", [])
        total_available = data["search"].get("total", 0)

        if not hits:
            break

        for hit in hits:
            if hit["id"] not in processed_set:
                all_targets.append(hit)

        page_idx += 1
        if len(all_targets) + len(processed_set) >= min(max_targets, total_available):
            break

    log.info("  Got %d targets to process (%d already done, %d total in OT)",
             len(all_targets), len(processed_set), total_available)

    if not all_targets:
        log.info("All targets already processed. Done.")
        conn.close()
        return

    # Phase 2: For each target, fetch literature PMIDs
    log.info("Phase 2: Fetching literature for each target...")

    for t_idx, target in enumerate(all_targets[:max_targets - len(processed_set)]):
        ensembl_id = target["id"]
        target_name = target.get("name", "")

        # Fetch first page to get count
        time.sleep(RATE_LIMIT_DELAY)
        data = fetch_literature(ensembl_id, page_index=0)

        if not data or not data.get("target"):
            continue

        target_data = data["target"]
        symbol = target_data.get("approvedSymbol", "")
        lit = target_data.get("literatureOcurrences", {})
        lit_count = lit.get("count", 0)
        rows = lit.get("rows", [])

        if lit_count == 0:
            processed_set.add(ensembl_id)
            continue

        # Collect PMIDs from first page
        pmids = set()
        for row in rows:
            p = row.get("pmid")
            if p:
                pmids.add(str(p))

        # Paginate for more (up to MAX_PAGES_PER_TARGET)
        pages_fetched = 1
        while pages_fetched < MAX_PAGES_PER_TARGET and len(pmids) < lit_count:
            time.sleep(RATE_LIMIT_DELAY)
            data = fetch_literature(ensembl_id, page_index=pages_fetched)

            if not data or not data.get("target"):
                break

            rows = data["target"].get("literatureOcurrences", {}).get("rows", [])
            if not rows:
                break

            for row in rows:
                p = row.get("pmid")
                if p:
                    pmids.add(str(p))

            pages_fetched += 1

        # Store target metadata
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO ot_targets (ensembl_id, symbol, name, literature_count)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (ensembl_id) DO UPDATE SET
                    literature_count = EXCLUDED.literature_count
            """, (ensembl_id, symbol, target_name, lit_count))
        conn.commit()

        # Phase 3: Match PMIDs to papers in batches
        pmid_list = list(pmids)
        target_links = 0

        for batch_start in range(0, len(pmid_list), PMID_BATCH_SIZE):
            batch = pmid_list[batch_start:batch_start + PMID_BATCH_SIZE]
            pmid_map = match_pmids_to_papers(conn, batch)

            if pmid_map:
                with conn.cursor() as cur:
                    psycopg2.extras.execute_values(
                        cur,
                        """INSERT INTO ot_paper_targets (paper_id, ensembl_id, pmid)
                           VALUES %s ON CONFLICT DO NOTHING""",
                        [(paper_id, ensembl_id, pmid)
                         for pmid, paper_id in pmid_map.items()],
                        template="(%s, %s, %s)",
                    )
                    target_links += cur.rowcount
                conn.commit()

        total_paper_links += target_links
        total_pmids_seen += len(pmids)
        processed_set.add(ensembl_id)

        # Log progress
        if (t_idx + 1) % 25 == 0 or target_links > 100:
            elapsed = time.time() - t0
            rate = (t_idx + 1) / elapsed * 3600 if elapsed > 0 else 0
            log.info("  [%d/%d] %s (%s): %d PMIDs, %d matched | Total: %d links, %.0f targets/hr",
                     t_idx + 1, len(all_targets), symbol, ensembl_id,
                     len(pmids), target_links, total_paper_links, rate)

        # Save progress every 50 targets
        if (t_idx + 1) % 50 == 0:
            progress.update({
                "targets_processed": len(processed_set),
                "targets_fetched": t_idx + 1,
                "total_pmids": total_pmids_seen,
                "paper_links": total_paper_links,
                "processed_ids": list(processed_set),
            })
            save_progress(progress)

    # Final save
    progress.update({
        "targets_processed": len(processed_set),
        "targets_fetched": len(all_targets),
        "total_pmids": total_pmids_seen,
        "paper_links": total_paper_links,
        "processed_ids": list(processed_set),
    })
    save_progress(progress)

    elapsed = time.time() - t0
    log.info("=" * 60)
    log.info("DONE — %d targets processed, %d total PMIDs, %d paper links in %.0fs",
             len(processed_set), total_pmids_seen, total_paper_links, elapsed)

    # Summary stats
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM ot_targets")
        t_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM ot_paper_targets")
        l_count = cur.fetchone()[0]
    log.info("DB totals: %d targets, %d paper-target links", t_count, l_count)

    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--targets", type=int, default=5000,
                        help="Max targets to process (default: 5000)")
    parser.add_argument("--no-resume", action="store_true",
                        help="Start fresh (ignore progress file)")
    args = parser.parse_args()
    run(max_targets=args.targets, resume=not args.no_resume)
