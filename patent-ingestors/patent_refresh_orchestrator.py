#!/usr/bin/env python3
"""
Patent Data Refresh Orchestrator — Continuous Ingestion System

Runs daily (or hourly for critical sources) to keep patent data fresh.
Each source has its own refresh logic with last-run tracking.

Sources:
  1. PatentsView S3 bulk (weekly — checks if new files available)
  2. USPTO Enriched Citations API (daily — new records since last run)
  3. EPO OPS (daily — new publications since last run, needs API key)
  4. DOI resolution (after each batch — resolve new citations to papers)
  5. Citation signals recomputation (daily — after new data loaded)

State is tracked in /opt/nobleblocks/paper-db/patent-ingestors/refresh_state.json

Deploy:
  1. Upload to server: aws s3 cp ... then pull on server
  2. Enable systemd timer: systemctl enable --now patent-refresh.timer

Run manually:
  DB_PASS=nb_papers_2026_prod python3 patent_refresh_orchestrator.py [--source NAME] [--force]
"""

import argparse
import json
import logging
import os
import re
import subprocess
import sys
import time
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg2
import psycopg2.extras
import requests

# ─── Config ───────────────────────────────────────────────────────────────────

DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_NAME = os.environ.get("DB_NAME", "paper_search")
DB_USER = os.environ.get("DB_USER", "nobleblocks")
DB_PASS = os.environ.get("DB_PASS", "")

STATE_FILE = os.environ.get(
    "REFRESH_STATE_FILE",
    "/opt/nobleblocks/paper-db/patent-ingestors/refresh_state.json",
)
LOG_FILE = "/var/log/patent-refresh.log"
LOCK_FILE = "/tmp/patent-refresh.lock"

# EPO OPS (set via environment or secrets)
EPO_CONSUMER_KEY = os.environ.get("EPO_CONSUMER_KEY", "")
EPO_CONSUMER_SECRET = os.environ.get("EPO_CONSUMER_SECRET", "")

# PatentsView S3
PATENTSVIEW_S3_BASE = "https://s3.amazonaws.com/data.patentsview.org/download"

# USPTO Enriched Citations
USPTO_ENRICHED_API = "https://developer.uspto.gov/ds-api/enriched_cited_reference_metadata/v3/records"

# DOI regex
DOI_PATTERN = re.compile(r"\b(10\.\d{4,9}/[^\s,;\"')\]>]+)")

# ─── Logging ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_FILE, mode="a"),
    ],
)
log = logging.getLogger("patent-refresh")

# ─── State Management ─────────────────────────────────────────────────────────


def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return json.load(f)
    return {
        "patentsview_last_check": None,
        "patentsview_last_etag": None,
        "enriched_last_offset": 0,
        "enriched_npl_last_offset": 0,
        "epo_last_date": None,
        "doi_resolution_last_run": None,
        "signals_last_run": None,
        "last_run": None,
        "runs_completed": 0,
    }


def save_state(state):
    state["last_run"] = datetime.now(timezone.utc).isoformat()
    state["runs_completed"] = state.get("runs_completed", 0) + 1
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


# ─── Database ─────────────────────────────────────────────────────────────────


def get_db():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
    )


# ─── Source 1: PatentsView S3 Weekly Check ────────────────────────────────────


def refresh_patentsview(state, force=False):
    """Check if PatentsView has new bulk files (they update weekly on Tuesdays)."""
    log.info("─── PatentsView S3 Check ───")

    # Check if new g_other_reference.tsv.zip is available via HEAD request
    url = f"{PATENTSVIEW_S3_BASE}/g_other_reference.tsv.zip"
    try:
        resp = requests.head(url, timeout=30)
        if resp.status_code != 200:
            log.warning(f"PatentsView HEAD returned {resp.status_code}")
            return False

        etag = resp.headers.get("ETag", "")
        last_modified = resp.headers.get("Last-Modified", "")

        if not force and etag == state.get("patentsview_last_etag"):
            log.info(f"No new PatentsView data (ETag unchanged: {etag})")
            return False

        log.info(f"New PatentsView data detected! ETag: {etag}, Modified: {last_modified}")

        # Run the bulk ingestor for incremental update
        script_dir = os.path.dirname(os.path.abspath(__file__))
        ingestor = os.path.join(script_dir, "ingest_patentsview_bulk.py")

        if os.path.exists(ingestor):
            log.info("Running PatentsView bulk ingestor...")
            env = os.environ.copy()
            env["DB_PASS"] = DB_PASS
            result = subprocess.run(
                [sys.executable, ingestor],
                env=env,
                capture_output=True,
                text=True,
                timeout=7200,  # 2 hour timeout
            )
            if result.returncode == 0:
                log.info("PatentsView ingestor completed successfully")
                state["patentsview_last_etag"] = etag
                state["patentsview_last_check"] = datetime.now(timezone.utc).isoformat()
                return True
            else:
                log.error(f"PatentsView ingestor failed: {result.stderr[-500:]}")
                return False
        else:
            log.warning(f"PatentsView ingestor not found at {ingestor}")
            return False

    except requests.Timeout:
        log.warning("PatentsView S3 HEAD request timed out")
        return False
    except Exception as e:
        log.error(f"PatentsView check failed: {e}")
        return False


# ─── Source 2: USPTO Enriched Citations Daily ─────────────────────────────────


def refresh_enriched_citations(state, force=False):
    """Poll USPTO Enriched Citations API for new NPL records since last run."""
    log.info("─── USPTO Enriched Citations ───")

    # Get records newer than our last offset
    last_npl_offset = state.get("enriched_npl_last_offset", 0)

    # Query for total NPL records to see if there are new ones
    try:
        resp = requests.post(
            USPTO_ENRICHED_API,
            data={"criteria": "nplIndicator:true", "start": 0, "rows": 1},
            timeout=30,
        )
        if resp.status_code != 200:
            log.warning(f"USPTO API returned {resp.status_code}")
            return False

        # Handle gzip
        try:
            content = gzip.decompress(resp.content).decode("utf-8")
        except (gzip.BadGzipFile, OSError):
            content = resp.text

        data = json.loads(content)
        total_npl = data.get("recordTotalCount", 0)

        if not force and total_npl <= last_npl_offset:
            log.info(f"No new enriched NPL records (total={total_npl}, last={last_npl_offset})")
            return False

        new_records = total_npl - last_npl_offset
        log.info(f"Found {new_records} new enriched NPL records to ingest")

        # Run the enriched ingestor (it has its own progress tracking)
        script_dir = os.path.dirname(os.path.abspath(__file__))
        ingestor = os.path.join(script_dir, "ingest_enriched_citations.py")

        if os.path.exists(ingestor):
            env = os.environ.copy()
            env["DB_PASS"] = DB_PASS
            result = subprocess.run(
                [sys.executable, ingestor],
                env=env,
                capture_output=True,
                text=True,
                timeout=3600,  # 1 hour timeout
            )
            if result.returncode == 0:
                state["enriched_npl_last_offset"] = total_npl
                log.info("Enriched citations ingestor completed")
                return True
            else:
                log.error(f"Enriched ingestor failed: {result.stderr[-500:]}")
                return False

        return False

    except Exception as e:
        log.error(f"Enriched citations check failed: {e}")
        return False


# ─── Source 3: EPO OPS Daily ──────────────────────────────────────────────────


def get_epo_token():
    """Get EPO OPS OAuth2 access token."""
    if not EPO_CONSUMER_KEY or not EPO_CONSUMER_SECRET:
        return None

    import base64

    auth = base64.b64encode(f"{EPO_CONSUMER_KEY}:{EPO_CONSUMER_SECRET}".encode()).decode()
    resp = requests.post(
        "https://ops.epo.org/3.2/auth/accesstoken",
        headers={"Authorization": f"Basic {auth}", "Content-Type": "application/x-www-form-urlencoded"},
        data="grant_type=client_credentials",
        timeout=30,
    )
    if resp.status_code == 200:
        return resp.json()["access_token"]
    log.error(f"EPO auth failed: {resp.status_code} {resp.text[:200]}")
    return None


def refresh_epo(state, force=False):
    """Pull new European patent publications from EPO OPS API."""
    log.info("─── EPO OPS ───")

    if not EPO_CONSUMER_KEY:
        log.info("EPO credentials not configured — skipping")
        return False

    token = get_epo_token()
    if not token:
        return False

    # Get publications since last run (or last 7 days if first run)
    last_date = state.get("epo_last_date")
    if not last_date or force:
        start_date = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y%m%d")
    else:
        start_date = last_date

    end_date = datetime.now(timezone.utc).strftime("%Y%m%d")

    log.info(f"Fetching EPO publications from {start_date} to {end_date}")

    conn = get_db()
    cur = conn.cursor()
    total_inserted = 0
    page = 1

    try:
        while True:
            # EPO OPS published-data/search
            range_str = f"{(page-1)*100 + 1}-{page*100}"
            url = f"https://ops.epo.org/3.2/rest-services/published-data/search"
            params = {
                "q": f"pd={start_date} {end_date}",
                "Range": range_str,
            }
            headers = {
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
            }

            resp = requests.get(url, params=params, headers=headers, timeout=60)

            if resp.status_code == 404:
                log.info("No more EPO results")
                break
            elif resp.status_code == 403:
                log.warning("EPO rate limit hit — backing off 60s")
                time.sleep(60)
                token = get_epo_token()
                if not token:
                    break
                continue
            elif resp.status_code != 200:
                log.warning(f"EPO returned {resp.status_code}")
                break

            data = resp.json()

            # Parse results
            results = (
                data.get("ops:world-patent-data", {})
                .get("ops:biblio-search", {})
                .get("ops:search-result", {})
                .get("ops:publication-reference", [])
            )

            if not results:
                break

            if not isinstance(results, list):
                results = [results]

            batch = []
            for pub_ref in results:
                doc_id = pub_ref.get("document-id", {})
                if isinstance(doc_id, list):
                    doc_id = doc_id[0]

                country = doc_id.get("country", {}).get("$", "")
                doc_number = doc_id.get("doc-number", {}).get("$", "")
                kind = doc_id.get("kind", {}).get("$", "")

                patent_id = f"{country}{doc_number}{kind}"

                batch.append((
                    patent_id,  # patent_id
                    None,  # title (fetched separately if needed)
                    None,  # abstract
                    None,  # claims
                    None,  # filing_date
                    None,  # grant_date
                    None,  # assignee
                    None,  # assignee_type
                    None,  # inventors
                    None,  # ipc_codes
                    None,  # cpc_codes
                    country,  # jurisdiction
                    None,  # legal_status
                    None,  # patent_family_id
                    "epo",  # source
                ))

            if batch:
                psycopg2.extras.execute_values(
                    cur,
                    """INSERT INTO patents (patent_id, title, abstract, claims_text,
                       filing_date, grant_date, assignee, assignee_type, inventors,
                       ipc_codes, cpc_codes, jurisdiction, legal_status, patent_family_id, source)
                       VALUES %s ON CONFLICT (patent_id) DO NOTHING""",
                    batch,
                )
                total_inserted += cur.rowcount
                conn.commit()

            page += 1
            time.sleep(1)  # EPO rate limit: ~10 req/sec, be conservative

            if page > 100:  # Safety cap: 10K patents per run
                break

    except Exception as e:
        log.error(f"EPO ingest error: {e}")
        conn.rollback()

    finally:
        cur.close()
        conn.close()

    log.info(f"EPO: {total_inserted} new patents inserted")
    state["epo_last_date"] = end_date
    return total_inserted > 0


# ─── Source 4: DOI Resolution ─────────────────────────────────────────────────


def resolve_new_dois(state, force=False):
    """Resolve any unresolved DOIs in patent_paper_citations to paper IDs."""
    log.info("─── DOI Resolution ───")

    conn = get_db()
    cur = conn.cursor()

    try:
        # Count unresolved
        cur.execute(
            "SELECT COUNT(*) FROM patent_paper_citations WHERE paper_doi IS NOT NULL AND paper_id IS NULL"
        )
        unresolved = cur.fetchone()[0]

        if unresolved == 0 and not force:
            log.info("No unresolved DOIs")
            return False

        log.info(f"Resolving {unresolved} unlinked DOIs...")

        # Batch resolve in chunks to avoid long-running transactions
        cur.execute("""
            UPDATE patent_paper_citations ppc
            SET paper_id = p.id, paper_title = p.title
            FROM papers p
            WHERE ppc.paper_doi = p.doi
              AND ppc.paper_id IS NULL
              AND ppc.paper_doi IS NOT NULL
        """)
        resolved = cur.rowcount
        conn.commit()

        log.info(f"Resolved {resolved} / {unresolved} DOIs to papers")
        state["doi_resolution_last_run"] = datetime.now(timezone.utc).isoformat()
        return resolved > 0

    except Exception as e:
        log.error(f"DOI resolution failed: {e}")
        conn.rollback()
        return False
    finally:
        cur.close()
        conn.close()


# ─── Source 5: Citation Signals Recomputation ─────────────────────────────────


def recompute_signals(state, force=False):
    """Recompute patent citation signals for all resolved papers."""
    log.info("─── Citation Signals ───")

    conn = get_db()
    cur = conn.cursor()

    try:
        # Delete previous all-time window (we'll recompute fresh)
        cur.execute(
            "DELETE FROM patent_citation_signals WHERE window_start = '1900-01-01'"
        )
        deleted = cur.rowcount
        log.info(f"Cleared {deleted} previous all-time signals")

        # Compute fresh signals
        cur.execute("""
            INSERT INTO patent_citation_signals
                (paper_id, paper_doi, window_start, window_end,
                 patent_citations_count, velocity_score, is_spike)
            SELECT
                ppc.paper_id,
                ppc.paper_doi,
                '1900-01-01'::date AS window_start,
                CURRENT_DATE AS window_end,
                COUNT(DISTINCT ppc.patent_id) AS patent_citations_count,
                COUNT(DISTINCT ppc.patent_id) FILTER (
                    WHERE p.filing_date > CURRENT_DATE - interval '2 years'
                ) AS velocity_score,
                (COUNT(DISTINCT ppc.patent_id) >= 5) AS is_spike
            FROM patent_paper_citations ppc
            LEFT JOIN patents p ON p.patent_id = ppc.patent_id
            WHERE ppc.paper_id IS NOT NULL
            GROUP BY ppc.paper_id, ppc.paper_doi
        """)
        inserted = cur.rowcount
        conn.commit()

        log.info(f"Computed signals for {inserted} papers")

        # Count spikes
        cur.execute("SELECT COUNT(*) FROM patent_citation_signals WHERE is_spike = TRUE")
        spikes = cur.fetchone()[0]
        log.info(f"Spike papers (5+ patent citations): {spikes}")

        state["signals_last_run"] = datetime.now(timezone.utc).isoformat()
        return True

    except Exception as e:
        log.error(f"Signal computation failed: {e}")
        conn.rollback()
        return False
    finally:
        cur.close()
        conn.close()


# ─── Lock File ────────────────────────────────────────────────────────────────


def acquire_lock():
    if os.path.exists(LOCK_FILE):
        # Check if PID is still alive
        try:
            with open(LOCK_FILE) as f:
                pid = int(f.read().strip())
            os.kill(pid, 0)  # Check if process exists
            return False  # Still running
        except (ProcessLookupError, ValueError):
            pass  # Stale lock

    with open(LOCK_FILE, "w") as f:
        f.write(str(os.getpid()))
    return True


def release_lock():
    if os.path.exists(LOCK_FILE):
        os.unlink(LOCK_FILE)


# ─── Main Orchestrator ────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(description="Patent Data Refresh Orchestrator")
    parser.add_argument("--source", choices=["patentsview", "enriched", "epo", "resolve", "signals", "all"],
                        default="all", help="Which source to refresh")
    parser.add_argument("--force", action="store_true", help="Force refresh even if no new data")
    args = parser.parse_args()

    if not acquire_lock():
        log.warning("Another refresh instance is running — exiting")
        sys.exit(0)

    try:
        state = load_state()
        log.info("=" * 60)
        log.info(f"Patent Refresh Orchestrator — {datetime.now(timezone.utc).isoformat()}")
        log.info(f"Last run: {state.get('last_run', 'never')}")
        log.info(f"Total runs completed: {state.get('runs_completed', 0)}")
        log.info("=" * 60)

        results = {}

        if args.source in ("patentsview", "all"):
            results["patentsview"] = refresh_patentsview(state, args.force)

        if args.source in ("enriched", "all"):
            results["enriched"] = refresh_enriched_citations(state, args.force)

        if args.source in ("epo", "all"):
            results["epo"] = refresh_epo(state, args.force)

        if args.source in ("resolve", "all"):
            results["resolve"] = resolve_new_dois(state, args.force)

        if args.source in ("signals", "all"):
            results["signals"] = recompute_signals(state, args.force)

        # Summary
        log.info("=" * 60)
        log.info("REFRESH SUMMARY:")
        for source, updated in results.items():
            status = "✓ UPDATED" if updated else "· no change"
            log.info(f"  {source}: {status}")
        log.info("=" * 60)

        save_state(state)

    except Exception as e:
        log.error(f"Orchestrator failed: {traceback.format_exc()}")
        sys.exit(1)
    finally:
        release_lock()


if __name__ == "__main__":
    main()
