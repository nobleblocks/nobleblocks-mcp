#!/usr/bin/env python3
"""
Lens.org Scholarly API Ingestor

Fetches patent-paper citation links from Lens.org's Scholarly API.
Lens aggregates patent citations to scholarly works across all major jurisdictions.

Source: https://docs.api.lens.org/
Auth: API key (free for non-commercial use, register at lens.org/lens/user/subscriptions)
Rate limit: 50 req/min (free tier), 10K results per query

This ingestor focuses on:
1. Scholarly works cited by patents (patent_citations field)
2. Multi-jurisdiction coverage (US, EP, WO, JP, KR, CN, etc.)
3. DOI-linked citations that map to our papers table

Deploy:
  DB_PASS=nb_papers_2026_prod LENS_API_KEY=<key> python3 ingest_lens_scholarly.py [--since YYYY-MM-DD] [--limit N]
"""

import requests
import psycopg2
import psycopg2.extras
import time
import json
import os
import sys
import re
import signal
import logging
from datetime import datetime, timedelta

# Config
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_NAME = os.environ.get("DB_NAME", "paper_search")
DB_USER = os.environ.get("DB_USER", "nobleblocks")
DB_PASS = os.environ.get("DB_PASS", "")

LENS_API_KEY = os.environ.get("LENS_API_KEY", "")
LENS_API_URL = "https://api.lens.org/scholarly/search"
LENS_PATENT_URL = "https://api.lens.org/patent/search"

BATCH_SIZE = 100  # Lens max per request
MAX_RESULTS = 10000  # Lens free tier limit per query
RATE_LIMIT_DELAY = 1.2  # seconds between requests (50/min = 1.2s)
PROGRESS_FILE = "/tmp/lens_ingest_progress.json"

DOI_PATTERN = re.compile(r'\b(10\.\d{4,9}/[^\s,;"\')\]>]+)')

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("/tmp/lens_ingest.log", mode="a"),
    ],
)
log = logging.getLogger("lens-ingest")

# Graceful shutdown
shutdown_requested = False
def handle_signal(sig, frame):
    global shutdown_requested
    shutdown_requested = True
    log.warning("Shutdown requested, finishing current batch...")

signal.signal(signal.SIGTERM, handle_signal)
signal.signal(signal.SIGINT, handle_signal)


def load_progress():
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE) as f:
            return json.load(f)
    return {"last_date": "2020-01-01", "total_records": 0, "offset": 0}


def save_progress(progress):
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f)


def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS
    )


class LensClient:
    """Lens.org Scholarly API client."""

    def __init__(self, api_key):
        if not api_key:
            raise ValueError("LENS_API_KEY environment variable must be set")
        self.api_key = api_key
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        })
        self.last_request_time = 0

    def _rate_limit(self):
        """Enforce rate limiting."""
        elapsed = time.time() - self.last_request_time
        if elapsed < RATE_LIMIT_DELAY:
            time.sleep(RATE_LIMIT_DELAY - elapsed)
        self.last_request_time = time.time()

    def search_patent_cited_works(self, since_date, offset=0, size=100):
        """
        Search for scholarly works that are cited by patents.
        Returns works with their patent citation metadata.
        """
        self._rate_limit()

        query = {
            "query": {
                "bool": {
                    "must": [
                        {"range": {"date_published": {"gte": since_date}}},
                        {"range": {"scholarly_citations_count": {"gte": 1}}},
                    ],
                    "should": [
                        {"exists": {"field": "referenced_by_patent"}},
                    ],
                    "minimum_should_match": 1,
                }
            },
            "size": size,
            "from": offset,
            "include": [
                "lens_id",
                "title",
                "doi",
                "date_published",
                "authors",
                "source",
                "referenced_by_patent",
                "scholarly_citations_count",
                "external_ids",
            ],
            "sort": [{"date_published": "desc"}],
        }

        try:
            resp = self.session.post(LENS_API_URL, json=query, timeout=60)
            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", 60))
                log.warning(f"Rate limited, waiting {retry_after}s")
                time.sleep(retry_after)
                return self.search_patent_cited_works(since_date, offset, size)
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.RequestException as e:
            log.error(f"Lens API error: {e}")
            return None

    def search_patents_citing_doi(self, doi, offset=0, size=100):
        """Search for patents that cite a specific DOI."""
        self._rate_limit()

        query = {
            "query": {
                "bool": {
                    "must": [
                        {"match": {"biblio.references.npl_resolved.external_ids": doi}},
                    ]
                }
            },
            "size": size,
            "from": offset,
            "include": [
                "lens_id",
                "doc_number",
                "kind",
                "jurisdiction",
                "title",
                "date_published",
                "biblio.parties.applicants",
                "biblio.classifications_ipcr",
                "biblio.references.npl_resolved",
            ],
        }

        try:
            resp = self.session.post(LENS_PATENT_URL, json=query, timeout=60)
            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", 60))
                log.warning(f"Rate limited, waiting {retry_after}s")
                time.sleep(retry_after)
                return self.search_patents_citing_doi(doi, offset, size)
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.RequestException as e:
            log.error(f"Lens Patent API error: {e}")
            return None


def extract_citations_from_work(work):
    """Extract patent citation links from a Lens scholarly work record."""
    citations = []
    doi = work.get("doi") or ""
    if not doi:
        # Try external_ids
        for ext in work.get("external_ids", []):
            if ext.get("type") == "doi":
                doi = ext.get("value", "")
                break

    if not doi:
        return citations

    doi = doi.lower().strip()
    patent_refs = work.get("referenced_by_patent", [])

    for patent_ref in patent_refs:
        patent_id = patent_ref.get("lens_id", "")
        jurisdiction = patent_ref.get("jurisdiction", "")
        doc_number = patent_ref.get("doc_number", "")

        if not patent_id and doc_number:
            patent_id = f"{jurisdiction}{doc_number}"

        if patent_id:
            citations.append({
                "patent_id": patent_id,
                "paper_doi": doi,
                "citation_type": "npl",
                "source": "lens",
                "jurisdiction": jurisdiction,
            })

    return citations


def insert_citations_batch(conn, citations):
    """Insert patent-paper citation links in batch."""
    if not citations:
        return 0

    cur = conn.cursor()
    inserted = 0

    # Use batch insert with ON CONFLICT
    values = []
    for c in citations:
        values.append((
            c["patent_id"],
            c["paper_doi"],
            c.get("citation_context", ""),
            c["citation_type"],
            c["source"],
        ))

    if values:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO patent_paper_citations (patent_id, paper_doi, citation_context, citation_type, source)
            VALUES %s
            ON CONFLICT (patent_id, paper_doi) DO NOTHING
            """,
            values,
            page_size=500,
        )
        inserted = cur.rowcount
        conn.commit()

    cur.close()
    return inserted


def resolve_dois_to_papers(conn, dois):
    """Link citations to actual paper IDs via DOI lookup."""
    if not dois:
        return 0

    cur = conn.cursor()
    cur.execute("""
        UPDATE patent_paper_citations pc
        SET paper_id = p.id
        FROM papers p
        WHERE pc.paper_doi = p.doi
          AND pc.paper_id IS NULL
          AND pc.paper_doi = ANY(%s)
    """, (list(dois),))
    resolved = cur.rowcount
    conn.commit()
    cur.close()
    return resolved


def run_scholarly_ingest(since_date, max_records=None):
    """Main ingestion loop: fetch scholarly works cited by patents."""
    if not LENS_API_KEY:
        log.error("LENS_API_KEY not set. Register at https://www.lens.org/lens/user/subscriptions")
        sys.exit(1)

    client = LensClient(LENS_API_KEY)
    conn = get_db_connection()
    progress = load_progress()

    # Resume from last offset if same date
    offset = progress.get("offset", 0) if progress.get("last_date") == since_date else 0
    total_citations = 0
    total_resolved = 0
    batch_num = 0
    start_time = time.time()

    log.info(f"Starting Lens.org scholarly ingest since {since_date}, offset={offset}")

    while True:
        if shutdown_requested:
            log.info("Shutdown requested, saving progress")
            break

        if max_records and total_citations >= max_records:
            log.info(f"Reached max_records limit ({max_records})")
            break

        if offset >= MAX_RESULTS:
            # Lens free tier caps at 10K results per query
            # Advance date window
            log.info(f"Reached 10K offset limit, advancing date window")
            break

        result = client.search_patent_cited_works(since_date, offset=offset, size=BATCH_SIZE)
        if not result:
            log.warning("Empty response from Lens API, retrying in 30s")
            time.sleep(30)
            continue

        total_hits = result.get("total", 0)
        data = result.get("data", [])

        if not data:
            log.info(f"No more results (total hits: {total_hits})")
            break

        # Process batch
        all_citations = []
        all_dois = set()

        for work in data:
            citations = extract_citations_from_work(work)
            all_citations.extend(citations)
            for c in citations:
                if c.get("paper_doi"):
                    all_dois.add(c["paper_doi"])

        # Insert citations
        inserted = insert_citations_batch(conn, all_citations)
        total_citations += inserted

        # Resolve DOIs to paper IDs
        resolved = resolve_dois_to_papers(conn, all_dois)
        total_resolved += resolved

        offset += len(data)
        batch_num += 1

        # Progress logging
        if batch_num % 10 == 0:
            elapsed = time.time() - start_time
            log.info(
                f"Batch {batch_num}: offset={offset}/{total_hits}, "
                f"citations={total_citations}, resolved={total_resolved}, "
                f"elapsed={elapsed:.0f}s"
            )

        # Save progress
        progress["offset"] = offset
        progress["last_date"] = since_date
        progress["total_records"] = progress.get("total_records", 0) + inserted
        save_progress(progress)

        time.sleep(RATE_LIMIT_DELAY)

    # Final stats
    elapsed = time.time() - start_time
    log.info(
        f"COMPLETE: {total_citations} citations inserted, "
        f"{total_resolved} resolved to papers, "
        f"{elapsed:.0f}s elapsed"
    )

    conn.close()
    return total_citations


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Lens.org Patent-Paper Citation Ingestor")
    parser.add_argument("--since", default=None,
                        help="Ingest works published since this date (YYYY-MM-DD)")
    parser.add_argument("--limit", type=int, default=None,
                        help="Max citations to ingest")
    parser.add_argument("--resume", action="store_true",
                        help="Resume from last saved progress")
    args = parser.parse_args()

    if args.resume:
        progress = load_progress()
        since_date = progress.get("last_date", "2020-01-01")
    elif args.since:
        since_date = args.since
    else:
        # Default: last 30 days
        since_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

    log.info("=" * 70)
    log.info("LENS.ORG SCHOLARLY PATENT CITATION INGESTOR")
    log.info(f"Since: {since_date}, Limit: {args.limit or 'none'}")
    log.info("=" * 70)

    run_scholarly_ingest(since_date, max_records=args.limit)


if __name__ == "__main__":
    main()
