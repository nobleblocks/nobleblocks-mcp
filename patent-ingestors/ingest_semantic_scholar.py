#!/usr/bin/env python3
"""
Semantic Scholar Patent Citation Ingestor

Uses Semantic Scholar's API to find papers that are cited by patents.
S2 has citation context including whether citations come from patents.

Strategy:
1. Query highly-cited papers across key biotech/pharma/AI domains
2. For each paper, check its citations — some come from patents
3. Store the patent→paper citation links

Rate limit: 1 req/sec without key, 10 req/sec with key
API: https://api.semanticscholar.org/graph/v1/

Note: S2 doesn't directly expose "cited by patents" filter, but we can:
- Search papers in patent-heavy fields (biotech, pharma, materials)
- Get citation counts which correlate with patent citations
- Cross-reference with our OpenAlex data
"""

import requests
import psycopg2
import psycopg2.extras
import time
import json
import os
import sys
import signal
from datetime import datetime

# Config
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_NAME = os.environ.get("DB_NAME", "paper_search")
DB_USER = os.environ.get("DB_USER", "nobleblocks")
DB_PASS = os.environ.get("DB_PASS", "")

S2_API_KEY = os.environ.get("S2_API_KEY", "")
S2_BASE = "https://api.semanticscholar.org/graph/v1"
PROGRESS_FILE = "/tmp/s2_patent_progress.json"

# Rate limit: 1/sec without key, 10/sec with key
# Without key, S2 aggressively 429s — use 3s delay
DELAY = 3.0 if not S2_API_KEY else 0.11

# Fields we need from papers
PAPER_FIELDS = "paperId,externalIds,title,year,citationCount,fieldsOfStudy,s2FieldsOfStudy,citations.paperId,citations.externalIds,citations.title,citations.contexts"

# Patent-heavy search domains
SEARCH_QUERIES = [
    # Biotech/Pharma
    "CRISPR gene editing therapy",
    "mRNA vaccine delivery",
    "antibody drug conjugate",
    "CAR-T cell immunotherapy",
    "protein engineering directed evolution",
    "gene therapy viral vector AAV",
    "siRNA therapeutics delivery",
    "checkpoint inhibitor PD-1 PD-L1",
    "nanobody single domain antibody",
    "lipid nanoparticle drug delivery",
    # AI/ML
    "transformer neural network architecture",
    "large language model training",
    "computer vision object detection",
    "reinforcement learning robotics",
    "graph neural network molecular",
    # Materials
    "lithium battery solid state electrolyte",
    "perovskite solar cell efficiency",
    "quantum dot semiconductor",
    "carbon nanotube composite",
    "metal organic framework MOF",
    # Chemistry
    "catalytic asymmetric synthesis",
    "polymer biodegradable plastic",
    "OLED organic light emitting",
]

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
    return {"query_index": 0, "offset": 0, "total_papers": 0, "total_stored": 0}


def save_progress(progress):
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f)


def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS
    )


def s2_headers():
    h = {}
    if S2_API_KEY:
        h["x-api-key"] = S2_API_KEY
    return h


def search_papers(query, offset=0, limit=100):
    """Search for papers in patent-heavy fields."""
    params = {
        "query": query,
        "offset": offset,
        "limit": limit,
        "fields": "paperId,externalIds,title,year,citationCount,fieldsOfStudy",
    }
    for attempt in range(5):
        resp = requests.get(f"{S2_BASE}/paper/search", params=params,
                           headers=s2_headers(), timeout=30)
        if resp.status_code == 429:
            wait = 30 * (attempt + 1)
            print(f"    Rate limited (429), waiting {wait}s (attempt {attempt+1}/5)...")
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return resp.json()
    # If all attempts fail, return empty
    print("    All retry attempts exhausted, skipping query")
    return {"data": [], "total": 0}


def get_paper_citations(paper_id, offset=0, limit=100):
    """Get citations for a specific paper (who cites this paper)."""
    params = {
        "offset": offset,
        "limit": limit,
        "fields": "paperId,externalIds,title,year,contexts,intents",
    }
    resp = requests.get(f"{S2_BASE}/paper/{paper_id}/citations", params=params,
                       headers=s2_headers(), timeout=30)
    if resp.status_code == 429:
        time.sleep(60)
        resp = requests.get(f"{S2_BASE}/paper/{paper_id}/citations", params=params,
                           headers=s2_headers(), timeout=30)
    if resp.status_code == 404:
        return {"data": []}
    resp.raise_for_status()
    return resp.json()


def store_paper_citation_signal(conn, paper_doi, paper_title, citation_count, fields_of_study, year):
    """Store a high-citation-count paper as a patent citation signal candidate."""
    if not paper_doi:
        return

    cur = conn.cursor()
    cur.execute("""
        INSERT INTO patent_citation_signals (paper_doi, signal_type, signal_score, metadata)
        VALUES (%s, 'high_citation_count', %s, %s)
        ON CONFLICT (paper_doi, signal_type) DO UPDATE SET
            signal_score = GREATEST(EXCLUDED.signal_score, patent_citation_signals.signal_score),
            metadata = EXCLUDED.metadata,
            updated_at = NOW()
    """, (
        paper_doi,
        min(citation_count / 100.0, 10.0),  # Normalize to 0-10 scale
        json.dumps({
            "title": paper_title,
            "citation_count": citation_count,
            "year": year,
            "fields": fields_of_study,
            "source": "semantic_scholar",
        })
    ))
    conn.commit()
    cur.close()


def store_patent_paper_link(conn, patent_context, paper_doi, paper_title):
    """Store when we find a patent explicitly citing a paper."""
    if not paper_doi:
        return

    cur = conn.cursor()
    # We don't have the patent_id yet, but store the citation signal
    cur.execute("""
        INSERT INTO patent_citation_signals (paper_doi, signal_type, signal_score, metadata)
        VALUES (%s, 'patent_citation_context', 5.0, %s)
        ON CONFLICT (paper_doi, signal_type) DO UPDATE SET
            signal_score = GREATEST(EXCLUDED.signal_score, patent_citation_signals.signal_score),
            metadata = EXCLUDED.metadata,
            updated_at = NOW()
    """, (
        paper_doi,
        json.dumps({
            "title": paper_title,
            "context": patent_context[:500] if patent_context else None,
            "source": "semantic_scholar_citations",
        })
    ))
    conn.commit()
    cur.close()


def main():
    print("=" * 60)
    print("  Semantic Scholar Patent Citation Signal Ingestor")
    print("=" * 60)
    print(f"  DB: {DB_HOST}:{DB_PORT}/{DB_NAME}")
    print(f"  API Key: {'SET' if S2_API_KEY else 'NOT SET (1 req/s)'}")
    print(f"  Queries: {len(SEARCH_QUERIES)}")
    print()

    conn = get_db_connection()
    progress = load_progress()

    query_index = progress.get("query_index", 0)
    total_papers = progress.get("total_papers", 0)
    total_stored = progress.get("total_stored", 0)

    start_time = time.time()

    while query_index < len(SEARCH_QUERIES) and not shutdown_requested:
        query = SEARCH_QUERIES[query_index]
        print(f"\n  [{query_index+1}/{len(SEARCH_QUERIES)}] Searching: {query}")

        offset = progress.get("offset", 0) if query_index == progress.get("query_index", 0) else 0

        while not shutdown_requested:
            try:
                data = search_papers(query, offset=offset)
            except Exception as e:
                print(f"    ⚠ Search error: {e}")
                time.sleep(10)
                break

            papers = data.get("data", [])
            total_available = data.get("total", 0)

            if not papers:
                break

            for paper in papers:
                total_papers += 1
                citation_count = paper.get("citationCount", 0)

                # Only store papers with significant citations (likely patent-cited)
                if citation_count < 50:
                    continue

                doi = paper.get("externalIds", {}).get("DOI")
                title = paper.get("title", "")
                year = paper.get("year")
                fields = paper.get("fieldsOfStudy", [])

                if doi:
                    store_paper_citation_signal(conn, doi, title, citation_count, fields, year)
                    total_stored += 1

            offset += len(papers)
            time.sleep(DELAY)

            if offset >= min(total_available, 1000):  # Cap at 1000 per query
                break

            if offset % 200 == 0:
                print(f"    Progress: {offset}/{total_available} papers scanned, {total_stored} stored")

        # Move to next query
        query_index += 1
        progress["query_index"] = query_index
        progress["offset"] = 0
        progress["total_papers"] = total_papers
        progress["total_stored"] = total_stored
        save_progress(progress)

    # Final save
    progress["completed"] = query_index >= len(SEARCH_QUERIES)
    progress["last_run"] = datetime.now().isoformat()
    save_progress(progress)

    elapsed = time.time() - start_time
    print(f"\n{'=' * 60}")
    print(f"  DONE in {elapsed:.0f}s")
    print(f"  Papers scanned: {total_papers:,}")
    print(f"  Citation signals stored: {total_stored:,}")
    print(f"{'=' * 60}")

    conn.close()


if __name__ == "__main__":
    main()
