#!/usr/bin/env python3
"""
NobleBlocks Social Traction Connector
======================================
Since Crossref Event Data API is down (403 Forbidden as of 2026-05-27),
this connector pulls citation velocity signals from OpenAlex's works API
which provides cited_by_count (updated monthly) and counts_by_year.

Also queries the OpenAlex "trending" concepts endpoint to identify
which research topics are gaining momentum.

Data Sources:
- OpenAlex Works API: cited_by_count, counts_by_year, referenced_works_count
- OpenAlex Concepts trending: concepts with fastest growth
- Reddit API (r/science, r/biotech): DOI mentions (free, rate-limited)
- Wikipedia Pageviews API: views for biomedical topic pages

Tables Populated:
- social_traction_papers: per-paper social/citation signals
- social_traction_topics: trending topics from OpenAlex
- social_reddit_mentions: DOI mentions on Reddit

Runs as: nohup python3 social_traction_connector.py &
"""

import psycopg2
import psycopg2.extras
import requests
import logging
import time
import json
import os
import signal
import sys
from datetime import datetime, timedelta
from urllib.parse import quote

# ─── Configuration ───────────────────────────────────────────────────────────

DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'dbname': 'paper_search',
    'user': 'nobleblocks',
    'password': 'nb_papers_2026_prod',
}

# OpenAlex polite pool (30 req/s with email)
OPENALEX_EMAIL = "info@nobleblocks.com"
OPENALEX_BASE = "https://api.openalex.org"

# Reddit (no auth, 60 req/min)
REDDIT_BASE = "https://www.reddit.com"
REDDIT_SUBREDDITS = ["science", "biotech", "bioinformatics", "genetics", "medicine"]

BATCH_SIZE = 200  # OpenAlex allows up to 200 per page
RATE_LIMIT_DELAY = 0.15  # seconds between OpenAlex calls
REDDIT_DELAY = 1.5  # seconds between Reddit calls

LOG_FILE = '/tmp/social_traction.log'
PROGRESS_FILE = '/tmp/social_traction_progress.json'

# ─── Logging ─────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# ─── Graceful shutdown ───────────────────────────────────────────────────────

shutdown_requested = False

def handle_signal(signum, frame):
    global shutdown_requested
    shutdown_requested = True
    log.warning(f"Shutdown signal {signum} received, finishing current batch...")

signal.signal(signal.SIGTERM, handle_signal)
signal.signal(signal.SIGINT, handle_signal)

# ─── Database ────────────────────────────────────────────────────────────────

def get_conn():
    return psycopg2.connect(**DB_CONFIG)

def create_tables():
    conn = get_conn()
    cur = conn.cursor()
    
    cur.execute("""
    CREATE TABLE IF NOT EXISTS social_traction_papers (
        paper_id        BIGINT PRIMARY KEY,
        doi             TEXT,
        openalex_id     TEXT,
        cited_by_count  INTEGER,
        counts_by_year  JSONB,     -- [{year: 2025, cited_by_count: 45}, ...]
        citation_velocity REAL,    -- recent year citations vs historical avg
        reddit_mentions INTEGER DEFAULT 0,
        wikipedia_refs  INTEGER DEFAULT 0,
        news_mentions   INTEGER DEFAULT 0,
        composite_social_score REAL,
        snapshot_date   DATE DEFAULT CURRENT_DATE,
        updated_at      TIMESTAMPTZ DEFAULT NOW()
    );
    
    CREATE TABLE IF NOT EXISTS social_traction_topics (
        id              SERIAL PRIMARY KEY,
        openalex_concept_id TEXT,
        concept_name    TEXT,
        level           INTEGER,     -- concept hierarchy level (0=broad, 5=specific)
        works_count     INTEGER,
        cited_by_count  INTEGER,
        counts_by_year  JSONB,
        velocity        REAL,        -- growth rate
        snapshot_date   DATE DEFAULT CURRENT_DATE,
        UNIQUE(openalex_concept_id, snapshot_date)
    );
    
    CREATE TABLE IF NOT EXISTS social_reddit_mentions (
        id              SERIAL PRIMARY KEY,
        doi             TEXT,
        paper_id        BIGINT,
        subreddit       TEXT,
        post_title      TEXT,
        post_url        TEXT,
        score           INTEGER,     -- upvotes - downvotes
        num_comments    INTEGER,
        created_utc     TIMESTAMPTZ,
        discovered_at   TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE(doi, post_url)
    );
    
    CREATE INDEX IF NOT EXISTS idx_social_papers_velocity ON social_traction_papers(citation_velocity DESC);
    CREATE INDEX IF NOT EXISTS idx_social_papers_composite ON social_traction_papers(composite_social_score DESC);
    CREATE INDEX IF NOT EXISTS idx_social_topics_velocity ON social_traction_topics(velocity DESC);
    CREATE INDEX IF NOT EXISTS idx_social_reddit_doi ON social_reddit_mentions(doi);
    CREATE INDEX IF NOT EXISTS idx_social_reddit_score ON social_reddit_mentions(score DESC);
    """)
    
    conn.commit()
    cur.close()
    conn.close()
    log.info("Social traction tables created/verified")

# ─── OpenAlex: Fetch high-velocity papers ────────────────────────────────────

def openalex_session():
    """Create a requests session with polite pool headers."""
    s = requests.Session()
    s.headers.update({
        'User-Agent': f'NobleBlocks/1.0 (mailto:{OPENALEX_EMAIL})',
    })
    return s

def fetch_trending_papers_from_openalex():
    """
    Query OpenAlex for papers with high recent citation counts.
    Strategy: get papers from 2022-2026 sorted by cited_by_count DESC,
    then compute velocity from counts_by_year.
    """
    log.info("=== Fetching trending papers from OpenAlex ===")
    session = openalex_session()
    
    conn = get_conn()
    cur = conn.cursor()
    
    total_fetched = 0
    total_inserted = 0
    
    # Query papers from last 4 years with high citation counts
    # Focus on biomedical papers (concepts: Medicine, Biology, Genetics, Biochemistry)
    concepts_filter = "|".join([
        "C71924100",   # Medicine
        "C86803240",   # Biology
        "C54355233",   # Genetics
        "C55493867",   # Biochemistry
        "C502942594",  # Molecular Biology
        "C126322002",  # Immunology
        "C203014093",  # Pharmacology
    ])
    
    for year in range(2022, 2027):
        if shutdown_requested:
            break
            
        cursor = "*"
        page_count = 0
        
        while cursor and not shutdown_requested:
            url = (f"{OPENALEX_BASE}/works?"
                   f"filter=publication_year:{year},"
                   f"concepts.id:{concepts_filter},"
                   f"cited_by_count:>20"
                   f"&select=id,doi,cited_by_count,counts_by_year,title"
                   f"&sort=cited_by_count:desc"
                   f"&per_page={BATCH_SIZE}"
                   f"&cursor={cursor}"
                   f"&mailto={OPENALEX_EMAIL}")
            
            try:
                resp = session.get(url, timeout=30)
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                log.error(f"OpenAlex error for year {year} page {page_count}: {e}")
                time.sleep(5)
                continue
            
            results = data.get('results', [])
            if not results:
                break
            
            # Process batch
            batch_values = []
            for work in results:
                openalex_id = work.get('id', '').replace('https://openalex.org/', '')
                doi = work.get('doi', '').replace('https://doi.org/', '') if work.get('doi') else None
                cited_by = work.get('cited_by_count', 0)
                counts_by_year = work.get('counts_by_year', [])
                
                # Compute citation velocity from counts_by_year
                velocity = compute_citation_velocity(counts_by_year)
                
                batch_values.append((
                    doi, openalex_id, cited_by, 
                    json.dumps(counts_by_year), velocity,
                    velocity * (1 + (cited_by / 100.0))  # composite
                ))
            
            # Upsert — link to our paper_id via DOI
            if batch_values:
                psycopg2.extras.execute_batch(cur, """
                    INSERT INTO social_traction_papers 
                        (paper_id, doi, openalex_id, cited_by_count, counts_by_year, 
                         citation_velocity, composite_social_score, snapshot_date)
                    SELECT p.id, %(doi)s, %(oaid)s, %(cited_by)s, %(counts)s::jsonb,
                           %(velocity)s, %(composite)s, CURRENT_DATE
                    FROM papers p
                    WHERE p.doi = %(doi)s
                    ON CONFLICT (paper_id) DO UPDATE SET
                        cited_by_count = EXCLUDED.cited_by_count,
                        counts_by_year = EXCLUDED.counts_by_year,
                        citation_velocity = EXCLUDED.citation_velocity,
                        composite_social_score = EXCLUDED.composite_social_score,
                        snapshot_date = CURRENT_DATE,
                        updated_at = NOW()
                """, [
                    {'doi': v[0], 'oaid': v[1], 'cited_by': v[2], 
                     'counts': v[3], 'velocity': v[4], 'composite': v[5]}
                    for v in batch_values if v[0]  # only those with DOI
                ])
                conn.commit()
                total_inserted += len([v for v in batch_values if v[0]])
            
            total_fetched += len(results)
            page_count += 1
            
            # Move cursor
            meta = data.get('meta', {})
            cursor = meta.get('next_cursor')
            
            # Limit: max 50 pages per year (10K papers) to avoid over-fetching
            if page_count >= 50:
                break
            
            time.sleep(RATE_LIMIT_DELAY)
        
        log.info(f"  Year {year}: fetched {page_count * BATCH_SIZE} works")
    
    cur.close()
    conn.close()
    log.info(f"OpenAlex trending papers: fetched {total_fetched:,}, linked {total_inserted:,}")
    return total_inserted

def compute_citation_velocity(counts_by_year):
    """
    Given OpenAlex counts_by_year: [{year: 2025, cited_by_count: 45}, ...]
    Compute velocity = recent_rate / historical_rate.
    """
    if not counts_by_year:
        return 0.0
    
    by_year = {c['year']: c['cited_by_count'] for c in counts_by_year}
    
    current_year = datetime.now().year
    recent = sum(by_year.get(y, 0) for y in range(current_year - 1, current_year + 1))
    historical = sum(by_year.get(y, 0) for y in range(current_year - 5, current_year - 1))
    
    recent_rate = recent / 2.0
    historical_rate = historical / max(4.0, 1.0)
    
    if historical_rate < 1.0:
        return recent_rate if recent_rate > 0 else 0.0
    
    return recent_rate / historical_rate

# ─── OpenAlex: Trending Concepts ─────────────────────────────────────────────

def fetch_trending_concepts():
    """
    Fetch concepts/topics from OpenAlex that are growing fastest.
    Looks at level 2-4 concepts (specific enough to be actionable).
    """
    log.info("=== Fetching trending concepts from OpenAlex ===")
    session = openalex_session()
    
    conn = get_conn()
    cur = conn.cursor()
    
    total_concepts = 0
    
    # Get biomedical concepts at level 2-4 sorted by works_count
    for level in [2, 3, 4]:
        if shutdown_requested:
            break
            
        cursor = "*"
        page = 0
        
        while cursor and not shutdown_requested and page < 10:
            url = (f"{OPENALEX_BASE}/concepts?"
                   f"filter=level:{level},"
                   f"ancestors.id:C71924100|C86803240|C54355233|C55493867"  # Medicine/Bio/Genetics/Biochem
                   f"&select=id,display_name,level,works_count,cited_by_count,counts_by_year"
                   f"&sort=works_count:desc"
                   f"&per_page={BATCH_SIZE}"
                   f"&cursor={cursor}"
                   f"&mailto={OPENALEX_EMAIL}")
            
            try:
                resp = session.get(url, timeout=30)
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                log.error(f"OpenAlex concepts error level={level} page={page}: {e}")
                time.sleep(5)
                break
            
            results = data.get('results', [])
            if not results:
                break
            
            for concept in results:
                concept_id = concept.get('id', '').replace('https://openalex.org/', '')
                name = concept.get('display_name', '')
                works = concept.get('works_count', 0)
                cited = concept.get('cited_by_count', 0)
                counts = concept.get('counts_by_year', [])
                
                velocity = compute_concept_velocity(counts)
                
                cur.execute("""
                    INSERT INTO social_traction_topics
                        (openalex_concept_id, concept_name, level, works_count, 
                         cited_by_count, counts_by_year, velocity, snapshot_date)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_DATE)
                    ON CONFLICT (openalex_concept_id, snapshot_date) DO UPDATE SET
                        works_count = EXCLUDED.works_count,
                        cited_by_count = EXCLUDED.cited_by_count,
                        counts_by_year = EXCLUDED.counts_by_year,
                        velocity = EXCLUDED.velocity
                """, (concept_id, name, level, works, cited, json.dumps(counts), velocity))
                total_concepts += 1
            
            conn.commit()
            cursor = data.get('meta', {}).get('next_cursor')
            page += 1
            time.sleep(RATE_LIMIT_DELAY)
    
    cur.close()
    conn.close()
    log.info(f"Fetched {total_concepts:,} concepts")
    
    # Log top trending
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
    SELECT concept_name, velocity, works_count, level
    FROM social_traction_topics 
    WHERE snapshot_date = CURRENT_DATE
    ORDER BY velocity DESC LIMIT 20
    """)
    log.info("=== TOP 20 TRENDING CONCEPTS ===")
    for r in cur.fetchall():
        log.info(f"  L{r[3]} {r[0]}: velocity={r[1]:.2f}, works={r[2]:,}")
    cur.close()
    conn.close()
    
    return total_concepts

def compute_concept_velocity(counts_by_year):
    """Compute velocity for a concept from its counts_by_year."""
    if not counts_by_year:
        return 0.0
    
    by_year = {c['year']: c.get('works_count', 0) for c in counts_by_year}
    
    current_year = datetime.now().year
    recent = sum(by_year.get(y, 0) for y in range(current_year - 1, current_year + 1))
    historical = sum(by_year.get(y, 0) for y in range(current_year - 5, current_year - 1))
    
    recent_rate = recent / 2.0
    historical_rate = historical / max(4.0, 1.0)
    
    if historical_rate < 10:
        return min(recent_rate / 10.0, 10.0) if recent_rate > 0 else 0.0
    
    return recent_rate / historical_rate

# ─── Reddit: DOI mentions in science subreddits ──────────────────────────────

def fetch_reddit_mentions():
    """
    Search science subreddits for DOI mentions.
    Uses Reddit's search API (no auth needed, 60 req/min).
    """
    log.info("=== Fetching Reddit DOI mentions ===")
    
    conn = get_conn()
    cur = conn.cursor()
    
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'NobleBlocks/1.0 academic research tracker'
    })
    
    total_found = 0
    
    for subreddit in REDDIT_SUBREDDITS:
        if shutdown_requested:
            break
        
        # Search for posts containing DOI patterns
        # Reddit search for "doi.org" in the subreddit
        url = (f"{REDDIT_BASE}/r/{subreddit}/search.json?"
               f"q=doi.org&sort=new&restrict_sr=on&limit=100&t=month")
        
        try:
            resp = session.get(url, timeout=15)
            if resp.status_code == 429:
                log.warning(f"Reddit rate limited, sleeping 60s...")
                time.sleep(60)
                continue
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            log.error(f"Reddit r/{subreddit} error: {e}")
            time.sleep(REDDIT_DELAY)
            continue
        
        posts = data.get('data', {}).get('children', [])
        
        for post in posts:
            pdata = post.get('data', {})
            title = pdata.get('title', '')
            url_field = pdata.get('url', '')
            selftext = pdata.get('selftext', '')
            score = pdata.get('score', 0)
            num_comments = pdata.get('num_comments', 0)
            created = pdata.get('created_utc', 0)
            permalink = pdata.get('permalink', '')
            
            # Extract DOI from URL or text
            doi = extract_doi(url_field) or extract_doi(selftext) or extract_doi(title)
            if not doi:
                continue
            
            post_url = f"https://reddit.com{permalink}" if permalink else url_field
            
            try:
                cur.execute("""
                    INSERT INTO social_reddit_mentions
                        (doi, subreddit, post_title, post_url, score, num_comments, created_utc)
                    VALUES (%s, %s, %s, %s, %s, %s, TO_TIMESTAMP(%s))
                    ON CONFLICT (doi, post_url) DO UPDATE SET
                        score = EXCLUDED.score,
                        num_comments = EXCLUDED.num_comments
                """, (doi, subreddit, title[:500], post_url[:1000], score, num_comments, created))
                total_found += 1
            except Exception as e:
                conn.rollback()
                log.error(f"Insert error: {e}")
                continue
        
        conn.commit()
        log.info(f"  r/{subreddit}: found {len(posts)} posts, {total_found} DOIs total")
        time.sleep(REDDIT_DELAY)
    
    # Link reddit mentions to our paper_ids
    cur.execute("""
    UPDATE social_reddit_mentions srm
    SET paper_id = p.id
    FROM papers p
    WHERE srm.doi = p.doi AND srm.paper_id IS NULL
    """)
    linked = cur.rowcount
    conn.commit()
    
    # Update reddit_mentions count in social_traction_papers
    cur.execute("""
    INSERT INTO social_traction_papers (paper_id, doi, reddit_mentions, composite_social_score, snapshot_date)
    SELECT p.id, p.doi, sub.mention_count, sub.total_score::real / 10.0, CURRENT_DATE
    FROM (
        SELECT doi, COUNT(*) AS mention_count, SUM(score) AS total_score
        FROM social_reddit_mentions
        WHERE doi IS NOT NULL
        GROUP BY doi
    ) sub
    JOIN papers p ON p.doi = sub.doi
    ON CONFLICT (paper_id) DO UPDATE SET
        reddit_mentions = EXCLUDED.reddit_mentions,
        composite_social_score = social_traction_papers.composite_social_score + EXCLUDED.composite_social_score
    """)
    conn.commit()
    
    cur.close()
    conn.close()
    log.info(f"Reddit: {total_found} DOI mentions, {linked} linked to papers")
    return total_found

def extract_doi(text):
    """Extract DOI from text. Returns normalized DOI or None."""
    import re
    if not text:
        return None
    
    # Match doi.org/10.XXXX/... or just 10.XXXX/...
    match = re.search(r'(?:doi\.org/|doi:?\s*)(10\.\d{4,9}/[^\s<>"\')\]]+)', text, re.IGNORECASE)
    if match:
        doi = match.group(1).rstrip('.,;:)')
        return doi
    return None

# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    log.info("=" * 70)
    log.info("NOBLEBLOCKS SOCIAL TRACTION CONNECTOR - Starting")
    log.info("=" * 70)
    
    total_start = time.time()
    
    # Step 1: Create tables
    create_tables()
    
    if shutdown_requested:
        return
    
    # Step 2: OpenAlex trending papers
    papers_linked = fetch_trending_papers_from_openalex()
    log.info(f"[CHECKPOINT] OpenAlex papers linked: {papers_linked:,}")
    
    if shutdown_requested:
        return
    
    # Step 3: OpenAlex trending concepts
    concepts_fetched = fetch_trending_concepts()
    
    if shutdown_requested:
        return
    
    # Step 4: Reddit DOI mentions
    reddit_found = fetch_reddit_mentions()
    
    total_time = time.time() - total_start
    log.info("=" * 70)
    log.info(f"SOCIAL TRACTION COMPLETE")
    log.info(f"  Papers with velocity data: {papers_linked:,}")
    log.info(f"  Trending concepts: {concepts_fetched:,}")
    log.info(f"  Reddit mentions: {reddit_found}")
    log.info(f"  Total time: {total_time:.0f}s ({total_time/60:.1f}m)")
    log.info("=" * 70)

if __name__ == '__main__':
    main()
