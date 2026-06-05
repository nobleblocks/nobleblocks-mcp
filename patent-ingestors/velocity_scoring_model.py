#!/usr/bin/env python3
"""
NobleBlocks Citation Velocity & Trending Scoring Model
======================================================
Computes trending scores for genes, diseases, chemicals, and papers
based on citation acceleration and publication velocity.

Scoring Dimensions:
1. Gene Velocity   - papers mentioning gene / year → acceleration
2. Disease Velocity - papers mentioning disease / year → acceleration  
3. Paper Velocity  - citation growth rate vs expected for age
4. Patent Traction - patent citations as market signal
5. Composite Score - weighted combination for VC dashboard

Tables Created:
- velocity_gene_scores: trending genes with velocity metrics
- velocity_disease_scores: trending diseases
- velocity_paper_scores: papers with unusual citation acceleration
- velocity_snapshots: daily snapshot metadata for time series

Runs as: nohup python3 velocity_scoring_model.py &
"""

import psycopg2
import psycopg2.extras
import logging
import time
import json
import os
import signal
import sys
from datetime import datetime, date
from dataclasses import dataclass

# ─── Configuration ───────────────────────────────────────────────────────────

DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'dbname': 'paper_search',
    'user': 'nobleblocks',
    'password': 'nb_papers_2026_prod',
}

BATCH_SIZE = 5000
CURRENT_YEAR = datetime.now().year  # 2026
RECENT_WINDOW = 3       # years considered "recent" (2024-2026)
HISTORICAL_START = 2010  # ignore papers before this for velocity

# Velocity formula: v = recent_rate / max(historical_rate, 1)
# Acceleration: a = (rate_last_year - rate_2yr_ago) / max(rate_2yr_ago, 1)
# Papers with v > 2.0 or a > 1.5 are "trending"

LOG_FILE = '/tmp/velocity_scoring.log'
PROGRESS_FILE = '/tmp/velocity_progress.json'

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

# ─── Database helpers ────────────────────────────────────────────────────────

def get_conn():
    return psycopg2.connect(**DB_CONFIG)

def create_tables():
    """Create velocity scoring tables if not exist."""
    conn = get_conn()
    cur = conn.cursor()
    
    cur.execute("""
    CREATE TABLE IF NOT EXISTS velocity_gene_scores (
        entity_id       BIGINT PRIMARY KEY,
        concept_id      TEXT,
        canonical_name  TEXT,
        total_papers    INTEGER,
        papers_recent   INTEGER,  -- last RECENT_WINDOW years
        papers_prior    INTEGER,  -- HISTORICAL_START to recent
        rate_recent     REAL,     -- papers per year in recent window
        rate_prior      REAL,     -- papers per year in prior window
        velocity        REAL,     -- rate_recent / rate_prior
        acceleration    REAL,     -- (rate_last_yr - rate_2yr_ago) / rate_2yr_ago
        patent_citations INTEGER DEFAULT 0,  -- papers citing this gene also cited in patents
        gwas_associations INTEGER DEFAULT 0, -- GWAS hits for this gene
        composite_score REAL,     -- weighted final score
        percentile      REAL,     -- percentile rank (0-100)
        snapshot_date   DATE DEFAULT CURRENT_DATE,
        updated_at      TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS velocity_disease_scores (
        entity_id       BIGINT PRIMARY KEY,
        concept_id      TEXT,
        canonical_name  TEXT,
        total_papers    INTEGER,
        papers_recent   INTEGER,
        papers_prior    INTEGER,
        rate_recent     REAL,
        rate_prior      REAL,
        velocity        REAL,
        acceleration    REAL,
        composite_score REAL,
        percentile      REAL,
        snapshot_date   DATE DEFAULT CURRENT_DATE,
        updated_at      TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS velocity_paper_scores (
        paper_id        BIGINT PRIMARY KEY,
        citation_count  INTEGER,
        paper_year      SMALLINT,
        age_years       SMALLINT,
        expected_rate   REAL,     -- avg citations/year for papers of this age
        actual_rate     REAL,     -- this paper's citations/year
        velocity        REAL,     -- actual_rate / expected_rate
        recent_citations INTEGER, -- citations in last 2 years
        patent_citations INTEGER DEFAULT 0,
        composite_score REAL,
        percentile      REAL,
        snapshot_date   DATE DEFAULT CURRENT_DATE,
        updated_at      TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS velocity_snapshots (
        id              SERIAL PRIMARY KEY,
        snapshot_date   DATE UNIQUE,
        genes_scored    INTEGER,
        diseases_scored INTEGER,
        papers_scored   INTEGER,
        top_gene        TEXT,
        top_gene_score  REAL,
        compute_seconds REAL,
        created_at      TIMESTAMPTZ DEFAULT NOW()
    );

    -- Indexes for fast lookups
    CREATE INDEX IF NOT EXISTS idx_vel_gene_composite ON velocity_gene_scores(composite_score DESC);
    CREATE INDEX IF NOT EXISTS idx_vel_gene_velocity ON velocity_gene_scores(velocity DESC);
    CREATE INDEX IF NOT EXISTS idx_vel_disease_composite ON velocity_disease_scores(composite_score DESC);
    CREATE INDEX IF NOT EXISTS idx_vel_paper_composite ON velocity_paper_scores(composite_score DESC);
    CREATE INDEX IF NOT EXISTS idx_vel_paper_velocity ON velocity_paper_scores(velocity DESC);
    """)
    
    conn.commit()
    cur.close()
    conn.close()
    log.info("Velocity scoring tables created/verified")

# ─── Gene Velocity Scoring ───────────────────────────────────────────────────

def compute_gene_velocity():
    """
    For each gene in kg_pubtator_entities, compute publication velocity.
    Uses pre-aggregation: kg_paper_pubtator JOIN papers → GROUP BY entity_id
    then compute velocity metrics from the aggregated counts.
    """
    log.info("=== Computing Gene Velocity Scores ===")
    start = time.time()
    
    conn = get_conn()
    cur = conn.cursor()
    
    cur.execute("DELETE FROM velocity_gene_scores")
    conn.commit()
    log.info("Cleared previous gene scores")
    
    params = {
        'recent_window': RECENT_WINDOW,
        'prior_years': CURRENT_YEAR - RECENT_WINDOW - HISTORICAL_START,
        'recent_start': CURRENT_YEAR - RECENT_WINDOW + 1,  # 2024
        'hist_start': HISTORICAL_START,  # 2010
        'last_year': CURRENT_YEAR - 1,  # 2025
        'two_yr_ago': CURRENT_YEAR - 2,  # 2024
    }
    
    log.info(f"Running gene velocity computation (pre-aggregation)...")
    log.info(f"  Recent window: {params['recent_start']}-{CURRENT_YEAR}")
    log.info(f"  Historical window: {HISTORICAL_START}-{params['recent_start']-1}")
    
    # Step 1: Pre-aggregate paper counts per entity in one scan of 143M rows
    # Step 2: JOIN with entity metadata and compute velocity
    # This scans kg_paper_pubtator once (not per-entity) → orders of magnitude faster
    query = """
    INSERT INTO velocity_gene_scores 
        (entity_id, concept_id, canonical_name, total_papers, 
         papers_recent, papers_prior, rate_recent, rate_prior,
         velocity, acceleration, composite_score, snapshot_date)
    WITH entity_paper_counts AS (
        SELECT 
            kpp.entity_id,
            COUNT(*) AS total_papers,
            COUNT(*) FILTER (WHERE p.year >= %(recent_start)s) AS papers_recent,
            COUNT(*) FILTER (WHERE p.year >= %(hist_start)s AND p.year < %(recent_start)s) AS papers_prior,
            COUNT(*) FILTER (WHERE p.year = %(last_year)s) AS papers_last_yr,
            COUNT(*) FILTER (WHERE p.year = %(two_yr_ago)s) AS papers_2yr_ago
        FROM kg_paper_pubtator kpp
        JOIN papers p ON p.id = kpp.paper_id
        JOIN kg_pubtator_entities e ON e.id = kpp.entity_id AND e.entity_type = 'gene'
        WHERE p.year >= %(hist_start)s
        GROUP BY kpp.entity_id
        HAVING COUNT(*) >= 5
    )
    SELECT 
        e.id,
        e.concept_id,
        e.canonical_name,
        epc.total_papers,
        epc.papers_recent,
        epc.papers_prior,
        epc.papers_recent::real / %(recent_window)s AS rate_recent,
        epc.papers_prior::real / GREATEST(%(prior_years)s, 1) AS rate_prior,
        -- Velocity
        CASE WHEN epc.papers_prior = 0 THEN
            CASE WHEN epc.papers_recent > 5 THEN 10.0 ELSE 0.0 END
        ELSE
            (epc.papers_recent::real / %(recent_window)s) / 
            (epc.papers_prior::real / %(prior_years)s)
        END AS velocity,
        -- Acceleration
        CASE WHEN epc.papers_2yr_ago = 0 THEN
            CASE WHEN epc.papers_last_yr > 3 THEN 5.0 ELSE 0.0 END
        ELSE
            (epc.papers_last_yr::real - epc.papers_2yr_ago::real) / 
            GREATEST(epc.papers_2yr_ago::real, 1.0)
        END AS acceleration,
        -- Composite: velocity * log(total+1) * (1 + acc/5)
        CASE WHEN epc.papers_prior = 0 AND epc.papers_recent <= 5 THEN 0.0
        ELSE GREATEST(
            (CASE WHEN epc.papers_prior = 0 THEN 10.0
             ELSE (epc.papers_recent::real / %(recent_window)s) / (epc.papers_prior::real / %(prior_years)s)
             END)
            * LN(epc.total_papers + 1)
            * (1.0 + LEAST(
                CASE WHEN epc.papers_2yr_ago = 0 THEN
                    CASE WHEN epc.papers_last_yr > 3 THEN 5.0 ELSE 0.0 END
                ELSE (epc.papers_last_yr::real - epc.papers_2yr_ago::real) / GREATEST(epc.papers_2yr_ago::real, 1.0)
                END
            , 10.0) / 5.0),
        0.0)
        END AS composite_score,
        CURRENT_DATE
    FROM entity_paper_counts epc
    JOIN kg_pubtator_entities e ON e.id = epc.entity_id
    """
    
    cur.execute(query, params)
    genes_scored = cur.rowcount
    conn.commit()
    
    log.info(f"Scored {genes_scored:,} genes")
    
    # Compute percentiles
    cur.execute("""
    UPDATE velocity_gene_scores vgs
    SET percentile = sub.pct
    FROM (
        SELECT entity_id, 
               PERCENT_RANK() OVER (ORDER BY composite_score) * 100 AS pct
        FROM velocity_gene_scores
    ) sub
    WHERE vgs.entity_id = sub.entity_id
    """)
    conn.commit()
    
    elapsed = time.time() - start
    log.info(f"Gene velocity complete in {elapsed:.1f}s")
    
    # Log top 20
    cur.execute("""
    SELECT canonical_name, concept_id, velocity, acceleration, composite_score, 
           total_papers, papers_recent, percentile
    FROM velocity_gene_scores 
    ORDER BY composite_score DESC 
    LIMIT 20
    """)
    rows = cur.fetchall()
    log.info("=== TOP 20 TRENDING GENES ===")
    for r in rows:
        log.info(f"  {r[0]} ({r[1]}): vel={r[2]:.2f} acc={r[3]:.2f} "
                 f"composite={r[4]:.2f} papers={r[5]} recent={r[6]} pct={r[7]:.1f}")
    
    cur.close()
    conn.close()
    return genes_scored, elapsed

# ─── Disease Velocity Scoring ────────────────────────────────────────────────

def compute_disease_velocity():
    """
    For each disease in kg_pubtator_entities, compute publication velocity.
    Uses pre-aggregation approach (same as genes).
    """
    log.info("=== Computing Disease Velocity Scores ===")
    start = time.time()
    
    conn = get_conn()
    cur = conn.cursor()
    
    cur.execute("DELETE FROM velocity_disease_scores")
    conn.commit()
    
    params = {
        'recent_window': RECENT_WINDOW,
        'prior_years': CURRENT_YEAR - RECENT_WINDOW - HISTORICAL_START,
        'recent_start': CURRENT_YEAR - RECENT_WINDOW + 1,
        'hist_start': HISTORICAL_START,
        'last_year': CURRENT_YEAR - 1,
        'two_yr_ago': CURRENT_YEAR - 2,
    }
    
    query = """
    INSERT INTO velocity_disease_scores 
        (entity_id, concept_id, canonical_name, total_papers, 
         papers_recent, papers_prior, rate_recent, rate_prior,
         velocity, acceleration, composite_score, snapshot_date)
    WITH entity_paper_counts AS (
        SELECT 
            kpp.entity_id,
            COUNT(*) AS total_papers,
            COUNT(*) FILTER (WHERE p.year >= %(recent_start)s) AS papers_recent,
            COUNT(*) FILTER (WHERE p.year >= %(hist_start)s AND p.year < %(recent_start)s) AS papers_prior,
            COUNT(*) FILTER (WHERE p.year = %(last_year)s) AS papers_last_yr,
            COUNT(*) FILTER (WHERE p.year = %(two_yr_ago)s) AS papers_2yr_ago
        FROM kg_paper_pubtator kpp
        JOIN papers p ON p.id = kpp.paper_id
        JOIN kg_pubtator_entities e ON e.id = kpp.entity_id AND e.entity_type = 'disease'
        WHERE p.year >= %(hist_start)s
        GROUP BY kpp.entity_id
        HAVING COUNT(*) >= 5
    )
    SELECT 
        e.id,
        e.concept_id,
        e.canonical_name,
        epc.total_papers,
        epc.papers_recent,
        epc.papers_prior,
        epc.papers_recent::real / %(recent_window)s,
        epc.papers_prior::real / GREATEST(%(prior_years)s, 1),
        CASE WHEN epc.papers_prior = 0 THEN
            CASE WHEN epc.papers_recent > 5 THEN 10.0 ELSE 0.0 END
        ELSE
            (epc.papers_recent::real / %(recent_window)s) / 
            (epc.papers_prior::real / %(prior_years)s)
        END,
        CASE WHEN epc.papers_2yr_ago = 0 THEN
            CASE WHEN epc.papers_last_yr > 3 THEN 5.0 ELSE 0.0 END
        ELSE
            (epc.papers_last_yr::real - epc.papers_2yr_ago::real) / 
            GREATEST(epc.papers_2yr_ago::real, 1.0)
        END,
        CASE WHEN epc.papers_prior = 0 AND epc.papers_recent <= 5 THEN 0.0
        ELSE GREATEST(
            (CASE WHEN epc.papers_prior = 0 THEN 10.0
             ELSE (epc.papers_recent::real / %(recent_window)s) / (epc.papers_prior::real / %(prior_years)s)
             END)
            * LN(epc.total_papers + 1)
            * (1.0 + LEAST(
                CASE WHEN epc.papers_2yr_ago = 0 THEN
                    CASE WHEN epc.papers_last_yr > 3 THEN 5.0 ELSE 0.0 END
                ELSE (epc.papers_last_yr::real - epc.papers_2yr_ago::real) / GREATEST(epc.papers_2yr_ago::real, 1.0)
                END
            , 10.0) / 5.0),
        0.0)
        END,
        CURRENT_DATE
    FROM entity_paper_counts epc
    JOIN kg_pubtator_entities e ON e.id = epc.entity_id
    """
    
    cur.execute(query, params)
    diseases_scored = cur.rowcount
    conn.commit()
    
    # Percentiles
    cur.execute("""
    UPDATE velocity_disease_scores vds
    SET percentile = sub.pct
    FROM (
        SELECT entity_id,
               PERCENT_RANK() OVER (ORDER BY composite_score) * 100 AS pct
        FROM velocity_disease_scores
    ) sub
    WHERE vds.entity_id = sub.entity_id
    """)
    conn.commit()
    
    elapsed = time.time() - start
    log.info(f"Scored {diseases_scored:,} diseases in {elapsed:.1f}s")
    
    cur.execute("""
    SELECT canonical_name, concept_id, velocity, acceleration, composite_score, total_papers
    FROM velocity_disease_scores ORDER BY composite_score DESC LIMIT 10
    """)
    for r in cur.fetchall():
        log.info(f"  {r[0]} ({r[1]}): vel={r[2]:.2f} acc={r[3]:.2f} composite={r[4]:.2f} papers={r[5]}")
    
    cur.close()
    conn.close()
    return diseases_scored, elapsed

# ─── Paper Velocity Scoring ──────────────────────────────────────────────────

def compute_paper_velocity():
    """
    For papers with citation_count >= 10 (from 2015+), compute citation velocity.
    Velocity = this paper's citations/year vs average for papers of same year.
    
    Optimized: pre-compute avg rates by year, then single-pass INSERT.
    """
    log.info("=== Computing Paper Citation Velocity ===")
    start = time.time()
    
    conn = get_conn()
    cur = conn.cursor()
    
    cur.execute("DELETE FROM velocity_paper_scores")
    conn.commit()
    
    # Step 1: Compute average citation rate by publication year  
    # (papers with citation_count > 0 only, to get meaningful baselines)
    log.info("Computing average citation rates by year...")
    cur.execute("""
    CREATE TEMP TABLE IF NOT EXISTS _year_avg_rates AS
    SELECT year, 
           AVG(citation_count::real / GREATEST(%(current_year)s - year, 1)) AS avg_rate,
           PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY citation_count::real / GREATEST(%(current_year)s - year, 1)) AS p75_rate
    FROM papers
    WHERE year >= 2015 AND year <= %(current_year)s AND citation_count > 0
    GROUP BY year
    """, {'current_year': CURRENT_YEAR})
    conn.commit()
    
    # Step 2: Score papers against their year's baseline
    log.info("Scoring papers against year baselines...")
    cur.execute("""
    INSERT INTO velocity_paper_scores
        (paper_id, citation_count, paper_year, age_years, 
         expected_rate, actual_rate, velocity, composite_score, snapshot_date)
    SELECT 
        p.id,
        p.citation_count,
        p.year,
        (%(current_year)s - p.year)::smallint,
        yr.avg_rate,
        p.citation_count::real / GREATEST(%(current_year)s - p.year, 1) AS actual_rate,
        -- Velocity: how many multiples of the average
        CASE WHEN yr.avg_rate < 0.1 THEN
            p.citation_count::real / GREATEST(%(current_year)s - p.year, 1)
        ELSE
            (p.citation_count::real / GREATEST(%(current_year)s - p.year, 1)) / yr.avg_rate
        END AS velocity,
        -- Composite: velocity * log(citations + 1)
        CASE WHEN yr.avg_rate < 0.1 THEN
            (p.citation_count::real / GREATEST(%(current_year)s - p.year, 1)) * LN(p.citation_count + 1)
        ELSE
            ((p.citation_count::real / GREATEST(%(current_year)s - p.year, 1)) / yr.avg_rate) * LN(p.citation_count + 1)
        END AS composite_score,
        CURRENT_DATE
    FROM papers p
    JOIN _year_avg_rates yr ON yr.year = p.year
    WHERE p.year >= 2015 AND p.year <= %(current_year)s
      AND p.citation_count >= 10
    """, {'current_year': CURRENT_YEAR})
    
    papers_scored = cur.rowcount
    conn.commit()
    log.info(f"Scored {papers_scored:,} papers")
    
    # Drop temp table
    cur.execute("DROP TABLE IF EXISTS _year_avg_rates")
    conn.commit()
    
    # Percentiles
    cur.execute("""
    UPDATE velocity_paper_scores vps
    SET percentile = sub.pct
    FROM (
        SELECT paper_id,
               PERCENT_RANK() OVER (ORDER BY composite_score) * 100 AS pct
        FROM velocity_paper_scores
    ) sub
    WHERE vps.paper_id = sub.paper_id
    """)
    conn.commit()
    
    elapsed = time.time() - start
    log.info(f"Paper velocity complete in {elapsed:.1f}s")
    
    # Top papers
    cur.execute("""
    SELECT vps.paper_id, p.title, vps.citation_count, vps.paper_year, 
           vps.velocity, vps.composite_score, vps.percentile
    FROM velocity_paper_scores vps
    JOIN papers p ON p.id = vps.paper_id
    ORDER BY vps.composite_score DESC
    LIMIT 10
    """)
    log.info("=== TOP 10 VELOCITY PAPERS ===")
    for r in cur.fetchall():
        title = (r[1] or '')[:60]
        log.info(f"  [{r[3]}] {title}... cit={r[2]} vel={r[4]:.2f} score={r[5]:.2f}")
    
    cur.close()
    conn.close()
    return papers_scored, elapsed

# ─── Enrich with Patent Citations ────────────────────────────────────────────

def enrich_patent_citations():
    """
    Cross-reference velocity_gene_scores with patent_citation_signals
    to boost genes whose papers are being cited by patents.
    """
    log.info("=== Enriching with Patent Citations ===")
    conn = get_conn()
    cur = conn.cursor()
    
    # For genes: count how many of their associated papers are cited by patents
    cur.execute("""
    UPDATE velocity_gene_scores vgs
    SET patent_citations = sub.patent_count,
        composite_score = composite_score * (1.0 + LEAST(sub.patent_count::real / 10.0, 2.0))
    FROM (
        SELECT kpp.entity_id, COUNT(DISTINCT pcs.paper_id) AS patent_count
        FROM kg_paper_pubtator kpp
        JOIN patent_citation_signals pcs ON pcs.paper_id = kpp.paper_id
        GROUP BY kpp.entity_id
    ) sub
    WHERE vgs.entity_id = sub.entity_id
      AND sub.patent_count > 0
    """)
    patent_enriched = cur.rowcount
    conn.commit()
    log.info(f"Enriched {patent_enriched:,} genes with patent citation data")
    
    # For papers: direct patent citation lookup
    cur.execute("""
    UPDATE velocity_paper_scores vps
    SET patent_citations = sub.patent_count,
        composite_score = composite_score * (1.0 + LEAST(sub.patent_count::real / 5.0, 3.0))
    FROM (
        SELECT paper_id, COUNT(*) AS patent_count
        FROM patent_citation_signals
        GROUP BY paper_id
    ) sub
    WHERE vps.paper_id = sub.paper_id
      AND sub.patent_count > 0
    """)
    papers_enriched = cur.rowcount
    conn.commit()
    log.info(f"Enriched {papers_enriched:,} papers with patent citations")
    
    cur.close()
    conn.close()
    return patent_enriched, papers_enriched

# ─── Enrich with GWAS ────────────────────────────────────────────────────────

def enrich_gwas():
    """
    Boost gene scores based on GWAS association count.
    Genes with GWAS hits are more likely to be drug targets.
    """
    log.info("=== Enriching with GWAS Associations ===")
    conn = get_conn()
    cur = conn.cursor()
    
    # kg_gwas_associations has mapped_gene (text) 
    # kg_pubtator_entities has canonical_name (text)
    # Match on gene name (case-insensitive)
    cur.execute("""
    UPDATE velocity_gene_scores vgs
    SET gwas_associations = sub.gwas_count,
        composite_score = composite_score * (1.0 + LEAST(sub.gwas_count::real / 20.0, 1.5))
    FROM (
        SELECT e.id AS entity_id, COUNT(*) AS gwas_count
        FROM kg_pubtator_entities e
        JOIN kg_gwas_associations g ON LOWER(g.mapped_gene) = LOWER(e.canonical_name)
        WHERE e.entity_type = 'gene'
        GROUP BY e.id
    ) sub
    WHERE vgs.entity_id = sub.entity_id
      AND sub.gwas_count > 0
    """)
    gwas_enriched = cur.rowcount
    conn.commit()
    log.info(f"Enriched {gwas_enriched:,} genes with GWAS data")
    
    cur.close()
    conn.close()
    return gwas_enriched

# ─── Save Snapshot ───────────────────────────────────────────────────────────

def save_snapshot(genes_scored, diseases_scored, papers_scored, total_time):
    """Record this scoring run."""
    conn = get_conn()
    cur = conn.cursor()
    
    cur.execute("SELECT canonical_name, composite_score FROM velocity_gene_scores ORDER BY composite_score DESC LIMIT 1")
    top = cur.fetchone()
    top_gene = top[0] if top else None
    top_score = top[1] if top else 0
    
    cur.execute("""
    INSERT INTO velocity_snapshots (snapshot_date, genes_scored, diseases_scored, papers_scored, 
                                    top_gene, top_gene_score, compute_seconds)
    VALUES (CURRENT_DATE, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (snapshot_date) DO UPDATE SET
        genes_scored = EXCLUDED.genes_scored,
        diseases_scored = EXCLUDED.diseases_scored,
        papers_scored = EXCLUDED.papers_scored,
        top_gene = EXCLUDED.top_gene,
        top_gene_score = EXCLUDED.top_gene_score,
        compute_seconds = EXCLUDED.compute_seconds
    """, (genes_scored, diseases_scored, papers_scored, top_gene, top_score, total_time))
    
    conn.commit()
    cur.close()
    conn.close()
    log.info(f"Snapshot saved: top gene = {top_gene} (score {top_score:.2f})")

# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    log.info("=" * 70)
    log.info("NOBLEBLOCKS VELOCITY SCORING MODEL - Starting")
    log.info(f"Current year: {CURRENT_YEAR}, Recent window: {CURRENT_YEAR-RECENT_WINDOW+1}-{CURRENT_YEAR}")
    log.info(f"Historical window: {HISTORICAL_START}-{CURRENT_YEAR-RECENT_WINDOW}")
    log.info("=" * 70)
    
    total_start = time.time()
    
    # Step 1: Create tables
    create_tables()
    
    if shutdown_requested:
        return
    
    # Step 2: Gene velocity (biggest computation)
    genes_scored, gene_time = compute_gene_velocity()
    log.info(f"[CHECKPOINT] Gene scoring done: {genes_scored:,} genes in {gene_time:.0f}s")
    
    if shutdown_requested:
        return
    
    # Step 3: Disease velocity
    diseases_scored, disease_time = compute_disease_velocity()
    
    if shutdown_requested:
        return
    
    # Step 4: Paper velocity
    papers_scored, paper_time = compute_paper_velocity()
    
    if shutdown_requested:
        return
    
    # Step 5: Enrich with patent citations
    enrich_patent_citations()
    
    if shutdown_requested:
        return
    
    # Step 6: Enrich with GWAS
    enrich_gwas()
    
    # Step 7: Recompute percentiles after enrichment
    log.info("Recomputing final percentiles after enrichment...")
    conn = get_conn()
    cur = conn.cursor()
    
    cur.execute("""
    UPDATE velocity_gene_scores vgs
    SET percentile = sub.pct
    FROM (
        SELECT entity_id,
               PERCENT_RANK() OVER (ORDER BY composite_score) * 100 AS pct
        FROM velocity_gene_scores
    ) sub
    WHERE vgs.entity_id = sub.entity_id
    """)
    
    cur.execute("""
    UPDATE velocity_paper_scores vps
    SET percentile = sub.pct
    FROM (
        SELECT paper_id,
               PERCENT_RANK() OVER (ORDER BY composite_score) * 100 AS pct
        FROM velocity_paper_scores
    ) sub
    WHERE vps.paper_id = sub.paper_id
    """)
    conn.commit()
    cur.close()
    conn.close()
    
    # Step 8: Save snapshot
    total_time = time.time() - total_start
    save_snapshot(genes_scored, diseases_scored, papers_scored, total_time)
    
    log.info("=" * 70)
    log.info(f"VELOCITY SCORING COMPLETE")
    log.info(f"  Genes:    {genes_scored:,}")
    log.info(f"  Diseases: {diseases_scored:,}")
    log.info(f"  Papers:   {papers_scored:,}")
    log.info(f"  Total time: {total_time:.0f}s ({total_time/60:.1f}m)")
    log.info("=" * 70)

if __name__ == '__main__':
    main()
