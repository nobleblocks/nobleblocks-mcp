#!/usr/bin/env python3
"""
DisGeNET Connector v2 — PubTator co-occurrence approach
========================================================
The original DisGeNET connector required a paid API key (returns HTML login).
This version builds gene-disease associations from PubTator co-occurrence:

Strategy:
  Papers in PubTator are tagged with both genes AND diseases.
  If a paper mentions both Gene X and Disease Y, that's evidence of an
  association between them. This is the same approach used by PubTator
  Central's gene-disease relation extraction.

  1. Query kg_paper_pubtator for papers that have BOTH gene and disease entities
  2. Create gene-disease association entries with co-occurrence counts
  3. Store paper links (the evidence papers)

Our PubTator data:
  - 2,812,265 gene entities
  - 11,216 disease entities
  - Co-occurrence of genes+diseases in same paper = gene-disease associations

This gives richer data than DisGeNET's free tier (limited to 10 queries).

Run:
  python3 kg_connector_disgenet_v2.py
  python3 kg_connector_disgenet_v2.py --min-cooccurrence 3  # require 3+ papers
"""

import argparse
import logging
import os
import sys
import time

import psycopg2
import psycopg2.extras

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("disgenet-v2")

# ── Config ────────────────────────────────────────────────────────────────────

DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = int(os.environ.get("DB_PORT", "5432"))
DB_NAME = os.environ.get("DB_NAME", "paper_search")
DB_USER = os.environ.get("DB_USER", "nobleblocks")
DB_PASS = os.environ.get("DB_PASS", "nb_papers_2026_prod")

BATCH_SIZE = 10000


def get_conn():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS,
    )


def ensure_tables(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS gene_disease_associations (
                id BIGSERIAL PRIMARY KEY,
                gene_id TEXT NOT NULL,
                gene_name TEXT,
                disease_id TEXT NOT NULL,
                disease_name TEXT,
                paper_count INT DEFAULT 0,
                score REAL DEFAULT 0,
                source TEXT DEFAULT 'pubtator_cooccurrence',
                created_at TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE(gene_id, disease_id)
            );
            CREATE TABLE IF NOT EXISTS gene_disease_papers (
                association_id BIGINT NOT NULL REFERENCES gene_disease_associations(id),
                paper_id BIGINT NOT NULL,
                PRIMARY KEY (association_id, paper_id)
            );
            CREATE INDEX IF NOT EXISTS idx_gda_gene ON gene_disease_associations(gene_id);
            CREATE INDEX IF NOT EXISTS idx_gda_disease ON gene_disease_associations(disease_id);
            CREATE INDEX IF NOT EXISTS idx_gda_score ON gene_disease_associations(score DESC);
            CREATE INDEX IF NOT EXISTS idx_gdp_paper ON gene_disease_papers(paper_id);
        """)
        conn.commit()


def build_associations(conn, min_cooccurrence=2, limit=0):
    """
    Build gene-disease associations from PubTator co-occurrence.

    Schema:
      kg_pubtator_entities: id, entity_type, concept_id, canonical_name
      kg_paper_pubtator: paper_id, entity_id (FK to kg_pubtator_entities.id)

    A co-occurrence is when the same paper_id has links to BOTH a Gene entity
    and a Disease entity.
    """
    log.info("Building gene-disease associations from PubTator co-occurrence...")
    log.info("  Min co-occurrence threshold: %d papers", min_cooccurrence)

    t0 = time.time()

    # Join kg_paper_pubtator with kg_pubtator_entities for both gene and disease
    cooccurrence_query = """
        INSERT INTO gene_disease_associations
            (gene_id, gene_name, disease_id, disease_name, paper_count, score)
        SELECT
            g_ent.concept_id AS gene_id,
            g_ent.canonical_name AS gene_name,
            d_ent.concept_id AS disease_id,
            d_ent.canonical_name AS disease_name,
            COUNT(DISTINCT g_link.paper_id) AS paper_count,
            -- Score: log-scaled co-occurrence count (0-1 range, cap at 100 papers)
            LEAST(1.0, LN(COUNT(DISTINCT g_link.paper_id) + 1) / LN(101))::REAL AS score
        FROM kg_paper_pubtator g_link
        INNER JOIN kg_pubtator_entities g_ent
            ON g_link.entity_id = g_ent.id
        INNER JOIN kg_paper_pubtator d_link
            ON g_link.paper_id = d_link.paper_id
        INNER JOIN kg_pubtator_entities d_ent
            ON d_link.entity_id = d_ent.id
        WHERE g_ent.entity_type = 'gene'
          AND d_ent.entity_type = 'disease'
          AND g_ent.concept_id IS NOT NULL AND g_ent.concept_id != ''
          AND d_ent.concept_id IS NOT NULL AND d_ent.concept_id != ''
          AND length(g_ent.canonical_name) >= 2
          AND length(d_ent.canonical_name) >= 2
        GROUP BY g_ent.concept_id, g_ent.canonical_name,
                 d_ent.concept_id, d_ent.canonical_name
        HAVING COUNT(DISTINCT g_link.paper_id) >= %s
        ON CONFLICT (gene_id, disease_id) DO UPDATE SET
            paper_count = EXCLUDED.paper_count,
            score = EXCLUDED.score,
            gene_name = COALESCE(NULLIF(EXCLUDED.gene_name, ''), gene_disease_associations.gene_name),
            disease_name = COALESCE(NULLIF(EXCLUDED.disease_name, ''), gene_disease_associations.disease_name)
    """

    with conn.cursor() as cur:
        log.info("  Running co-occurrence query (this may take 10-30 minutes)...")
        cur.execute(cooccurrence_query, (min_cooccurrence,))
        assoc_count = cur.rowcount
        conn.commit()

    elapsed = time.time() - t0
    log.info("  Created/updated %d gene-disease associations in %.0fs", assoc_count, elapsed)
    return assoc_count


def build_paper_links(conn, batch_size=50000):
    """
    For each gene-disease association, store the specific papers
    where the co-occurrence was observed.
    """
    log.info("Building paper links for associations...")
    t0 = time.time()

    # Get associations that don't have paper links yet
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, gene_id, disease_id
            FROM gene_disease_associations
            WHERE id NOT IN (SELECT DISTINCT association_id FROM gene_disease_papers)
            ORDER BY paper_count DESC
        """)
        associations = cur.fetchall()

    log.info("  %d associations need paper links", len(associations))
    total_links = 0

    for i in range(0, len(associations), 100):
        batch = associations[i:i + 100]

        with conn.cursor() as cur:
            for assoc_id, gene_id, disease_id in batch:
                cur.execute("""
                    INSERT INTO gene_disease_papers (association_id, paper_id)
                    SELECT %s, g_link.paper_id
                    FROM kg_paper_pubtator g_link
                    INNER JOIN kg_pubtator_entities g_ent
                        ON g_link.entity_id = g_ent.id
                    INNER JOIN kg_paper_pubtator d_link
                        ON g_link.paper_id = d_link.paper_id
                    INNER JOIN kg_pubtator_entities d_ent
                        ON d_link.entity_id = d_ent.id
                    WHERE g_ent.entity_type = 'gene'
                      AND g_ent.concept_id = %s
                      AND d_ent.entity_type = 'disease'
                      AND d_ent.concept_id = %s
                    ON CONFLICT DO NOTHING
                """, (assoc_id, gene_id, disease_id))
                total_links += cur.rowcount

        conn.commit()

        if (i + 100) % 1000 == 0:
            elapsed = time.time() - t0
            log.info("  Processed %d/%d associations, %d paper links (%.0fs)",
                     i + 100, len(associations), total_links, elapsed)

    elapsed = time.time() - t0
    log.info("  Created %d paper links in %.0fs", total_links, elapsed)
    return total_links


def run(min_cooccurrence=2, limit=0, skip_paper_links=False):
    log.info("DisGeNET Connector v2 (PubTator co-occurrence)")
    log.info("=" * 60)

    conn = get_conn()
    ensure_tables(conn)

    # Check current state
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM gene_disease_associations")
        existing = cur.fetchone()[0]
        cur.execute("""
            SELECT COUNT(*) FROM kg_pubtator_entities WHERE entity_type = 'gene'
        """)
        gene_count = cur.fetchone()[0]
        cur.execute("""
            SELECT COUNT(*) FROM kg_pubtator_entities WHERE entity_type = 'disease'
        """)
        disease_count = cur.fetchone()[0]

    log.info("Current state:")
    log.info("  Gene entity links: %s", f"{gene_count:,}")
    log.info("  Disease entity links: %s", f"{disease_count:,}")
    log.info("  Existing associations: %s", f"{existing:,}")

    # Phase 1: Build associations from co-occurrence
    assoc_count = build_associations(conn, min_cooccurrence=min_cooccurrence, limit=limit)

    # Phase 2: Build paper links (evidence)
    if not skip_paper_links and assoc_count > 0:
        paper_links = build_paper_links(conn)
    else:
        paper_links = 0

    # Final summary
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM gene_disease_associations")
        total_assoc = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM gene_disease_papers")
        total_papers = cur.fetchone()[0]
        cur.execute("""
            SELECT COUNT(DISTINCT gene_id) FROM gene_disease_associations
        """)
        unique_genes = cur.fetchone()[0]
        cur.execute("""
            SELECT COUNT(DISTINCT disease_id) FROM gene_disease_associations
        """)
        unique_diseases = cur.fetchone()[0]

    log.info("=" * 60)
    log.info("FINAL SUMMARY:")
    log.info("  Total associations: %s", f"{total_assoc:,}")
    log.info("  Unique genes: %s", f"{unique_genes:,}")
    log.info("  Unique diseases: %s", f"{unique_diseases:,}")
    log.info("  Paper evidence links: %s", f"{total_papers:,}")

    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--min-cooccurrence", type=int, default=2,
                        help="Minimum papers for a gene-disease pair (default: 2)")
    parser.add_argument("--limit", type=int, default=0,
                        help="Limit associations (0=unlimited)")
    parser.add_argument("--skip-paper-links", action="store_true",
                        help="Skip building individual paper links")
    args = parser.parse_args()
    run(min_cooccurrence=args.min_cooccurrence, limit=args.limit,
        skip_paper_links=args.skip_paper_links)
