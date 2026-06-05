#!/usr/bin/env python3
"""
DrugBank Connector v2 — MeSH Chemical-to-Drug mapping
======================================================
The original DrugBank connector required a paid vocabulary download (403 error).
This version builds drug→paper links using PubTator chemical entities which use
MeSH IDs:

  - MESH:D* = Drugs and Chemicals category (established drugs/compounds)
  - MESH:C* = Supplemental Concept Records (specific chemicals, newer drugs)

Strategy:
  1. Load all chemical entities from kg_pubtator_entities (125K entities)
  2. Classify: MESH:D* IDs = known drugs, MESH:C* = supplemental compounds
  3. Cross-reference MeSH via NCBI E-utilities to get drug metadata
  4. Create paper links using existing kg_paper_pubtator join table

Our PubTator has 125,404 chemical entities already linked to papers.
Many of these (tacrolimus, methotrexate, omalizumab, etc.) ARE the drugs
that DrugBank catalogs.

Run:
  python3 kg_connector_drugbank_v2.py
  python3 kg_connector_drugbank_v2.py --batch-size 200
"""

import argparse
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
log = logging.getLogger("drugbank-v2")

# ── Config ────────────────────────────────────────────────────────────────────

DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = int(os.environ.get("DB_PORT", "5432"))
DB_NAME = os.environ.get("DB_NAME", "paper_search")
DB_USER = os.environ.get("DB_USER", "nobleblocks")
DB_PASS = os.environ.get("DB_PASS", "nb_papers_2026_prod")

PROGRESS_FILE = "/tmp/drugbank_v2_progress.json"
# NCBI E-utilities for MeSH lookup (optional enrichment)
MESH_API = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi"
RATE_LIMIT = 0.35  # seconds between API calls


def get_conn():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS,
    )


def ensure_tables(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS drug_compounds (
                id BIGSERIAL PRIMARY KEY,
                mesh_id TEXT UNIQUE NOT NULL,
                name TEXT,
                is_drug BOOLEAN DEFAULT FALSE,
                mesh_category TEXT,
                entity_id BIGINT,
                paper_count INT DEFAULT 0,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE TABLE IF NOT EXISTS drug_paper_links (
                paper_id BIGINT NOT NULL,
                drug_id BIGINT NOT NULL REFERENCES drug_compounds(id),
                PRIMARY KEY (paper_id, drug_id)
            );
            CREATE INDEX IF NOT EXISTS idx_drug_compounds_mesh ON drug_compounds(mesh_id);
            CREATE INDEX IF NOT EXISTS idx_drug_compounds_drug ON drug_compounds(is_drug) WHERE is_drug;
            CREATE INDEX IF NOT EXISTS idx_drug_paper_drug ON drug_paper_links(drug_id);
        """)
        conn.commit()


def load_progress():
    try:
        with open(PROGRESS_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"phase": "init", "offset": 0, "drugs_stored": 0, "paper_links": 0}


def save_progress(prog):
    with open(PROGRESS_FILE, "w") as f:
        json.dump(prog, f)


def run(batch_size=500):
    log.info("DrugBank Connector v2 (MeSH Chemical approach)")
    log.info("=" * 60)

    conn = get_conn()
    ensure_tables(conn)

    progress = load_progress()
    t0 = time.time()

    # Phase 1: Load chemical entities and classify them
    log.info("Phase 1: Loading chemical entities from PubTator...")

    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, concept_id, canonical_name
            FROM kg_pubtator_entities
            WHERE entity_type = 'chemical'
              AND concept_id IS NOT NULL
              AND concept_id != ''
              AND concept_id != '-'
              AND length(canonical_name) >= 2
            ORDER BY id
        """)
        entities = cur.fetchall()

    log.info("  Found %d chemical entities with valid MeSH IDs", len(entities))

    # Phase 2: Insert into drug_compounds (classify by MeSH prefix)
    log.info("Phase 2: Classifying and storing compounds...")

    drugs_stored = 0
    compounds_data = []

    for ent_id, mesh_id, name in entities:
        # MESH:D = established drugs/chemicals in MeSH tree
        # MESH:C = supplemental concepts (often newer drugs, specific compounds)
        is_drug = mesh_id.startswith("MESH:D")
        category = "drug" if mesh_id.startswith("MESH:D") else "supplement"

        # Use first canonical name (before |)
        display_name = name.split("|")[0].strip() if name else ""

        compounds_data.append((
            mesh_id, display_name, is_drug, category, ent_id
        ))

    if compounds_data:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                """INSERT INTO drug_compounds (mesh_id, name, is_drug, mesh_category, entity_id)
                   VALUES %s
                   ON CONFLICT (mesh_id) DO UPDATE SET
                     name = COALESCE(NULLIF(EXCLUDED.name, ''), drug_compounds.name),
                     is_drug = EXCLUDED.is_drug
                """,
                compounds_data,
                template="(%s, %s, %s, %s, %s)",
            )
            drugs_stored = cur.rowcount
        conn.commit()

    log.info("  Stored %d compounds (%d are MESH:D drugs)",
             drugs_stored, sum(1 for _, _, d, _, _ in compounds_data if d))

    # Phase 3: Build paper links from kg_paper_pubtator
    log.info("Phase 3: Building paper links...")

    with conn.cursor() as cur:
        # Efficient bulk insert using the entity_id FK
        cur.execute("""
            INSERT INTO drug_paper_links (paper_id, drug_id)
            SELECT kpp.paper_id, dc.id
            FROM kg_paper_pubtator kpp
            INNER JOIN drug_compounds dc ON dc.entity_id = kpp.entity_id
            ON CONFLICT DO NOTHING
        """)
        paper_links = cur.rowcount
        conn.commit()

    log.info("  Created %d paper-drug links", paper_links)

    # Phase 4: Update paper counts
    log.info("Phase 4: Updating paper counts...")
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE drug_compounds dc SET paper_count = sub.cnt
            FROM (
                SELECT drug_id, COUNT(*) as cnt
                FROM drug_paper_links
                GROUP BY drug_id
            ) sub
            WHERE dc.id = sub.drug_id
        """)
        conn.commit()

    elapsed = time.time() - t0

    # Final summary
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM drug_compounds")
        total_compounds = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM drug_compounds WHERE is_drug")
        total_drugs = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM drug_paper_links")
        total_links = cur.fetchone()[0]
        cur.execute("""
            SELECT name, paper_count FROM drug_compounds
            ORDER BY paper_count DESC LIMIT 10
        """)
        top_drugs = cur.fetchall()

    log.info("=" * 60)
    log.info("DONE in %.0fs", elapsed)
    log.info("  Total compounds: %s", f"{total_compounds:,}")
    log.info("  Known drugs (MESH:D): %s", f"{total_drugs:,}")
    log.info("  Paper-drug links: %s", f"{total_links:,}")
    log.info("  Top drugs by paper count:")
    for name, count in top_drugs:
        log.info("    %s: %s papers", name[:40], f"{count:,}")

    progress.update({
        "phase": "done",
        "drugs_stored": total_compounds,
        "paper_links": total_links,
    })
    save_progress(progress)
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-size", type=int, default=500)
    args = parser.parse_args()
    run(batch_size=args.batch_size)
