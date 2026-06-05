#!/usr/bin/env python3
"""
Load IPC/CPC classification codes from BigQuery export into patents table.

Input: JSONL.GZ file with fields: publication_number, ipc_codes, cpc_codes
Updates: patents.ipc_codes (text[]) and patents.cpc_codes (text[]) columns

Usage:
    python3 load_ipc_codes.py /path/to/patents_ipc_codes_*.jsonl.gz [--batch-size 5000]
"""

import argparse
import gzip
import json
import logging
import sys
import time

import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "paper_search",
    "user": "nobleblocks",
    "password": "nb_papers_2026_prod",
}


def ensure_columns(conn):
    """Verify ipc_codes and cpc_codes columns exist (no ALTER TABLE to avoid ACCESS EXCLUSIVE lock)."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name FROM information_schema.columns 
            WHERE table_name = 'patents' AND column_name IN ('ipc_codes', 'cpc_codes')
        """)
        cols = [r[0] for r in cur.fetchall()]
        if 'ipc_codes' not in cols or 'cpc_codes' not in cols:
            raise RuntimeError(f"Missing columns! Found: {cols}. Run ALTER TABLE manually first.")
    log.info("Verified ipc_codes and cpc_codes columns exist")


def normalize_patent_id(pub_number: str) -> str:
    """Convert BigQuery publication_number to our patent_id format.
    BQ: US-12224364-B2 (with kind code)
    Ours: US-12224364 (jurisdiction-number, no kind code)
    """
    pub_number = pub_number.strip().upper()
    parts = pub_number.split("-")
    if len(parts) >= 3:
        return f"{parts[0]}-{parts[1]}"
    elif len(parts) == 2:
        return pub_number
    return pub_number


def load_patent_ids(conn):
    """Load all patent_ids into memory for fast filtering."""
    log.info("Loading all patent_ids from DB into memory...")
    with conn.cursor() as cur:
        cur.execute("SELECT patent_id FROM patents WHERE ipc_codes IS NULL OR ipc_codes = '{}'")
        ids = set(row[0] for row in cur)
    log.info(f"Loaded {len(ids):,} patent_ids needing IPC codes")
    return ids


def load_file(conn, filepath: str, batch_size: int = 5000):
    """Stream JSONL.GZ file and batch-update patents table."""
    log.info(f"Loading IPC/CPC codes from: {filepath}")
    
    # Preload patent_ids for fast in-memory filtering
    valid_ids = load_patent_ids(conn)
    
    batch = []
    total_processed = 0
    total_skipped = 0
    total_updated = 0
    start_time = time.time()
    
    opener = gzip.open if filepath.endswith(".gz") else open
    
    with opener(filepath, "rt", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            
            row = json.loads(line)
            patent_id = normalize_patent_id(row["publication_number"])
            
            # Skip if not in our DB
            if patent_id not in valid_ids:
                total_skipped += 1
                if total_skipped % 500000 == 0:
                    elapsed = time.time() - start_time
                    log.info(f"  Skipped {total_skipped:,} non-matching rows ({elapsed:.0f}s)")
                continue
            
            ipc = row.get("ipc_codes", "")
            cpc = row.get("cpc_codes", "")
            
            # Parse pipe-separated codes into arrays
            ipc_arr = [c.strip() for c in ipc.split("|") if c.strip()] if ipc else []
            cpc_arr = [c.strip() for c in cpc.split("|") if c.strip()] if cpc else []
            
            if not ipc_arr and not cpc_arr:
                continue
            
            batch.append((patent_id, ipc_arr, cpc_arr))
            
            if len(batch) >= batch_size:
                updated = flush_batch(conn, batch)
                total_updated += updated
                total_processed += len(batch)
                batch = []
                
                elapsed = time.time() - start_time
                rate = total_processed / elapsed if elapsed > 0 else 0
                log.info(
                    f"  Processed {total_processed:,} | Updated {total_updated:,} | "
                    f"Skipped {total_skipped:,} | {rate:.0f} rows/sec"
                )
    
    # Final batch
    if batch:
        updated = flush_batch(conn, batch)
        total_updated += updated
        total_processed += len(batch)
    
    elapsed = time.time() - start_time
    log.info(
        f"COMPLETE: Processed {total_processed:,} rows, "
        f"updated {total_updated:,} patents, "
        f"skipped {total_skipped:,} non-matching in {elapsed:.1f}s"
    )
    return total_updated


def flush_batch(conn, batch):
    """Update patents with IPC/CPC codes using VALUES list."""
    with conn.cursor() as cur:
        execute_values(
            cur,
            """UPDATE patents p
               SET ipc_codes = v.ipc_codes, cpc_codes = v.cpc_codes
               FROM (VALUES %s) AS v(patent_id, ipc_codes, cpc_codes)
               WHERE p.patent_id = v.patent_id""",
            [(pid, ipc, cpc) for pid, ipc, cpc in batch],
            template="(%s, %s::text[], %s::text[])",
            page_size=len(batch)
        )
        updated = cur.rowcount
        conn.commit()

    return updated


def main():
    parser = argparse.ArgumentParser(description="Load IPC/CPC codes into patents table")
    parser.add_argument("file", help="Path to JSONL or JSONL.GZ file")
    parser.add_argument("--batch-size", type=int, default=25000)
    args = parser.parse_args()
    
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        ensure_columns(conn)
        load_file(conn, args.file, args.batch_size)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
