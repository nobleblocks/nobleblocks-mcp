#!/usr/bin/env python3
"""
NobleID Writeback — applies noble_id from _writeback_stage to papers table.

The _writeback_stage table has (doi, noble_id) pairs loaded from NobleID.
This script batch-updates papers.noble_id WHERE papers.doi matches.

Designed for 116M+ rows: processes in batches of 50K with progress logging.
Safe to restart — only updates rows where noble_id IS NULL.
"""

import os
import sys
import time
import logging
import psycopg2
from psycopg2.extras import execute_values

# === Configuration ===
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "paper_search")
DB_USER = os.getenv("DB_USER", "nobleblocks")
DB_PASS = os.getenv("DB_PASS", "nb_papers_2026_prod")

BATCH_SIZE = 50_000  # rows per batch
PROGRESS_INTERVAL = 10  # log every N batches
SLEEP_BETWEEN = 0.5  # seconds between batches (avoid I/O saturation)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("/tmp/nobleid_writeback.log")
    ]
)
log = logging.getLogger(__name__)


def get_conn():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS,
        options="-c statement_timeout=300000"  # 5 min per statement
    )


def ensure_noble_id_column(conn):
    """Add noble_id column to papers if it doesn't exist."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'papers' AND column_name = 'noble_id'
        """)
        if cur.fetchone() is None:
            log.info("Adding noble_id column to papers table...")
            cur.execute("ALTER TABLE papers ADD COLUMN noble_id TEXT")
            conn.commit()
            log.info("Column added.")
        else:
            log.info("papers.noble_id column exists.")


def ensure_index(conn):
    """Create index on _writeback_stage(doi) if not exists."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 1 FROM pg_indexes
            WHERE tablename = '_writeback_stage' AND indexname = 'idx_writeback_stage_doi'
        """)
        if cur.fetchone() is None:
            log.info("Creating index on _writeback_stage(doi)... (this may take a few minutes)")
            conn.commit()  # end any open transaction
            old_autocommit = conn.autocommit
            conn.autocommit = True
            cur.execute("CREATE INDEX CONCURRENTLY idx_writeback_stage_doi ON _writeback_stage(doi)")
            conn.autocommit = old_autocommit
            log.info("Index created.")
        else:
            log.info("_writeback_stage(doi) index exists.")


def get_pending_count(conn):
    """Count papers that need noble_id applied (have a match in _writeback_stage but noble_id IS NULL)."""
    with conn.cursor() as cur:
        # Use estimate for speed on 100M+ tables
        cur.execute("""
            SELECT reltuples::bigint FROM pg_class WHERE relname = '_writeback_stage'
        """)
        stage_est = cur.fetchone()[0]
        cur.execute("""
            SELECT count(*) FROM papers WHERE noble_id IS NOT NULL
        """)
        already_done = cur.fetchone()[0]
        return stage_est, already_done


def run_writeback(conn):
    """
    Batch writeback using a temp table approach:
    1. Select a batch of DOIs from _writeback_stage that haven't been applied yet
    2. UPDATE papers SET noble_id = ws.noble_id WHERE doi matches
    3. Repeat until done
    """
    total_updated = 0
    batch_num = 0
    start_time = time.time()

    # Use a server-side cursor to iterate through _writeback_stage efficiently
    # We join against papers to find only un-applied rows
    with conn.cursor("writeback_cursor") as read_cur:
        read_cur.itersize = BATCH_SIZE
        read_cur.execute("""
            SELECT ws.doi, ws.noble_id
            FROM _writeback_stage ws
            JOIN papers p ON p.doi = ws.doi
            WHERE p.noble_id IS NULL
        """)

        batch = []
        for row in read_cur:
            batch.append(row)
            if len(batch) >= BATCH_SIZE:
                updated = apply_batch(conn, batch)
                total_updated += updated
                batch_num += 1
                batch = []

                if batch_num % PROGRESS_INTERVAL == 0:
                    elapsed = time.time() - start_time
                    rate = total_updated / elapsed if elapsed > 0 else 0
                    log.info(
                        f"Batch {batch_num}: {total_updated:,} updated "
                        f"({rate:.0f} rows/sec, elapsed {elapsed:.0f}s)"
                    )

                time.sleep(SLEEP_BETWEEN)

        # Final partial batch
        if batch:
            updated = apply_batch(conn, batch)
            total_updated += updated
            batch_num += 1

    elapsed = time.time() - start_time
    log.info(
        f"COMPLETE: {total_updated:,} papers updated in {batch_num} batches "
        f"({elapsed:.0f}s, {total_updated/elapsed:.0f} rows/sec)"
    )
    return total_updated


def apply_batch(conn, batch):
    """Apply a batch of (doi, noble_id) updates to papers."""
    with conn.cursor() as cur:
        # Use a temp table for efficient batch update
        cur.execute("""
            CREATE TEMP TABLE IF NOT EXISTS _wb_batch (
                doi TEXT NOT NULL,
                noble_id TEXT NOT NULL
            ) ON COMMIT DELETE ROWS
        """)
        execute_values(cur, "INSERT INTO _wb_batch (doi, noble_id) VALUES %s", batch)
        cur.execute("""
            UPDATE papers p
            SET noble_id = b.noble_id
            FROM _wb_batch b
            WHERE p.doi = b.doi AND p.noble_id IS NULL
        """)
        updated = cur.rowcount
        conn.commit()
        return updated


def main():
    log.info("=" * 70)
    log.info("NOBLEBLOCKS NobleID WRITEBACK — Starting")
    log.info(f"Batch size: {BATCH_SIZE:,}, Sleep between: {SLEEP_BETWEEN}s")
    log.info("=" * 70)

    conn = get_conn()
    try:
        # Step 1: Ensure column exists
        ensure_noble_id_column(conn)

        # Step 2: Ensure index on staging table
        ensure_index(conn)

        # Step 3: Get estimates
        stage_est, already_done = get_pending_count(conn)
        log.info(f"_writeback_stage estimated rows: {stage_est:,}")
        log.info(f"papers with noble_id already set: {already_done:,}")
        pending_est = max(0, stage_est - already_done)
        log.info(f"Estimated pending: ~{pending_est:,}")

        if pending_est == 0:
            log.info("Nothing to do — all records appear to be applied.")
            return

        # Step 4: Run the writeback
        run_writeback(conn)

    except KeyboardInterrupt:
        log.info("Interrupted — progress saved (safe to restart)")
    except Exception as e:
        log.error(f"Error: {e}", exc_info=True)
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
