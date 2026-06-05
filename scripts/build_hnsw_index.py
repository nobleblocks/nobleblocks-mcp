#!/usr/bin/env python3
"""
Build HNSW index on papers.embedding for vector similarity search.

This creates the index CONCURRENTLY (non-blocking, no table locks) so the
search API keeps working during the build. Expected build time: 4-12 hours
on r6g.2xlarge with ~100M+ embedded papers.

Usage:
  python3 build_hnsw_index.py          # Start build
  python3 build_hnsw_index.py --check  # Check if index exists / build progress

Deploy to paper-db via SSM:
  aws s3 cp build_hnsw_index.py s3://nobleblocks-deploy-temp/paper-db/build_hnsw_index.py
  aws ssm send-command --instance-ids i-0cb48faa3f931c661 \
    --document-name AWS-RunShellScript \
    --parameters 'commands=["aws s3 cp s3://nobleblocks-deploy-temp/paper-db/build_hnsw_index.py /opt/nobleblocks/paper-db/scripts/build_hnsw_index.py && cd /opt/nobleblocks/paper-db/scripts && nohup python3.9 build_hnsw_index.py > /opt/nobleblocks/paper-db/logs/hnsw_build.log 2>&1 &"]' \
    --region ap-southeast-1

Or run via systemd-run (survives SSM disconnect):
  systemd-run --unit=hnsw-build --remain-after-exit \
    python3.9 /opt/nobleblocks/paper-db/scripts/build_hnsw_index.py

Monitor:
  journalctl -u hnsw-build -f
  tail -f /opt/nobleblocks/paper-db/logs/hnsw_build.log
  # Check build progress (pg_stat_progress_create_index):
  psql -d paper_search -c "SELECT * FROM pg_stat_progress_create_index;"
"""

import sys
import time
import psycopg2

DB_PARAMS = {
    "host": "localhost",
    "port": 5432,
    "dbname": "paper_search",
    "user": "nobleblocks",
    "password": "nb_papers_2026_prod",
    "connect_timeout": 10,
}

INDEX_NAME = "idx_papers_embedding_hnsw"
# HNSW params: m=16 (default), ef_construction=64 (default).
# Lower ef_construction = faster build, slightly lower recall.
# For 100M+ vectors, m=16 + ef_construction=64 is a good balance.
# At query time, set hnsw.ef_search = 40-100 for recall/speed tradeoff.
INDEX_SQL = """
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_papers_embedding_hnsw
ON papers USING hnsw (embedding vector_cosine_ops)
WITH (m = 16, ef_construction = 64);
"""

CHECK_SQL = """
SELECT indexname, pg_size_pretty(pg_relation_size(indexname::regclass)) as size
FROM pg_indexes
WHERE tablename = 'papers' AND indexname LIKE '%embed%' OR indexname LIKE '%hnsw%';
"""

PROGRESS_SQL = """
SELECT phase, lockers_total, lockers_done, blocks_total, blocks_done,
       tuples_total, tuples_done,
       CASE WHEN tuples_total > 0 THEN round(100.0 * tuples_done / tuples_total, 1) ELSE 0 END as pct
FROM pg_stat_progress_create_index;
"""


def check_index():
    """Check if HNSW index exists and show build progress."""
    conn = psycopg2.connect(**DB_PARAMS)
    conn.autocommit = True
    cur = conn.cursor()

    # Check existing indexes
    cur.execute(CHECK_SQL)
    rows = cur.fetchall()
    if rows:
        print("Existing embedding indexes:")
        for row in rows:
            print(f"  {row[0]} — size: {row[1]}")
    else:
        print("No embedding/HNSW indexes found.")

    # Check in-progress build
    cur.execute(PROGRESS_SQL)
    progress = cur.fetchall()
    if progress:
        print("\nIndex build in progress:")
        for row in progress:
            print(f"  Phase: {row[0]}, tuples: {row[6]:,}/{row[5]:,} ({row[7]}%)")
    else:
        print("\nNo index build in progress.")

    # Quick embedding count estimate
    cur.execute("""
        SELECT reltuples::bigint as est_rows
        FROM pg_class WHERE relname = 'papers';
    """)
    total = cur.fetchone()[0]
    cur.execute("""
        SELECT count(*) FROM (SELECT 1 FROM papers WHERE embedding IS NOT NULL LIMIT 1000) t;
    """)
    sample = cur.fetchone()[0]
    print(f"\nTable rows (estimate): {total:,}")
    print(f"Embedding sample (first 1000): {sample} have embeddings")

    cur.close()
    conn.close()


def build_index():
    """Build the HNSW index (CONCURRENTLY — non-blocking)."""
    print(f"[{time.strftime('%H:%M:%S')}] Starting HNSW index build (CONCURRENTLY)...")
    print(f"  Index: {INDEX_NAME}")
    print(f"  This will take several hours. Monitor with:")
    print(f"    psql -d paper_search -c \"SELECT * FROM pg_stat_progress_create_index;\"")
    print()

    conn = psycopg2.connect(**DB_PARAMS)
    conn.autocommit = True  # Required for CREATE INDEX CONCURRENTLY
    cur = conn.cursor()

    # Set maintenance_work_mem high for faster build
    cur.execute("SET maintenance_work_mem = '4GB';")

    t0 = time.time()
    try:
        cur.execute(INDEX_SQL)
        elapsed = time.time() - t0
        print(f"\n[{time.strftime('%H:%M:%S')}] HNSW index built successfully in {elapsed/3600:.1f} hours")
    except Exception as e:
        elapsed = time.time() - t0
        print(f"\n[{time.strftime('%H:%M:%S')}] ERROR after {elapsed/60:.1f} min: {e}")
        # Check if it's an "already exists" non-error
        if "already exists" in str(e):
            print("  Index already exists — nothing to do.")
        else:
            raise
    finally:
        cur.close()
        conn.close()

    # Verify
    check_index()


if __name__ == "__main__":
    if "--check" in sys.argv:
        check_index()
    else:
        build_index()
