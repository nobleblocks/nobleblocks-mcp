#!/usr/bin/env python3
"""
PatentsView API Ingestor — US Patent Data + NPL Citations

PatentsView (https://patentsview.org) is a free USPTO API that provides:
- Patent metadata (title, abstract, assignees, inventors, dates, IPC/CPC)
- Non-Patent Literature (NPL) citations (academic papers cited by patents)
- No registration needed, JSON API, generous rate limits

This is the BEST source for US patent→paper citation links when
bulk.data.gov isn't accessible from the server.

API docs: https://patentsview.org/apis/api-endpoints/patents
Rate limit: ~45 requests/minute
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
from datetime import datetime, timedelta

# Config
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_NAME = os.environ.get("DB_NAME", "paper_search")
DB_USER = os.environ.get("DB_USER", "nobleblocks")
DB_PASS = os.environ.get("DB_PASS", "")

PATENTSVIEW_BASE = "https://api.patentsview.org/patents/query"
BATCH_SIZE = 500
PROGRESS_FILE = "/tmp/patentsview_ingest_progress.json"
PER_PAGE = 100  # PatentsView max per page

DOI_PATTERN = re.compile(r'10\.\d{4,9}/[^\s,;"\'>]+')

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
    return {"last_date": "2024-01-01", "page": 1, "total_patents": 0, "total_links": 0}


def save_progress(progress):
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f)


def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS
    )


def query_patents(from_date, page=1):
    """Query PatentsView for patents with NPL citations."""
    q = json.dumps({"_gte": {"patent_date": from_date}})
    f = json.dumps([
        "patent_number", "patent_title", "patent_abstract",
        "patent_date", "patent_type",
        "assignee_organization", "assignee_type",
        "inventor_first_name", "inventor_last_name",
        "ipc_class", "ipc_subclass",
        "cpc_group_id",
        "cited_patent_number",
        "citedby_patent_number",
    ])
    o = json.dumps({"per_page": PER_PAGE, "page": page})
    s = json.dumps([{"patent_date": "asc"}])

    params = {"q": q, "f": f, "o": o, "s": s}
    resp = requests.get(PATENTSVIEW_BASE, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    if not data:
        # If empty, try POST as fallback
        payload = {"q": json.loads(q), "f": json.loads(f), "o": json.loads(o), "s": json.loads(s)}
        resp = requests.post(PATENTSVIEW_BASE, json=payload,
                           headers={"Content-Type": "application/json"}, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    return data


def query_npl_citations(patent_numbers):
    """Get NPL citations for specific patents.
    PatentsView doesn't directly expose NPL in the main endpoint,
    so we use the /patents/query with cited_patent_category filter."""

    # PatentsView doesn't have a direct NPL endpoint in the public API
    # We'll use the alternative approach: check if patent cites papers via other means
    # For now, return empty — this will be enhanced with USPTO bulk data later
    return []


def extract_patent_data(patent):
    """Extract patent record from PatentsView response."""
    patent_num = patent.get("patent_number", "")
    if not patent_num:
        return None

    # Build inventors list
    inventors = []
    for inv in patent.get("inventors", []):
        first = inv.get("inventor_first_name", "")
        last = inv.get("inventor_last_name", "")
        if first or last:
            inventors.append(f"{first} {last}".strip())

    # Build IPC codes
    ipc_codes = []
    for ipc in patent.get("IPCs", []):
        code = f"{ipc.get('ipc_class', '')}{ipc.get('ipc_subclass', '')}"
        if code.strip():
            ipc_codes.append(code)

    # CPC codes
    cpc_codes = []
    for cpc in patent.get("cpcs", []):
        if cpc.get("cpc_group_id"):
            cpc_codes.append(cpc["cpc_group_id"])

    # Assignee
    assignees = patent.get("assignees", [])
    assignee = assignees[0].get("assignee_organization") if assignees else None
    assignee_type = assignees[0].get("assignee_type") if assignees else None
    # PatentsView type codes: 1=foreign, 2=US company, 3=US individual, 4=US gov, 5=foreign gov
    type_map = {"2": "corporate", "3": "individual", "4": "government", "5": "government"}
    assignee_type = type_map.get(str(assignee_type), "corporate") if assignee_type else None

    return {
        "patent_id": f"US-{patent_num}",
        "title": patent.get("patent_title"),
        "abstract": patent.get("patent_abstract"),
        "grant_date": patent.get("patent_date"),
        "assignee": assignee,
        "assignee_type": assignee_type,
        "inventors": inventors[:20],
        "ipc_codes": ipc_codes[:20],
        "cpc_codes": cpc_codes[:20],
        "jurisdiction": "US",
        "source": "patentsview",
    }


def insert_patents_batch(conn, patents):
    """Bulk insert patents."""
    if not patents:
        return 0

    sql = """
        INSERT INTO patents (patent_id, title, abstract, grant_date, assignee,
                            assignee_type, inventors, ipc_codes, cpc_codes,
                            jurisdiction, source)
        VALUES %s
        ON CONFLICT (patent_id) DO UPDATE SET
            title = COALESCE(EXCLUDED.title, patents.title),
            abstract = COALESCE(EXCLUDED.abstract, patents.abstract),
            assignee = COALESCE(EXCLUDED.assignee, patents.assignee),
            updated_at = NOW()
    """
    values = [
        (p["patent_id"], p.get("title"), p.get("abstract"), p.get("grant_date"),
         p.get("assignee"), p.get("assignee_type"), p.get("inventors", []),
         p.get("ipc_codes", []), p.get("cpc_codes", []),
         p["jurisdiction"], p["source"])
        for p in patents
    ]
    cur = conn.cursor()
    psycopg2.extras.execute_values(cur, sql, values,
        template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
    conn.commit()
    inserted = cur.rowcount
    cur.close()
    return inserted


def main():
    print("=" * 60)
    print("  PatentsView API Ingestor — US Patents")
    print("=" * 60)
    print(f"  DB: {DB_HOST}:{DB_PORT}/{DB_NAME}")
    print(f"  API: {PATENTSVIEW_BASE}")
    print()

    # Test connectivity first
    print("  Testing API connectivity...")
    try:
        test_resp = requests.get(
            "https://api.patentsview.org/patents/query",
            params={"q": json.dumps({"patent_number": "11000000"}), "f": '["patent_number"]'},
            timeout=15
        )
        print(f"  API status: {test_resp.status_code}")
        if test_resp.status_code != 200:
            print(f"  ⚠ API returned {test_resp.status_code}: {test_resp.text[:200]}")
            print("  Trying alternative approach...")
    except Exception as e:
        print(f"  ❌ Cannot reach PatentsView API: {e}")
        print("  This server may not have outbound access to patentsview.org")
        print("  Alternative: Download data locally and upload to S3")
        sys.exit(1)

    conn = get_db_connection()
    progress = load_progress()

    from_date = progress.get("last_date", "2024-01-01")
    page = progress.get("page", 1)
    total_patents = progress.get("total_patents", 0)

    print(f"  From: {from_date}, page: {page}")
    print()

    start_time = time.time()
    patent_batch = []
    consecutive_errors = 0

    while not shutdown_requested:
        try:
            data = query_patents(from_date, page)
            consecutive_errors = 0
        except requests.exceptions.RequestException as e:
            consecutive_errors += 1
            if consecutive_errors > 5:
                print(f"  ❌ Too many consecutive errors. Stopping.")
                break
            print(f"  ⚠ API error: {e}, retrying in 30s...")
            time.sleep(30)
            continue

        patents = data.get("patents", [])
        total_found = data.get("total_patent_count", 0)

        if not patents:
            print("  ✅ No more patents — ingestion complete!")
            break

        for patent in patents:
            record = extract_patent_data(patent)
            if record:
                patent_batch.append(record)

        # Flush batch
        if len(patent_batch) >= BATCH_SIZE:
            inserted = insert_patents_batch(conn, patent_batch)
            total_patents += inserted
            patent_batch = []

        page += 1
        elapsed = time.time() - start_time
        rate = total_patents / elapsed if elapsed > 0 else 0

        if page % 10 == 0:
            print(f"  Page {page}: {total_patents:,}/{total_found:,} patents ({rate:.0f}/s)")
            progress["page"] = page
            progress["total_patents"] = total_patents
            save_progress(progress)

        # Rate limit: ~45 req/min = 1 every 1.3s
        time.sleep(1.5)

    # Flush remaining
    if patent_batch:
        inserted = insert_patents_batch(conn, patent_batch)
        total_patents += inserted

    progress["page"] = page
    progress["total_patents"] = total_patents
    progress["last_run"] = datetime.now().isoformat()
    save_progress(progress)

    elapsed = time.time() - start_time
    print(f"\n{'=' * 60}")
    print(f"  DONE in {elapsed:.0f}s")
    print(f"  US Patents indexed: {total_patents:,}")
    print(f"{'=' * 60}")

    conn.close()


if __name__ == "__main__":
    main()
