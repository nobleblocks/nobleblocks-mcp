#!/usr/bin/env python3
"""Test deep paging - handles both gzip and non-gzip responses."""
import gzip
import json
import requests

API = "https://developer.uspto.gov/ds-api/enriched_cited_reference_metadata/v3/records"

def fetch(criteria, start=0, rows=1):
    resp = requests.post(API, data={"criteria": criteria, "start": start, "rows": rows}, timeout=30)
    try:
        content = gzip.decompress(resp.content).decode("utf-8")
    except:
        content = resp.text
    data = json.loads(content)
    # Handle both response structures
    if "response" in data:
        return data["response"].get("numFound", 0), data["response"].get("docs", [])
    elif "recordTotalCount" in data:
        return data["recordTotalCount"], data.get("results", [])
    else:
        print(f"    Unknown response: {json.dumps(data)[:200]}")
        return 0, []

# Test 1: Partition by techCenter
print("=== Partition by techCenter ===")
for tc in ["1600", "1700", "2100", "2400", "2600", "2800", "3600", "3700"]:
    criteria = f"nplIndicator:true AND techCenter:{tc}"
    n, _ = fetch(criteria)
    print(f"  techCenter={tc}: {n:,}")

# Test 2: Partition by groupArtUnitNumber ranges
print("\n=== Total without filter (for comparison) ===")
n, _ = fetch("nplIndicator:true")
print(f"  All NPL: {n:,}")

# Test 3: Try officeActionDate with proper Solr date format
print("\n=== Date ranges (YYYYMMDD format) ===")
date_ranges = [
    ("20200101", "20201231"),
    ("20210101", "20211231"),
    ("20220101", "20221231"),
    ("20230101", "20231231"),
    ("20240101", "20241231"),
    ("20250101", "20251231"),
]
total = 0
for start_d, end_d in date_ranges:
    criteria = f"nplIndicator:true AND officeActionDate:[{start_d} TO {end_d}]"
    n, _ = fetch(criteria)
    total += n
    print(f"  {start_d}-{end_d}: {n:,}")
print(f"  Sum: {total:,}")

# Test 4: Wildcard filter on patentApplicationNumber
print("\n=== Partition by application number prefix ===")
for prefix in ["16*", "17*", "18*"]:
    criteria = f"nplIndicator:true AND patentApplicationNumber:{prefix}"
    n, _ = fetch(criteria)
    print(f"  appNum={prefix}: {n:,}")
