#!/usr/bin/env python3
"""Test if cursorMark or sort-based deep pagination works."""
import gzip
import json
import requests

API = "https://developer.uspto.gov/ds-api/enriched_cited_reference_metadata/v3/records"

# Test 1: Try cursorMark (Solr deep paging)
print("=== Test cursorMark ===")
try:
    resp = requests.post(API, data={
        "criteria": "nplIndicator:true",
        "start": 0, "rows": 1,
        "sort": "officeActionDate asc",
        "cursorMark": "*"
    }, timeout=30)
    content = gzip.decompress(resp.content)
    data = json.loads(content)
    print(f"  cursorMark: {json.dumps(data)[:300]}")
except Exception as e:
    print(f"  cursorMark: ERROR — {e}")

# Test 2: Check date range filtering
print("\n=== Date range partition test ===")
date_ranges = [
    "officeActionDate:[2020-01-01 TO 2020-12-31]",
    "officeActionDate:[2021-01-01 TO 2021-12-31]",
    "officeActionDate:[2022-01-01 TO 2022-12-31]",
    "officeActionDate:[2023-01-01 TO 2023-12-31]",
    "officeActionDate:[2024-01-01 TO 2024-12-31]",
    "officeActionDate:[2025-01-01 TO 2025-12-31]",
    "officeActionDate:[2026-01-01 TO 2026-12-31]",
]

total_in_ranges = 0
for dr in date_ranges:
    criteria = f"nplIndicator:true AND {dr}"
    try:
        resp = requests.post(API, data={"criteria": criteria, "start": 0, "rows": 1}, timeout=30)
        content = gzip.decompress(resp.content)
        data = json.loads(content)
        n = data.get("response", {}).get("numFound", 0)
        total_in_ranges += n
        print(f"  {dr}: {n:,} records")
    except Exception as e:
        print(f"  {dr}: ERROR — {e}")

print(f"\n  Total in ranges: {total_in_ranges:,} / 170,244")

# Test 3: Try smaller date ranges for years with > 10K
print("\n=== Monthly breakdown for large years ===")
for month in range(1, 13):
    start = f"2024-{month:02d}-01"
    end = f"2024-{month:02d}-28"  # approximate
    criteria = f"nplIndicator:true AND officeActionDate:[{start} TO {end}]"
    try:
        resp = requests.post(API, data={"criteria": criteria, "start": 0, "rows": 1}, timeout=30)
        content = gzip.decompress(resp.content)
        data = json.loads(content)
        n = data.get("response", {}).get("numFound", 0)
        print(f"  2024-{month:02d}: {n:,}")
    except Exception as e:
        print(f"  2024-{month:02d}: ERROR")
