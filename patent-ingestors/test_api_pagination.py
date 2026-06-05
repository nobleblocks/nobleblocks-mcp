#!/usr/bin/env python3
"""Test USPTO Enriched Citations API pagination limits."""
import gzip
import json
import requests

API = "https://developer.uspto.gov/ds-api/enriched_cited_reference_metadata/v3/records"

for offset in [0, 5000, 9999, 10000, 10001, 20000]:
    try:
        resp = requests.post(API, data={"criteria": "nplIndicator:true", "start": offset, "rows": 1}, timeout=30)
        try:
            content = gzip.decompress(resp.content)
        except:
            content = resp.content
        data = json.loads(content)
        response = data.get("response", data)
        num_found = response.get("numFound", "N/A")
        num_docs = len(response.get("docs", []))
        print(f"  offset={offset:>6}: numFound={num_found}, docs_returned={num_docs}")
    except Exception as e:
        print(f"  offset={offset:>6}: ERROR — {e}")
