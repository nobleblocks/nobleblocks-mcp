#!/usr/bin/env python3
"""Verify ClinicalTrials.gov results appear in phase=fast (MCP) searches."""
import httpx

URL = "https://www.dev.nobleblocks.com/api/v1/papers/search"
HEADERS = {"X-Internal-Token": "6vdNeoQJ0Dbi2-NDhRiNljKZoKCzxxg2vxI-i1oy7u56mdr_YP6K1H9RDu1amXLu"}

queries = [
    "Alzheimer disease clinical trial",
    "CAR-T cell therapy cancer",
    "CRISPR gene therapy sickle cell",
]

for q in queries:
    r = httpx.get(URL, params={"query": q, "phase": "fast", "limit": 20}, headers=HEADERS, timeout=35)
    data = r.json()
    papers = data.get("papers") or data.get("results") or []
    ct_papers = [p for p in papers if p.get("source") == "ClinicalTrials.gov" or "ClinicalTrials.gov" in (p.get("venue") or "")]
    total = len(papers)
    print(f"  [{q}] {total} results, {len(ct_papers)} from ClinicalTrials.gov")
    for p in ct_papers[:2]:
        print(f"    - {p.get('title', '?')[:70]}")
    if not ct_papers:
        # Check sources metadata
        sources = data.get("sources") or {}
        if "ClinicalTrials.gov" in sources:
            print(f"    (CT.gov in sources metadata: {sources['ClinicalTrials.gov']})")
        else:
            print(f"    (No CT.gov in sources — may have timed out or query didn't match)")
