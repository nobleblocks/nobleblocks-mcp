#!/usr/bin/env python3
"""Quick filter verification test for the dev deployment."""
import httpx

URL = "https://www.dev.nobleblocks.com/api/v1/papers/search"
HEADERS = {"X-Internal-Token": "6vdNeoQJ0Dbi2-NDhRiNljKZoKCzxxg2vxI-i1oy7u56mdr_YP6K1H9RDu1amXLu"}

def test_filter(label, params, check_fn):
    r = httpx.get(URL, params=params, headers=HEADERS, timeout=35)
    data = r.json()
    papers = data.get("papers") or data.get("results") or []
    passed, msg = check_fn(papers)
    status = "PASS" if passed else "FAIL"
    print(f"  [{status}] {label} — {len(papers)} results. {msg}")
    return passed

results = []

# Test 1: min_citations=10 should filter out low-citation papers
results.append(test_filter(
    "CRISPR + min_citations=10",
    {"query": "CRISPR gene editing", "min_citations": 10, "sort": "citations", "limit": 20},
    lambda papers: (
        all((p.get("citationCount") or 0) >= 10 for p in papers),
        f"Min cites in results: {min((p.get('citationCount') or 0) for p in papers) if papers else 'N/A'}"
    )
))

# Test 2: min_year=2022 should filter out older papers
results.append(test_filter(
    "machine learning + min_year=2022",
    {"query": "machine learning", "min_year": 2022, "limit": 20},
    lambda papers: (
        all((p.get("year") or 9999) >= 2022 for p in papers),
        f"Years: {sorted(set(p.get('year') for p in papers if p.get('year')))[:5]}"
    )
))

# Test 3: max_year=2020 should filter out newer papers
results.append(test_filter(
    "deep learning + max_year=2020",
    {"query": "deep learning neural networks", "max_year": 2020, "limit": 20},
    lambda papers: (
        all((p.get("year") or 0) <= 2020 for p in papers if p.get("year")),
        f"Years: {sorted(set(p.get('year') for p in papers if p.get('year')))[-5:]}"
    )
))

# Test 4: sort=citations should return papers in descending citation order
results.append(test_filter(
    "cancer + sort=citations",
    {"query": "cancer treatment", "sort": "citations", "limit": 20},
    lambda papers: (
        all((papers[i].get("citationCount") or 0) >= (papers[i+1].get("citationCount") or 0)
            for i in range(len(papers)-1)) if len(papers) > 1 else True,
        f"Top cites: {[p.get('citationCount', 0) for p in papers[:5]]}"
    )
))

# Test 5: Combined min_citations + min_year (the exact scenario that was broken)
results.append(test_filter(
    "CRISPR + min_year=2023 + min_citations=10",
    {"query": "CRISPR", "min_year": 2023, "min_citations": 10, "sort": "citations", "limit": 20},
    lambda papers: (
        all((p.get("citationCount") or 0) >= 10 and (p.get("year") or 0) >= 2023 for p in papers),
        f"All papers >=2023 with >=10 cites: {len(papers)} found"
    )
))

print(f"\n  Summary: {sum(results)}/{len(results)} passed")
