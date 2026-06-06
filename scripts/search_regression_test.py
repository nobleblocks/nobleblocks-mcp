#!/usr/bin/env python3
"""
MCP Search Regression Test Suite
=================================
Tests the search API with various filter combinations to prevent regressions.
Run against dev or prod after any search-related change.

Usage:
  python3 scripts/search_regression_test.py                # test dev
  python3 scripts/search_regression_test.py --prod         # test prod
  python3 scripts/search_regression_test.py --mcp          # test via MCP endpoint
"""

import sys
import json
import time
import argparse
import urllib.request
import urllib.parse

# Internal token for direct API testing (bypasses auth)
INTERNAL_TOKEN = "6vdNeoQJ0Dbi2-NDhRiNljKZoKCzxxg2vxI-i1oy7u56mdr_YP6K1H9RDu1amXLu"

DEV_URL = "https://www.dev.nobleblocks.com/api/v1/papers/search"
PROD_URL = "https://www.nobleblocks.com/api/v1/papers/search"
MCP_URL = "https://mcp.nobleblocks.com"

# ──────────────────────────────────────────────────────────────────────────────
# Test cases: (description, params, min_expected_results, max_latency_seconds)
# ──────────────────────────────────────────────────────────────────────────────
SEARCH_TESTS = [
    # === Basic queries (no filters) ===
    # NOTE: x-internal-token triggers isMcpRequest=true → limit capped at 20 server-side.
    # Tests sending limit>20 will still get at most 20 results. This is by design.
    # Single broad terms (1 word) may return fewer results when Paper DB caches are cold
    # (after restart). Multi-word queries always hit GIN directly and work reliably.
    ("Single broad term: CRISPR", {"query": "CRISPR", "phase": "fast", "limit": "20"}, 8, 30),
    ("Single broad term: cancer", {"query": "cancer", "phase": "fast", "limit": "20"}, 15, 30),
    ("Single broad term: COVID-19", {"query": "COVID-19", "phase": "fast", "limit": "20"}, 15, 30),
    ("Two-word: machine learning", {"query": "machine learning", "phase": "fast", "limit": "20"}, 10, 30),
    ("Multi-word: attention mechanism transformer", {"query": "attention mechanism transformer", "phase": "fast", "limit": "20"}, 5, 30),
    ("Specific: CRISPR gene editing", {"query": "CRISPR gene editing", "phase": "fast", "limit": "20"}, 5, 30),

    # === Year filters (CRITICAL — these were the original regression) ===
    # Paper DB GIN grabs 500 candidates without year ordering. Client-side filtering
    # must produce at least some results from 500 candidates for broad terms.
    # NOTE: Single-word "CRISPR" + year may return few results when caches cold.
    # Multi-word queries (CRISPR gene editing) are the reliable indicator.
    ("CRISPR + min_year=2020", {"query": "CRISPR", "phase": "fast", "limit": "20", "min_year": "2020"}, 1, 30),
    ("CRISPR + min_year=2023", {"query": "CRISPR", "phase": "fast", "limit": "20", "min_year": "2023"}, 1, 30),
    ("machine learning + min_year=2022", {"query": "machine learning", "phase": "fast", "limit": "20", "min_year": "2022"}, 3, 30),
    ("CRISPR gene editing + min_year=2023", {"query": "CRISPR gene editing", "phase": "fast", "limit": "20", "min_year": "2023"}, 5, 30),

    # === Citation filters ===
    ("CRISPR + min_citations=100", {"query": "CRISPR", "phase": "fast", "limit": "20", "min_citations": "100"}, 3, 30),
    ("cancer treatment + min_citations=50", {"query": "cancer treatment", "phase": "fast", "limit": "20", "min_citations": "50"}, 3, 30),
    ("deep learning + min_citations=200", {"query": "deep learning", "phase": "fast", "limit": "20", "min_citations": "200"}, 2, 30),

    # === Combined year + citation (strict — may be 0 legitimately) ===
    # CRISPR papers from 2023+ rarely have 50+ citations yet. These test that the
    # pipeline doesn't crash, not that results exist.
    ("CRISPR + min_year=2023 + min_citations=50", {"query": "CRISPR", "phase": "fast", "limit": "20", "min_year": "2023", "min_citations": "50"}, 0, 30),
    ("gene therapy + min_year=2020 + min_citations=100", {"query": "gene therapy", "phase": "fast", "limit": "20", "min_year": "2020", "min_citations": "100"}, 1, 30),
    ("CRISPR gene editing + min_year=2023 + min_citations=50 + sort=citations", {"query": "CRISPR gene editing", "phase": "fast", "limit": "20", "min_year": "2023", "min_citations": "50", "sort": "citations"}, 0, 30),

    # === Sort variants ===
    ("CRISPR + sort=citations", {"query": "CRISPR", "phase": "fast", "limit": "20", "sort": "citations"}, 8, 30),
    ("CRISPR + sort=year", {"query": "CRISPR", "phase": "fast", "limit": "20", "sort": "year"}, 8, 30),
    ("machine learning + sort=citations + min_year=2022", {"query": "machine learning", "phase": "fast", "limit": "20", "sort": "citations", "min_year": "2022"}, 3, 30),

    # === Source filters ===
    ("CRISPR + source=pubmed", {"query": "CRISPR", "phase": "fast", "limit": "20", "source": "pubmed"}, 5, 30),
    ("COVID-19 + source=openalex", {"query": "COVID-19", "phase": "fast", "limit": "20", "source": "openalex"}, 5, 30),

    # === Limit variations ===
    # NOTE: MCP requests are capped at 20 results server-side (isMcpRequest=true).
    # Requesting limit=50 still returns max 20. This is correct — MCP doesn't need huge pages.
    # Single broad term "CRISPR" may return 10 when Paper DB cache cold.
    ("CRISPR + limit=20 (MCP max)", {"query": "CRISPR", "phase": "fast", "limit": "20"}, 8, 30),

    # === Edge cases ===
    ("Very specific: CRISPR Cas9 base editing sickle cell", {"query": "CRISPR Cas9 base editing sickle cell", "phase": "fast", "limit": "10"}, 1, 30),
    ("Niche: perovskite solar cells", {"query": "perovskite solar cells", "phase": "fast", "limit": "10"}, 3, 30),
    ("Broad + strict filter: AI + min_year=2024 + min_citations=100", {"query": "artificial intelligence", "phase": "fast", "limit": "20", "min_year": "2024", "min_citations": "100"}, 0, 30),  # Too new for 100 cites
]


def search(base_url: str, params: dict, timeout: int = 35) -> tuple[dict, float]:
    """Execute a search query and return (response_data, latency_seconds)."""
    url = f"{base_url}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url)
    req.add_header("x-internal-token", INTERNAL_TOKEN)
    req.add_header("User-Agent", "nobleblocks-mcp/2.0.0")
    req.add_header("Accept", "application/json")

    t0 = time.time()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read())
            latency = time.time() - t0
            return data, latency
    except Exception as e:
        latency = time.time() - t0
        return {"error": str(e), "papers": [], "results": []}, latency


def count_results(data: dict) -> int:
    """Extract result count from response."""
    papers = data.get("papers") or data.get("results") or []
    return len(papers)


def run_tests(base_url: str, verbose: bool = False) -> tuple[int, int, list]:
    """Run all test cases. Returns (passed, failed, failures_list)."""
    passed = 0
    failed = 0
    failures = []

    print(f"\n{'='*70}")
    print(f"  Search Regression Tests — {base_url}")
    print(f"{'='*70}\n")

    for desc, params, min_expected, max_latency in SEARCH_TESTS:
        data, latency = search(base_url, params)
        n_results = count_results(data)
        total = data.get("total", 0)

        # Check conditions
        result_ok = n_results >= min_expected
        latency_ok = latency <= max_latency
        no_error = "error" not in data

        if result_ok and latency_ok and no_error:
            passed += 1
            status = "✓"
            if verbose:
                print(f"  {status} {desc}")
                print(f"      Results: {n_results} (min {min_expected}), Total: {total}, Latency: {latency:.1f}s")
        else:
            failed += 1
            status = "✗"
            reason = []
            if not no_error:
                reason.append(f"ERROR: {data.get('error', '?')}")
            if not result_ok:
                reason.append(f"results={n_results} < min={min_expected}")
            if not latency_ok:
                reason.append(f"latency={latency:.1f}s > max={max_latency}s")

            failures.append((desc, params, n_results, min_expected, latency, reason))
            print(f"  {status} {desc}")
            print(f"      Results: {n_results} (min {min_expected}), Total: {total}, Latency: {latency:.1f}s")
            print(f"      FAIL: {'; '.join(reason)}")

    return passed, failed, failures


def main():
    parser = argparse.ArgumentParser(description="MCP Search Regression Tests")
    parser.add_argument("--prod", action="store_true", help="Test production")
    parser.add_argument("--dev", action="store_true", help="Test dev (default)")
    parser.add_argument("--both", action="store_true", help="Test both dev and prod")
    parser.add_argument("-v", "--verbose", action="store_true", help="Show passing tests")
    args = parser.parse_args()

    targets = []
    if args.both:
        targets = [DEV_URL, PROD_URL]
    elif args.prod:
        targets = [PROD_URL]
    else:
        targets = [DEV_URL]

    total_passed = 0
    total_failed = 0
    all_failures = []

    for url in targets:
        p, f, failures = run_tests(url, verbose=args.verbose)
        total_passed += p
        total_failed += f
        all_failures.extend(failures)

    print(f"\n{'─'*70}")
    print(f"  TOTAL: {total_passed} passed, {total_failed} failed ({total_passed + total_failed} tests)")
    print(f"{'─'*70}")

    if all_failures:
        print(f"\n  FAILURES:")
        for desc, params, n, minr, lat, reasons in all_failures:
            print(f"    - {desc}: {'; '.join(reasons)}")

    # Exit code: 0 if all pass, 1 if any fail
    sys.exit(0 if total_failed == 0 else 1)


if __name__ == "__main__":
    main()
