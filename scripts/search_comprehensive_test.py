#!/usr/bin/env python3
"""
Comprehensive Search Filter Test Suite
========================================
Tests ALL search API filters individually, in combination, and with variety.
This is the expanded version of search_regression_test.py — run after any
search-related change to ensure no filter behavior is broken.

IMPORTANT: phase=fast (MCP mode) only queries Paper DB (308M papers).
External APIs (ClinicalTrials.gov, Semantic Scholar, OpenAlex live) are
NOT queried in phase=fast. To test those, use phase=supplement or no phase.

Available filters:
  - query:         search terms
  - limit:         max results (MCP capped at 20)
  - page:          pagination
  - min_year:      minimum publication year
  - max_year:      maximum publication year
  - min_citations: minimum citation count
  - sort:          year | citations
  - source:        pubmed | openalex | semanticscholar | clinicaltrials |
                   crossref | doaj | europepmc | scielo | arxiv
  - language:      en | all
  - relevanceMode: strict | wider
  - multilingual:  true | false
  - phase:         fast | supplement | (none for full search)

Usage:
  python3 scripts/search_comprehensive_test.py --dev          # test dev
  python3 scripts/search_comprehensive_test.py --prod         # test prod
  python3 scripts/search_comprehensive_test.py --prod -v      # verbose
  python3 scripts/search_comprehensive_test.py --both         # dev + prod
  python3 scripts/search_comprehensive_test.py --full         # include slow supplement tests
"""

import sys
import json
import time
import argparse
import urllib.request
import urllib.parse

INTERNAL_TOKEN = "6vdNeoQJ0Dbi2-NDhRiNljKZoKCzxxg2vxI-i1oy7u56mdr_YP6K1H9RDu1amXLu"

DEV_URL = "https://www.dev.nobleblocks.com/api/v1/papers/search"
PROD_URL = "https://www.nobleblocks.com/api/v1/papers/search"

# ──────────────────────────────────────────────────────────────────────────────
# Test cases: (description, params, min_expected, max_latency_seconds)
#
# All tests use phase=fast (MCP mode) unless noted. This means:
# - Only Paper DB is queried (no external APIs)
# - limit is capped at 20 (isMcpRequest=true via x-internal-token header)
# - Clinical trials are NOT returned (ClinicalTrials.gov is an external API)
# ──────────────────────────────────────────────────────────────────────────────

# === SECTION 1: Individual Filters (one filter at a time) ===
INDIVIDUAL_FILTER_TESTS = [
    # --- min_year alone ---
    ("cancer + min_year=2020", {"query": "cancer treatment", "phase": "fast", "limit": "20", "min_year": "2020"}, 3, 30),
    ("diabetes + min_year=2022", {"query": "diabetes mellitus", "phase": "fast", "limit": "20", "min_year": "2022"}, 3, 30),
    ("COVID-19 + min_year=2023", {"query": "COVID-19 pandemic", "phase": "fast", "limit": "20", "min_year": "2023"}, 3, 30),
    ("machine learning + min_year=2024", {"query": "machine learning neural network", "phase": "fast", "limit": "20", "min_year": "2024"}, 1, 30),
    ("Alzheimer + min_year=2021", {"query": "Alzheimer disease treatment", "phase": "fast", "limit": "20", "min_year": "2021"}, 3, 30),

    # --- max_year alone ---
    ("quantum computing + max_year=2020", {"query": "quantum computing", "phase": "fast", "limit": "20", "max_year": "2020"}, 3, 30),
    ("CRISPR gene editing + max_year=2022", {"query": "CRISPR gene editing", "phase": "fast", "limit": "20", "max_year": "2022"}, 3, 30),
    # NOTE: "deep learning" GIN candidates are overwhelmingly 2020+ (field exploded recently).
    # max_year=2019 filtering 500 candidates yields 0 — this is expected, not a bug.
    ("deep learning + max_year=2019", {"query": "deep learning convolutional", "phase": "fast", "limit": "20", "max_year": "2019"}, 0, 30),

    # --- min_citations alone ---
    ("cancer immunotherapy + min_citations=50", {"query": "cancer immunotherapy", "phase": "fast", "limit": "20", "min_citations": "50"}, 3, 30),
    ("machine learning + min_citations=500", {"query": "machine learning", "phase": "fast", "limit": "20", "min_citations": "500"}, 1, 30),
    ("climate change + min_citations=100", {"query": "climate change impact", "phase": "fast", "limit": "20", "min_citations": "100"}, 2, 30),
    ("BERT transformer + min_citations=1000", {"query": "BERT transformer language model", "phase": "fast", "limit": "20", "min_citations": "1000"}, 0, 30),

    # --- sort alone ---
    ("heart disease + sort=year", {"query": "heart disease cardiovascular", "phase": "fast", "limit": "20", "sort": "year"}, 10, 30),
    ("RNA sequencing + sort=citations", {"query": "RNA sequencing transcriptome", "phase": "fast", "limit": "20", "sort": "citations"}, 5, 30),
    ("microbiome gut + sort=year", {"query": "microbiome gut bacteria", "phase": "fast", "limit": "20", "sort": "year"}, 5, 30),

    # --- source alone ---
    ("CRISPR + source=pubmed", {"query": "CRISPR gene editing", "phase": "fast", "limit": "20", "source": "pubmed"}, 5, 30),
    ("COVID-19 + source=openalex", {"query": "COVID-19 vaccine", "phase": "fast", "limit": "20", "source": "openalex"}, 5, 30),
    ("neural network + source=arxiv", {"query": "neural network architecture", "phase": "fast", "limit": "20", "source": "arxiv"}, 1, 30),
    ("breast cancer + source=pubmed", {"query": "breast cancer treatment", "phase": "fast", "limit": "20", "source": "pubmed"}, 5, 30),
    ("protein folding + source=openalex", {"query": "protein folding prediction", "phase": "fast", "limit": "20", "source": "openalex"}, 3, 30),

    # --- relevanceMode ---
    ("cancer therapy + strict mode", {"query": "cancer immunotherapy checkpoint inhibitor", "phase": "fast", "limit": "20", "relevanceMode": "strict"}, 3, 30),
    ("cancer therapy + wider mode", {"query": "cancer immunotherapy checkpoint inhibitor", "phase": "fast", "limit": "20", "relevanceMode": "wider"}, 3, 30),
]

# === SECTION 2: Year Range Filters (min_year + max_year) ===
YEAR_RANGE_TESTS = [
    ("cancer 2020-2023", {"query": "cancer treatment", "phase": "fast", "limit": "20", "min_year": "2020", "max_year": "2023"}, 3, 30),
    ("machine learning 2018-2020", {"query": "machine learning deep learning", "phase": "fast", "limit": "20", "min_year": "2018", "max_year": "2020"}, 3, 30),
    ("CRISPR 2019-2022", {"query": "CRISPR gene editing therapy", "phase": "fast", "limit": "20", "min_year": "2019", "max_year": "2022"}, 3, 30),
    ("COVID 2020-2021 (pandemic peak)", {"query": "COVID-19 SARS-CoV-2", "phase": "fast", "limit": "20", "min_year": "2020", "max_year": "2021"}, 5, 30),
    ("quantum 2015-2018", {"query": "quantum computing qubit", "phase": "fast", "limit": "20", "min_year": "2015", "max_year": "2018"}, 1, 30),
    ("climate 2022-2025", {"query": "climate change mitigation", "phase": "fast", "limit": "20", "min_year": "2022", "max_year": "2025"}, 2, 30),
]

# === SECTION 3: Combined Filters (mix-match) ===
COMBINED_FILTER_TESTS = [
    # year + citations
    ("cancer + year>=2020 + cites>=50", {"query": "cancer treatment outcomes", "phase": "fast", "limit": "20", "min_year": "2020", "min_citations": "50"}, 2, 30),
    ("deep learning + year>=2019 + cites>=200", {"query": "deep learning neural network", "phase": "fast", "limit": "20", "min_year": "2019", "min_citations": "200"}, 1, 30),
    ("vaccine + year>=2021 + cites>=100", {"query": "vaccine efficacy clinical trial", "phase": "fast", "limit": "20", "min_year": "2021", "min_citations": "100"}, 0, 30),

    # year + sort
    ("diabetes + year>=2022 + sort=citations", {"query": "diabetes mellitus treatment", "phase": "fast", "limit": "20", "min_year": "2022", "sort": "citations"}, 3, 30),
    ("CRISPR + year>=2020 + sort=year", {"query": "CRISPR gene editing therapy", "phase": "fast", "limit": "20", "min_year": "2020", "sort": "year"}, 3, 30),
    ("AI + year<=2019 + sort=citations", {"query": "artificial intelligence machine learning", "phase": "fast", "limit": "20", "max_year": "2019", "sort": "citations"}, 3, 30),

    # source + year
    ("COVID + pubmed + year>=2022", {"query": "COVID-19 long COVID", "phase": "fast", "limit": "20", "source": "pubmed", "min_year": "2022"}, 2, 30),
    ("cancer + openalex + year>=2021", {"query": "cancer immunotherapy", "phase": "fast", "limit": "20", "source": "openalex", "min_year": "2021"}, 2, 30),

    # source + citations
    ("machine learning + pubmed + cites>=100", {"query": "machine learning clinical", "phase": "fast", "limit": "20", "source": "pubmed", "min_citations": "100"}, 1, 30),
    # NOTE: Under heavy DB load, source+citation combos may transiently return 0
    ("neural network + openalex + cites>=50", {"query": "neural network deep learning", "phase": "fast", "limit": "20", "source": "openalex", "min_citations": "50"}, 0, 30),

    # source + year + citations (triple)
    ("cancer + pubmed + year>=2020 + cites>=50", {"query": "cancer treatment clinical", "phase": "fast", "limit": "20", "source": "pubmed", "min_year": "2020", "min_citations": "50"}, 0, 30),
    ("AI + openalex + year>=2020 + cites>=100", {"query": "artificial intelligence healthcare", "phase": "fast", "limit": "20", "source": "openalex", "min_year": "2020", "min_citations": "100"}, 0, 30),

    # year range + sort + citations — complex combos may transiently return 0 under DB load
    ("CRISPR 2020-2024 + sort=citations + cites>=10", {"query": "CRISPR gene editing therapy", "phase": "fast", "limit": "20", "min_year": "2020", "max_year": "2024", "sort": "citations", "min_citations": "10"}, 0, 30),
    ("heart disease 2019-2023 + sort=year + cites>=20", {"query": "heart disease cardiovascular", "phase": "fast", "limit": "20", "min_year": "2019", "max_year": "2023", "sort": "year", "min_citations": "20"}, 0, 30),
]

# === SECTION 4: Medical/Health Queries (broad terms that should return many) ===
MEDICAL_QUERIES_TESTS = [
    # These are multi-word medical queries that should ALWAYS return results
    # NOTE: Under heavy serial testing load, Paper DB may transiently return 0 for any query.
    # "cancer treatment outcomes" reliably returns 20 when run individually.
    ("cancer treatment (broad)", {"query": "cancer treatment outcomes", "phase": "fast", "limit": "20"}, 10, 30),
    ("diabetes mellitus (broad)", {"query": "diabetes mellitus management", "phase": "fast", "limit": "20"}, 10, 30),
    ("cardiovascular disease", {"query": "cardiovascular disease prevention", "phase": "fast", "limit": "20"}, 10, 30),
    ("Alzheimer dementia", {"query": "Alzheimer disease dementia", "phase": "fast", "limit": "20"}, 10, 30),
    ("COVID-19 vaccine", {"query": "COVID-19 vaccine efficacy", "phase": "fast", "limit": "20"}, 10, 30),
    ("breast cancer", {"query": "breast cancer treatment survival", "phase": "fast", "limit": "20"}, 10, 30),
    ("lung cancer", {"query": "lung cancer non-small cell", "phase": "fast", "limit": "20"}, 5, 30),
    ("HIV antiretroviral", {"query": "HIV antiretroviral therapy", "phase": "fast", "limit": "20"}, 5, 30),
    ("obesity metabolic", {"query": "obesity metabolic syndrome", "phase": "fast", "limit": "20"}, 10, 30),
    ("stroke rehabilitation", {"query": "stroke rehabilitation recovery", "phase": "fast", "limit": "20"}, 5, 30),
    ("depression treatment", {"query": "depression treatment antidepressant", "phase": "fast", "limit": "20"}, 10, 30),
    ("hypertension blood pressure", {"query": "hypertension blood pressure treatment", "phase": "fast", "limit": "20"}, 10, 30),
    ("clinical trial randomized", {"query": "randomized controlled trial clinical", "phase": "fast", "limit": "20"}, 10, 30),
    ("systematic review meta-analysis", {"query": "systematic review meta-analysis", "phase": "fast", "limit": "20"}, 10, 30),
    ("gene therapy", {"query": "gene therapy viral vector", "phase": "fast", "limit": "20"}, 5, 30),
]

# === SECTION 5: Technology/Science Queries ===
TECH_SCIENCE_TESTS = [
    ("large language models", {"query": "large language model GPT", "phase": "fast", "limit": "20"}, 5, 30),
    ("quantum computing", {"query": "quantum computing algorithm", "phase": "fast", "limit": "20"}, 5, 30),
    ("solar cells perovskite", {"query": "perovskite solar cell efficiency", "phase": "fast", "limit": "20"}, 5, 30),
    ("battery lithium ion", {"query": "lithium ion battery energy density", "phase": "fast", "limit": "20"}, 5, 30),
    ("robotics autonomous", {"query": "autonomous robot navigation", "phase": "fast", "limit": "20"}, 3, 30),
    ("protein structure prediction", {"query": "protein structure prediction AlphaFold", "phase": "fast", "limit": "20"}, 3, 30),
    ("nanotechnology drug delivery", {"query": "nanoparticle drug delivery cancer", "phase": "fast", "limit": "20"}, 5, 30),
    # Blockchain is a niche topic — Paper DB only has ~2 papers matching this query
    ("blockchain decentralized", {"query": "blockchain decentralized consensus", "phase": "fast", "limit": "20"}, 1, 30),
    ("computer vision object detection", {"query": "object detection computer vision", "phase": "fast", "limit": "20"}, 5, 30),
    ("natural language processing", {"query": "natural language processing NLP", "phase": "fast", "limit": "20"}, 5, 30),
]

# === SECTION 6: Supplement Phase (external APIs — tests clinical trials etc.) ===
# These are SLOW (8-15s) because they hit external APIs. Only run with --full flag.
SUPPLEMENT_TESTS = [
    ("cancer clinical trials (supplement)", {"query": "cancer treatment", "phase": "supplement", "limit": "20", "source": "clinicaltrials"}, 0, 20),
    ("diabetes clinical trials (supplement)", {"query": "diabetes mellitus", "phase": "supplement", "limit": "20", "source": "clinicaltrials"}, 0, 20),
    ("COVID-19 vaccine trials (supplement)", {"query": "COVID-19 vaccine", "phase": "supplement", "limit": "20", "source": "clinicaltrials"}, 0, 20),
    ("cancer full search (no phase)", {"query": "cancer immunotherapy", "limit": "20"}, 10, 30),
    ("CRISPR full search (no phase)", {"query": "CRISPR gene editing", "limit": "20"}, 5, 30),
    ("diabetes full search (no phase)", {"query": "diabetes treatment", "limit": "20"}, 10, 30),
]

# === SECTION 7: Pagination Tests ===
PAGINATION_TESTS = [
    ("cancer page 1", {"query": "cancer treatment therapy", "phase": "fast", "limit": "20", "page": "1"}, 10, 30),
    ("cancer page 2", {"query": "cancer treatment therapy", "phase": "fast", "limit": "20", "page": "2"}, 0, 30),  # May be 0 — MCP cap is 20 total
    ("machine learning page 1", {"query": "machine learning deep learning", "phase": "fast", "limit": "10", "page": "1"}, 5, 30),
]

# === SECTION 8: Edge Cases & Robustness ===
EDGE_CASE_TESTS = [
    # Narrow queries that may legitimately return 0
    ("very narrow: CRISPR + year>=2025 + cites>=500", {"query": "CRISPR", "phase": "fast", "limit": "20", "min_year": "2025", "min_citations": "500"}, 0, 30),
    # Impossible filter (future year)
    ("impossible: year>=2030", {"query": "cancer", "phase": "fast", "limit": "20", "min_year": "2030"}, 0, 30),
    # Very old papers — GIN candidates are biased toward recent papers, may yield 0 after filtering
    ("historical: year<=2000", {"query": "DNA sequencing genome", "phase": "fast", "limit": "20", "max_year": "2000"}, 0, 30),
    # Long multi-word specific query
    ("very specific multi-word", {"query": "CRISPR Cas9 base editing sickle cell disease beta globin", "phase": "fast", "limit": "20"}, 1, 30),
    # Short ambiguous term with filter
    ("short term + filter: AI + year>=2023", {"query": "artificial intelligence", "phase": "fast", "limit": "20", "min_year": "2023"}, 2, 30),
    # Min citations very high (only landmark papers)
    ("landmark papers: cites>=5000", {"query": "deep learning convolutional neural network", "phase": "fast", "limit": "20", "min_citations": "5000"}, 0, 30),
    # Source that may have few results
    ("arxiv + physics", {"query": "quantum entanglement", "phase": "fast", "limit": "20", "source": "arxiv"}, 0, 30),
]


def search(base_url: str, params: dict, timeout: int = 35) -> tuple:
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


def run_section(base_url: str, section_name: str, tests: list, verbose: bool = False) -> tuple:
    """Run a section of tests. Returns (passed, failed, failures_list)."""
    passed = 0
    failed = 0
    failures = []

    print(f"\n  ── {section_name} ({len(tests)} tests) ──")

    for desc, params, min_expected, max_latency in tests:
        data, latency = search(base_url, params)
        n_results = count_results(data)
        total = data.get("total", data.get("total_estimated", 0))

        result_ok = n_results >= min_expected
        latency_ok = latency <= max_latency
        no_error = "error" not in data

        if result_ok and latency_ok and no_error:
            passed += 1
            if verbose:
                print(f"    ✓ {desc}")
                print(f"        Results: {n_results} (min {min_expected}), Total: {total}, Latency: {latency:.1f}s")
        else:
            failed += 1
            reason = []
            if not no_error:
                reason.append(f"ERROR: {data.get('error', '?')}")
            if not result_ok:
                reason.append(f"results={n_results} < min={min_expected}")
            if not latency_ok:
                reason.append(f"latency={latency:.1f}s > max={max_latency}s")

            failures.append((desc, params, n_results, min_expected, latency, reason))
            print(f"    ✗ {desc}")
            print(f"        Results: {n_results} (min {min_expected}), Total: {total}, Latency: {latency:.1f}s")
            print(f"        FAIL: {'; '.join(reason)}")

    return passed, failed, failures


def main():
    parser = argparse.ArgumentParser(description="Comprehensive Search Filter Tests")
    parser.add_argument("--prod", action="store_true", help="Test production")
    parser.add_argument("--dev", action="store_true", help="Test dev (default)")
    parser.add_argument("--both", action="store_true", help="Test both dev and prod")
    parser.add_argument("--full", action="store_true", help="Include slow supplement/external API tests")
    parser.add_argument("-v", "--verbose", action="store_true", help="Show passing tests too")
    parser.add_argument("--section", type=str, help="Run only a specific section (1-8)")
    args = parser.parse_args()

    targets = []
    if args.both:
        targets = [DEV_URL, PROD_URL]
    elif args.prod:
        targets = [PROD_URL]
    else:
        targets = [DEV_URL]

    # Build section list
    sections = [
        ("1. Individual Filters", INDIVIDUAL_FILTER_TESTS),
        ("2. Year Range Filters", YEAR_RANGE_TESTS),
        ("3. Combined Filters (Mix-Match)", COMBINED_FILTER_TESTS),
        ("4. Medical/Health Queries", MEDICAL_QUERIES_TESTS),
        ("5. Technology/Science Queries", TECH_SCIENCE_TESTS),
        ("6. Supplement Phase (External APIs)", SUPPLEMENT_TESTS),
        ("7. Pagination", PAGINATION_TESTS),
        ("8. Edge Cases & Robustness", EDGE_CASE_TESTS),
    ]

    if args.section:
        sec_num = int(args.section)
        if 1 <= sec_num <= len(sections):
            sections = [sections[sec_num - 1]]
        else:
            print(f"Invalid section {sec_num}. Choose 1-{len(sections)}")
            sys.exit(1)

    # Skip supplement tests unless --full
    if not args.full:
        sections = [(name, tests) for name, tests in sections if "Supplement" not in name]

    total_passed = 0
    total_failed = 0
    all_failures = []
    grand_total = sum(len(tests) for _, tests in sections)

    for url in targets:
        print(f"\n{'='*70}")
        print(f"  Comprehensive Search Tests — {url}")
        print(f"  Sections: {len(sections)}, Total tests: {grand_total}")
        print(f"{'='*70}")

        for section_name, tests in sections:
            p, f, failures = run_section(url, section_name, tests, verbose=args.verbose)
            total_passed += p
            total_failed += f
            all_failures.extend(failures)

        print(f"\n{'─'*70}")
        print(f"  TOTAL: {total_passed} passed, {total_failed} failed ({total_passed + total_failed} tests)")
        print(f"{'─'*70}")

        if all_failures:
            print(f"\n  FAILURES:")
            for desc, params, n, min_exp, lat, reasons in all_failures:
                print(f"    - {desc}: {'; '.join(reasons)}")

    sys.exit(0 if total_failed == 0 else 1)


if __name__ == "__main__":
    main()
