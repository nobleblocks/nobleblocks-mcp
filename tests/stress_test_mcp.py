#!/usr/bin/env python3
"""
NobleBlocks MCP Stress Test — 100+ queries across all endpoints.
Tests search, similar, lookup, citation-graph, and KG explore.
Reports failures, latency, and edge cases.
"""
import asyncio
import httpx
import json
import time
import sys
from dataclasses import dataclass, field

# ─── Config ───────────────────────────────────────────────────────────────────
API_BASE = "https://www.nobleblocks.com"
HEADERS = {"User-Agent": "nobleblocks-mcp/2.0.0", "Accept": "application/json"}
TIMEOUT = 30.0
MAX_CONCURRENT = 5  # Don't overwhelm prod

# ─── Test Definitions ─────────────────────────────────────────────────────────

SEARCH_QUERIES = [
    # Basic single-word
    ("CRISPR", {}),
    ("cancer", {}),
    ("diabetes", {}),
    ("Alzheimer", {}),
    ("COVID", {}),
    ("transformer", {}),
    ("BERT", {}),
    ("quantum", {}),
    ("graphene", {}),
    ("microbiome", {}),
    ("epigenetics", {}),
    ("photosynthesis", {}),
    ("nanomedicine", {}),
    ("immunotherapy", {}),
    ("telomeres", {}),
    # Multi-word queries
    ("machine learning drug discovery", {}),
    ("CRISPR gene editing", {}),
    ("deep learning protein folding", {}),
    ("climate change biodiversity", {}),
    ("gut brain axis", {}),
    ("single cell RNA sequencing", {}),
    ("large language models", {}),
    ("attention mechanism neural networks", {}),
    ("mRNA vaccine technology", {}),
    ("autonomous vehicles safety", {}),
    ("quantum computing error correction", {}),
    ("BRCA1 breast cancer risk", {}),
    ("dark matter detection", {}),
    ("antibiotic resistance mechanisms", {}),
    ("renewable energy storage", {}),
    # Natural language questions
    ("what are the latest treatments for lupus", {}),
    ("how does metformin work for diabetes", {}),
    ("what causes Parkinson's disease", {}),
    ("best approaches for managing perimenopause", {}),
    ("how do transformers work in NLP", {}),
    # With filters
    ("CRISPR", {"min_year": 2023}),
    ("cancer immunotherapy", {"min_year": 2022, "min_citations": 10}),
    ("deep learning", {"min_citations": 100}),
    ("COVID vaccine", {"min_year": 2024}),
    ("protein structure", {"source": "pubmed"}),
    ("machine learning", {"sort": "citations"}),
    ("climate change", {"sort": "year"}),
    # Edge cases - short queries
    ("AI", {}),
    ("RNA", {}),
    ("DNA", {}),
    ("HIV", {}),
    ("MRI", {}),
    # Edge cases - typos and near-misses
    ("alzhiemers disease", {}),  # common misspelling
    ("diabeties", {}),  # misspelling
    ("nueral network", {}),  # misspelling
    ("protien folding", {}),  # misspelling
    # Edge cases - special characters (should be handled)
    ("p53 tumor suppressor", {}),
    ("IL-6 inflammation", {}),
    ("COVID-19 variants", {}),
    ("HER2+ breast cancer", {}),
    ("α-synuclein aggregation", {}),
    ("β-amyloid plaque", {}),
    # Edge cases - very long queries
    ("the effects of long-term exposure to particulate matter on cardiovascular disease outcomes in urban populations", {}),
    ("systematic review and meta-analysis of randomized controlled trials evaluating the efficacy of cognitive behavioral therapy", {}),
    # Edge cases - numbers and acronyms
    ("SARS-CoV-2", {}),
    ("GPT-4", {}),
    ("ResNet-50", {}),
    ("PM2.5 health effects", {}),
    # Broad topics (stress: should use cache)
    ("biology", {}),
    ("physics", {}),
    ("chemistry", {}),
    ("medicine", {}),
    ("engineering", {}),
    # Niche topics
    ("optogenetics prefrontal cortex", {}),
    ("CRISPR base editing adenine", {}),
    ("topological insulators spin Hall effect", {}),
    ("chimeric antigen receptor T cell", {}),
    ("metal-organic frameworks gas separation", {}),
    # Limit variations
    ("machine learning", {"limit": 1}),
    ("machine learning", {"limit": 50}),
    ("deep learning", {"limit": 5}),
]

SIMILAR_QUERIES = [
    "Attention Is All You Need",
    "CRISPR-Cas9 genome editing in human embryos",
    "AlphaFold protein structure prediction",
    "mRNA vaccines for infectious diseases",
    "Graph neural networks for molecular property prediction",
    "Single-cell transcriptomics reveals cellular heterogeneity",
    "Quantum supremacy using a programmable superconducting processor",
    "Climate tipping points in the Earth system",
    "Gut microbiome and mental health connection",
    "Large language models emergent abilities",
]

LOOKUP_IDS = [
    "10.1038/s41586-020-2649-2",  # AlphaFold
    "10.1126/science.aax9003",    # Popular paper
    "10.1038/nature14539",        # Deep learning review
    "10.1016/j.cell.2020.09.037", # Cell paper
    "10.1001/jama.2020.6775",     # COVID
    "invalid-doi-12345",          # Should fail gracefully
    "not-a-real-id",              # Should fail gracefully
]

KG_QUERIES = [
    "BRCA1 breast cancer",
    "metformin diabetes",
    "dopamine Parkinson",
    "insulin resistance obesity",
    "serotonin depression",
    "p53 apoptosis",
    "TNF-alpha inflammation",
    "ACE2 SARS-CoV-2",
    "EGFR lung cancer",
    "amyloid beta Alzheimer",
]


@dataclass
class TestResult:
    endpoint: str
    query: str
    status: int
    latency: float
    result_count: int = 0
    error: str = ""
    passed: bool = True


@dataclass
class TestSummary:
    total: int = 0
    passed: int = 0
    failed: int = 0
    errors: list = field(default_factory=list)
    slow: list = field(default_factory=list)
    latencies: list = field(default_factory=list)


async def test_search(client: httpx.AsyncClient, query: str, params: dict) -> TestResult:
    """Test /api/v1/papers/search"""
    start = time.time()
    all_params = {"query": query, "limit": params.get("limit", 10), **params}
    try:
        resp = await client.get(f"{API_BASE}/api/v1/papers/search", params={k: v for k, v in all_params.items() if v is not None})
        latency = time.time() - start
        if resp.status_code != 200:
            return TestResult("search", query, resp.status_code, latency, error=f"HTTP {resp.status_code}", passed=False)
        data = resp.json()
        papers = data.get("papers") or data.get("results") or []
        total = data.get("total", 0)
        # Validation
        passed = True
        error = ""
        if len(papers) == 0 and total == 0:
            # Some queries legitimately return 0
            if query.lower() not in ("invalid", "asdfghjkl"):
                error = f"Zero results for '{query}'"
                passed = False
        return TestResult("search", query, resp.status_code, latency, len(papers), error, passed)
    except httpx.TimeoutException:
        return TestResult("search", query, 504, time.time() - start, error="TIMEOUT", passed=False)
    except Exception as e:
        return TestResult("search", query, 500, time.time() - start, error=str(e)[:200], passed=False)


async def test_similar(client: httpx.AsyncClient, query: str) -> TestResult:
    """Test /api/v1/papers/similar"""
    start = time.time()
    try:
        resp = await client.get(f"{API_BASE}/api/v1/papers/similar", params={"query": query, "limit": 5})
        latency = time.time() - start
        if resp.status_code != 200:
            return TestResult("similar", query, resp.status_code, latency, error=f"HTTP {resp.status_code}", passed=False)
        data = resp.json()
        papers = data.get("papers") or data.get("results") or []
        return TestResult("similar", query, resp.status_code, latency, len(papers))
    except httpx.TimeoutException:
        return TestResult("similar", query, 504, time.time() - start, error="TIMEOUT", passed=False)
    except Exception as e:
        return TestResult("similar", query, 500, time.time() - start, error=str(e)[:200], passed=False)


async def test_lookup(client: httpx.AsyncClient, paper_id: str) -> TestResult:
    """Test /api/v1/papers/lookup"""
    start = time.time()
    try:
        resp = await client.get(f"{API_BASE}/api/v1/papers/lookup", params={"id": paper_id})
        latency = time.time() - start
        if resp.status_code == 404:
            # Expected for invalid IDs
            if "invalid" in paper_id or "not-a-real" in paper_id:
                return TestResult("lookup", paper_id, 404, latency, error="Expected 404", passed=True)
            return TestResult("lookup", paper_id, 404, latency, error="Not found", passed=False)
        if resp.status_code != 200:
            return TestResult("lookup", paper_id, resp.status_code, latency, error=f"HTTP {resp.status_code}", passed=False)
        data = resp.json()
        paper = data.get("paper") or data
        has_title = bool(paper.get("title"))
        return TestResult("lookup", paper_id, resp.status_code, latency, 1 if has_title else 0, 
                         error="" if has_title else "No title in response", passed=has_title)
    except httpx.TimeoutException:
        return TestResult("lookup", paper_id, 504, time.time() - start, error="TIMEOUT", passed=False)
    except Exception as e:
        return TestResult("lookup", paper_id, 500, time.time() - start, error=str(e)[:200], passed=False)


async def test_kg(client: httpx.AsyncClient, query: str) -> TestResult:
    """Test /api/v1/kg/explore"""
    start = time.time()
    try:
        resp = await client.get(f"{API_BASE}/api/v1/kg/explore", params={"query": query, "max_nodes": 10})
        latency = time.time() - start
        if resp.status_code != 200:
            return TestResult("kg", query, resp.status_code, latency, error=f"HTTP {resp.status_code}", passed=False)
        data = resp.json()
        nodes = data.get("nodes") or []
        return TestResult("kg", query, resp.status_code, latency, len(nodes))
    except httpx.TimeoutException:
        return TestResult("kg", query, 504, time.time() - start, error="TIMEOUT", passed=False)
    except Exception as e:
        return TestResult("kg", query, 500, time.time() - start, error=str(e)[:200], passed=False)


async def run_tests():
    """Run all tests with concurrency control."""
    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    results: list[TestResult] = []
    
    async with httpx.AsyncClient(timeout=TIMEOUT, headers=HEADERS) as client:
        async def bounded(coro):
            async with semaphore:
                return await coro
        
        # Build all tasks
        tasks = []
        
        # Search tests (82 queries)
        for query, params in SEARCH_QUERIES:
            tasks.append(bounded(test_search(client, query, params)))
        
        # Similar tests (10 queries) - known to be broken, testing fallback
        for query in SIMILAR_QUERIES:
            tasks.append(bounded(test_similar(client, query)))
        
        # Lookup tests (7 queries)
        for paper_id in LOOKUP_IDS:
            tasks.append(bounded(test_lookup(client, paper_id)))
        
        # KG tests (10 queries)
        for query in KG_QUERIES:
            tasks.append(bounded(test_kg(client, query)))
        
        print(f"\n{'='*70}")
        print(f"  NobleBlocks MCP Stress Test — {len(tasks)} total tests")
        print(f"{'='*70}\n")
        
        # Run with progress
        completed = 0
        for coro in asyncio.as_completed(tasks):
            result = await coro
            results.append(result)
            completed += 1
            status_char = "✓" if result.passed else "✗"
            if not result.passed or result.latency > 10:
                print(f"  [{completed}/{len(tasks)}] {status_char} {result.endpoint:8s} | {result.latency:5.1f}s | {result.query[:50]:50s} | {result.error[:60]}")
            elif completed % 10 == 0:
                print(f"  [{completed}/{len(tasks)}] Progress... ({sum(1 for r in results if r.passed)} passed)")
    
    # ─── Summary ──────────────────────────────────────────────────────────────
    print(f"\n{'='*70}")
    print("  RESULTS SUMMARY")
    print(f"{'='*70}\n")
    
    # Group by endpoint
    endpoints = {}
    for r in results:
        endpoints.setdefault(r.endpoint, []).append(r)
    
    for ep, ep_results in sorted(endpoints.items()):
        passed = sum(1 for r in ep_results if r.passed)
        failed = sum(1 for r in ep_results if not r.passed)
        latencies = [r.latency for r in ep_results if r.passed]
        avg_lat = sum(latencies) / len(latencies) if latencies else 0
        p95_lat = sorted(latencies)[int(len(latencies) * 0.95)] if latencies else 0
        max_lat = max(latencies) if latencies else 0
        
        print(f"  {ep:10s}: {passed}/{len(ep_results)} passed | avg {avg_lat:.1f}s | p95 {p95_lat:.1f}s | max {max_lat:.1f}s")
        if failed:
            print(f"             FAILURES ({failed}):")
            for r in ep_results:
                if not r.passed:
                    print(f"               - [{r.status}] {r.query[:45]} → {r.error[:50]}")
    
    # Overall
    total_passed = sum(1 for r in results if r.passed)
    total_failed = sum(1 for r in results if not r.passed)
    all_latencies = [r.latency for r in results]
    
    print(f"\n{'─'*70}")
    print(f"  TOTAL: {total_passed}/{len(results)} passed, {total_failed} failed")
    print(f"  LATENCY: avg {sum(all_latencies)/len(all_latencies):.1f}s, "
          f"p50 {sorted(all_latencies)[len(all_latencies)//2]:.1f}s, "
          f"p95 {sorted(all_latencies)[int(len(all_latencies)*0.95)]:.1f}s, "
          f"max {max(all_latencies):.1f}s")
    
    # Slow queries (>5s)
    slow = [r for r in results if r.latency > 5 and r.passed]
    if slow:
        print(f"\n  SLOW QUERIES (>{5}s, {len(slow)} total):")
        for r in sorted(slow, key=lambda x: -x.latency)[:15]:
            print(f"    {r.latency:5.1f}s | {r.endpoint:8s} | {r.query[:60]}")
    
    # Zero-result searches
    zero = [r for r in results if r.endpoint == "search" and r.result_count == 0 and r.passed]
    if zero:
        print(f"\n  ZERO-RESULT SEARCHES ({len(zero)}):")
        for r in zero:
            print(f"    {r.query}")
    
    print(f"\n{'='*70}\n")
    
    return total_failed


if __name__ == "__main__":
    failures = asyncio.run(run_tests())
    sys.exit(1 if failures > 5 else 0)  # Allow some expected failures
