#!/usr/bin/env python3
"""
Comprehensive MCP Stress Test — Edge Cases & Abuse Patterns

Tests 2-letter words, 3-letter, 4-letter, 5-letter, 10-word phrases,
35-word searches, typos, bad spellings, different languages, Unicode,
special characters, and lit review features.

Run: python3 tests/stress_test_edge_cases.py
"""

import asyncio
import time
import json
from dataclasses import dataclass, field

import httpx

API_BASE = "https://www.nobleblocks.com"
TIMEOUT = 35.0
MAX_CONCURRENT = 5

HEADERS = {
    "User-Agent": "nobleblocks-mcp/2.0.0",
    "Accept": "application/json",
    "x-internal-token": "6vdNeoQJ0Dbi2-NDhRiNljKZoKCzxxg2vxI-i1oy7u56mdr_YP6K1H9RDu1amXLu",
}

# ─── Test categories ────────────────────────────────────────────────────────

# 2-letter words (minimum viable queries)
TWO_LETTER = [
    ("AI", {}),
    ("MR", {}),
    ("CT", {}),
    ("UV", {}),
    ("pH", {}),
]

# 3-letter words
THREE_LETTER = [
    ("RNA", {}),
    ("DNA", {}),
    ("HIV", {}),
    ("MRI", {}),
    ("EEG", {}),
]

# 4-letter words
FOUR_LETTER = [
    ("CRISPR"[:4], {}),  # "CRIS"
    ("gene", {}),
    ("drug", {}),
    ("cell", {}),
    ("lung", {}),
]

# 5-letter words
FIVE_LETTER = [
    ("brain", {}),
    ("heart", {}),
    ("COVID", {}),
    ("tumor", {}),
    ("liver", {}),
]

# 10-word phrase searches
TEN_WORD_PHRASES = [
    ("machine learning for early detection of Alzheimer disease biomarkers", {}),
    ("CRISPR gene therapy clinical trials for sickle cell disease", {}),
    ("deep reinforcement learning applications in autonomous vehicle navigation systems", {}),
    ("long COVID cardiovascular complications in young adults systematic review", {}),
    ("single cell RNA sequencing reveals tumor microenvironment heterogeneity in melanoma", {}),
]

# 35-word extreme searches (natural language questions)
LONG_SEARCHES = [
    ("What are the most effective combination immunotherapy approaches using checkpoint inhibitors and CAR-T cell therapy for treating refractory diffuse large B-cell lymphoma in elderly patients who have failed at least two prior lines of therapy including R-CHOP", {}),
    ("I want to find all systematic reviews and meta-analyses published after 2020 that compare the long-term cardiovascular outcomes of GLP-1 receptor agonists versus SGLT2 inhibitors in patients with type 2 diabetes mellitus and established atherosclerotic cardiovascular disease", {}),
]

# Bad spellings / typos (should trigger "did_you_mean")
TYPOS = [
    ("alzhiemers disease treatment", {}),
    ("nueral networks deep lerning", {}),
    ("cardiovascualr hypertention", {}),
    ("diabeties insuline resistance", {}),
    ("vacine mRNA tecnology", {}),
    ("cancre immunotherpy", {}),
    ("epigentcs dna methlyation", {}),
    ("parkinsons nuero degeneration", {}),
    ("quantm computing eror correction", {}),
    ("machien learing natral language", {}),
]

# Different languages
MULTILINGUAL = [
    ("機械学習 深層学習", {}),  # Japanese: machine learning, deep learning
    ("الذكاء الاصطناعي", {}),  # Arabic: artificial intelligence
    ("apprentissage automatique", {}),  # French: machine learning
    ("aprendizaje profundo redes neuronales", {}),  # Spanish: deep learning neural networks
    ("Maschinelles Lernen Gesundheitswesen", {}),  # German: machine learning healthcare
    ("인공지능 의료", {}),  # Korean: AI medicine
    ("机器学习 蛋白质结构预测", {}),  # Chinese: ML protein structure prediction
    ("нейронные сети обработка языка", {}),  # Russian: neural networks language processing
    ("aprendizado de máquina diagnóstico", {}),  # Portuguese: machine learning diagnosis
    ("yapay zeka kanser tedavisi", {}),  # Turkish: AI cancer treatment
]

# Special characters / Unicode edge cases
SPECIAL_CHARS = [
    ("β-amyloid plaque", {}),
    ("α-synuclein aggregation", {}),
    ("TNF-α inhibitor", {}),
    ("IL-6 signaling", {}),
    ("p53 tumor suppressor", {}),
    ("HER2+ breast cancer", {}),
    ("CD4+ T cells", {}),
    ("ΔΔCt method", {}),
    ("SARS-CoV-2 spike protein", {}),
    ("Ca²⁺ channel", {}),
]

# With min_year and sort params (auth test)
WITH_PARAMS = [
    ("CRISPR", {"min_year": 2023, "sort": "citations"}),
    ("mRNA vaccine", {"min_year": 2024}),
    ("large language models", {"sort": "year"}),
    ("quantum computing", {"min_year": 2022, "sort": "relevance"}),
    ("climate change mitigation", {"min_year": 2023, "sort": "citations"}),
]

# Lit review / SLR style queries (long, specific, clinical)
LIT_REVIEW = [
    ("systematic review efficacy of cognitive behavioral therapy for treatment-resistant depression in adolescents", {}),
    ("meta-analysis comparing PD-1 and PD-L1 inhibitors overall survival non-small cell lung cancer", {}),
    ("randomized controlled trials SGLT2 inhibitors heart failure preserved ejection fraction", {}),
    ("scoping review artificial intelligence radiology diagnostic accuracy breast cancer screening", {}),
    ("network meta-analysis biologics psoriatic arthritis PASI 90 response", {}),
]

# Adversarial / garbage inputs
ADVERSARIAL = [
    ("", {}),  # empty
    ("a", {}),  # single char
    ("   ", {}),  # whitespace only
    ("asdfghjkl", {}),  # random keyboard
    ("12345678", {}),  # numbers only
    ("<script>alert('xss')</script>", {}),  # XSS attempt
    ("'; DROP TABLE papers; --", {}),  # SQL injection
    ("a" * 500, {}),  # extremely long single "word"
    ("the and or but if", {}),  # all stopwords
    ("🧬🔬🧪💊🦠", {}),  # emoji only
]


@dataclass
class TestResult:
    category: str
    query: str
    status: int
    latency: float
    count: int = 0
    error: str = ""
    passed: bool = True
    did_you_mean: str = ""


async def test_search(client: httpx.AsyncClient, query: str, params: dict, category: str) -> TestResult:
    """Test a single search query."""
    start = time.time()
    all_params = {"query": query, "limit": 10, "phase": "fast", **params}
    try:
        resp = await client.get(
            f"{API_BASE}/api/v1/papers/search",
            params={k: v for k, v in all_params.items() if v is not None and v != ""},
        )
        latency = time.time() - start

        if resp.status_code == 400:
            # Expected for empty/invalid queries
            return TestResult(category, query[:50], resp.status_code, latency, passed=True)
        if resp.status_code != 200:
            return TestResult(category, query[:50], resp.status_code, latency, error=f"HTTP {resp.status_code}", passed=False)

        data = resp.json()
        papers = data.get("papers") or data.get("results") or []
        total = data.get("total", 0)
        corrected = data.get("correctedQuery") or ""

        # For adversarial inputs, any non-500 response is a pass
        if category == "adversarial":
            return TestResult(category, query[:50], resp.status_code, latency, len(papers), passed=True, did_you_mean=corrected)

        # For typos, check if did_you_mean is returned
        passed = True
        error = ""
        if category == "typos" and not corrected:
            error = "No spelling correction returned"
            # Not a hard fail — some typos might not be in dictionary
            passed = True

        # For normal queries, zero results for known-good terms is concerning
        if category not in ("adversarial", "special_chars", "multilingual") and len(papers) == 0 and total == 0:
            if query.strip() and len(query.strip()) >= 3:
                error = f"Zero results"
                # Not a hard fail for multilingual/special — some may legitimately be empty
                if category in ("two_letter", "three_letter", "five_letter", "ten_word", "with_params"):
                    passed = False

        return TestResult(category, query[:50], resp.status_code, latency, len(papers), error, passed, corrected)

    except httpx.TimeoutException:
        return TestResult(category, query[:50], 504, time.time() - start, error="TIMEOUT", passed=False)
    except Exception as e:
        return TestResult(category, query[:50], 500, time.time() - start, error=str(e)[:100], passed=False)


async def run_all():
    """Run all test categories."""
    all_tests = [
        ("two_letter", TWO_LETTER),
        ("three_letter", THREE_LETTER),
        ("four_letter", FOUR_LETTER),
        ("five_letter", FIVE_LETTER),
        ("ten_word", TEN_WORD_PHRASES),
        ("long_search", LONG_SEARCHES),
        ("typos", TYPOS),
        ("multilingual", MULTILINGUAL),
        ("special_chars", SPECIAL_CHARS),
        ("with_params", WITH_PARAMS),
        ("lit_review", LIT_REVIEW),
        ("adversarial", ADVERSARIAL),
    ]

    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    results: list[TestResult] = []

    async with httpx.AsyncClient(timeout=TIMEOUT, headers=HEADERS) as client:
        async def bounded(coro):
            async with semaphore:
                return await coro

        tasks = []
        for category, queries in all_tests:
            for query, params in queries:
                tasks.append(bounded(test_search(client, query, params, category)))

        results = await asyncio.gather(*tasks)

    # ── Report ──
    print("\n" + "=" * 70)
    print("  COMPREHENSIVE EDGE-CASE STRESS TEST")
    print("=" * 70)

    categories = {}
    for r in results:
        if r.category not in categories:
            categories[r.category] = []
        categories[r.category].append(r)

    total_pass = 0
    total_fail = 0
    all_latencies = []
    typo_corrections = []

    for cat, cat_results in categories.items():
        passed = sum(1 for r in cat_results if r.passed)
        failed = sum(1 for r in cat_results if not r.passed)
        total_pass += passed
        total_fail += failed
        latencies = [r.latency for r in cat_results if r.latency > 0]
        all_latencies.extend(latencies)
        avg_lat = sum(latencies) / len(latencies) if latencies else 0
        max_lat = max(latencies) if latencies else 0

        status = "✓" if failed == 0 else "✗"
        print(f"\n  {status} {cat:15s}: {passed}/{len(cat_results)} passed | avg {avg_lat:.1f}s | max {max_lat:.1f}s")

        # Show failures
        failures = [r for r in cat_results if not r.passed]
        for r in failures[:5]:
            print(f"     [{r.status}] {r.query} → {r.error}")

        # Show spelling corrections for typo category
        if cat == "typos":
            corrections = [r for r in cat_results if r.did_you_mean]
            if corrections:
                print(f"     Corrections returned: {len(corrections)}/{len(cat_results)}")
                for r in corrections[:3]:
                    print(f"       \"{r.query}\" → \"{r.did_you_mean}\"")
            else:
                print(f"     ⚠ No spelling corrections returned (check localSpellCheck)")

        # Show zero-result warnings
        zeros = [r for r in cat_results if r.count == 0 and r.passed and r.error]
        if zeros and cat != "adversarial":
            print(f"     Zero results: {len(zeros)}")
            for r in zeros[:3]:
                print(f"       \"{r.query}\"")

    # Overall summary
    total = total_pass + total_fail
    print(f"\n{'─' * 70}")
    print(f"  TOTAL: {total_pass}/{total} passed, {total_fail} failed")
    if all_latencies:
        all_latencies.sort()
        p50 = all_latencies[len(all_latencies) // 2]
        p95 = all_latencies[int(len(all_latencies) * 0.95)]
        print(f"  LATENCY: avg {sum(all_latencies)/len(all_latencies):.1f}s, p50 {p50:.1f}s, p95 {p95:.1f}s, max {max(all_latencies):.1f}s")

    # Slow queries
    slow = [(r.latency, r.category, r.query) for r in results if r.latency > 5.0]
    if slow:
        slow.sort(reverse=True)
        print(f"\n  SLOW (>5s): {len(slow)} queries")
        for lat, cat, q in slow[:10]:
            print(f"     {lat:.1f}s | {cat:12s} | {q}")

    print("\n" + "=" * 70)


if __name__ == "__main__":
    asyncio.run(run_all())
