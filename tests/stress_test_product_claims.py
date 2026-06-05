#!/usr/bin/env python3
"""
Product Claims Feature — Comprehensive Stress Test

Tests URL handling (Amazon, iHerb, Walmart, etc.), product name inputs,
ingredient parsing, edge cases, and response quality.

Run: python3 tests/stress_test_product_claims.py
"""

import asyncio
import time
import json
from dataclasses import dataclass, field

import httpx

API_BASE = "https://www.nobleblocks.com"
TIMEOUT = 90.0  # Product claims can take a while (evidence retrieval)
MAX_CONCURRENT = 3  # Don't hammer the endpoint

HEADERS = {
    "User-Agent": "nobleblocks-mcp/2.0.0",
    "Accept": "application/json",
    "Content-Type": "application/json",
    "x-internal-token": "6vdNeoQJ0Dbi2-NDhRiNljKZoKCzxxg2vxI-i1oy7u56mdr_YP6K1H9RDu1amXLu",
}

# ─── Test categories ────────────────────────────────────────────────────────

TESTS: dict[str, list[dict]] = {
    # ── Amazon URLs (the user's main complaint) ──
    "amazon_urls": [
        {"product_url": "https://www.amazon.com/NatureWise-Vitamin-D3-5000iu-Softgels/dp/B00GB85JR4", "expect_name_contains": "vitamin d"},
        {"product_url": "https://www.amazon.com/Optimum-Nutrition-Standard-Protein-Chocolate/dp/B000QSNYGI", "expect_name_contains": "protein"},
        {"product_url": "https://www.amazon.com/dp/B00GB85JR4", "desc": "no-slug Amazon URL (just /dp/ASIN)"},
        {"product_url": "https://www.amazon.com/Nutricost-Creatine-Monohydrate-Micronized-Powder/dp/B01BI0NZBI/ref=sr_1_1?keywords=creatine&sr=8-1", "expect_name_contains": "creatine"},
        {"product_url": "https://www.amazon.co.uk/Solgar-Vitamin-Vegetable-Capsules-Pack/dp/B000VJ3K3A", "expect_name_contains": "capsule"},
        {"product_url": "https://www.amazon.com/Sports-Research-Triple-Strength-Omega/dp/B01GV4O37E/ref=pd_bxgy_d_sccl_1/137-2826743-4567820?pd_rd_w=xyz", "expect_name_contains": "omega"},
        {"product_url": "https://www.amazon.com/100-Whey-Isolate-Protein-Chocolate-Peppermint/dp/B0EXAMPLE1", "expect_name_contains": "whey"},
        {"product_url": "https://www.amazon.com/Garden-Life-Vitamin-Code-Women/dp/B003TOKMJE", "expect_name_contains": "vitamin"},
    ],

    # ── Other retailer URLs ──
    "retailer_urls": [
        {"product_url": "https://www.iherb.com/pr/california-gold-nutrition-omega-3-premium-fish-oil/61864", "expect_name_contains": "omega"},
        {"product_url": "https://www.walmart.com/ip/Nature-Made-Vitamin-D3-2000-IU-50-mcg-Tablets-250-Count/12345678", "expect_name_contains": "vitamin d"},
        {"product_url": "https://www.thorne.com/products/dp/basic-nutrients-2-day", "desc": "Thorne basic-nutrients (DSLD resolves to nearest match)"},
        {"product_url": "https://www.cvs.com/shop/nature-made-magnesium-oxide-250-mg-tablets-100ct-prodid-1080106", "expect_name_contains": "magnesium"},
        {"product_url": "https://www.target.com/p/nature-made-fish-oil-1200mg-softgels/-/A-14789234", "expect_name_contains": "fish oil"},
    ],

    # ── Product name searches (common supplements) ──
    "product_names": [
        {"product_name": "Creatine Monohydrate", "expect_ingredients": True},
        {"product_name": "Vitamin D3 5000 IU", "expect_ingredients": True},
        {"product_name": "Omega-3 Fish Oil", "expect_ingredients": True},
        {"product_name": "AG1 Athletic Greens", "expect_ingredients": True},
        {"product_name": "Magnesium Glycinate 400mg", "expect_ingredients": True},
        {"product_name": "Ashwagandha KSM-66", "expect_ingredients": True},
        {"product_name": "Collagen Peptides", "expect_ingredients": True},
        {"product_name": "Probiotics 50 Billion CFU", "expect_ingredients": True},
    ],

    # ── Product names with typos ──
    "typo_names": [
        {"product_name": "creetine monohydrate", "desc": "creatine misspelled"},
        {"product_name": "ashwaganda ksm 66", "desc": "ashwagandha misspelled"},
        {"product_name": "glucosimine chondrotin", "desc": "glucosamine chondroitin misspelled"},
        {"product_name": "malatonin 5mg", "desc": "melatonin misspelled"},
        {"product_name": "tumeric curcumin", "desc": "turmeric misspelled"},
    ],

    # ── Skincare products ──
    "skincare": [
        {"product_name": "CeraVe Moisturizing Cream", "expect_ingredients": True},
        {"product_name": "The Ordinary Niacinamide 10% + Zinc 1%", "expect_ingredients": True},
        {"product_name": "La Roche-Posay Anthelios SPF 50", "expect_ingredients": True},
        {"product_url": "https://www.sephora.com/product/the-ordinary-niacinamide-10-zinc-1-P427417", "expect_name_contains": "niacinamide"},
    ],

    # ── Edge cases ──
    "edge_cases": [
        {"product_name": "", "expect_error": True, "desc": "empty product name"},
        {"product_name": "a", "desc": "single char — returns generic result"},
        {"product_url": "not-a-url", "desc": "invalid URL"},
        {"product_url": "https://www.google.com/search?q=vitamin+d", "desc": "non-product URL"},
        {"product_url": "https://amzn.to/3xABcDe", "desc": "Amazon short link"},
        {"product_name": "水素サプリメント", "desc": "Japanese supplement name"},
        {"product_name": "Витамин С 1000мг", "desc": "Russian vitamin C"},
        {"product_name": "🧴💊 Super Health Boost!!!", "desc": "emojis and special chars"},
        {"product_name": "<script>alert('xss')</script>", "desc": "XSS attempt"},
        {"product_name": "'; DROP TABLE products; --", "desc": "SQL injection"},
    ],

    # ── Adversarial / quality checks ──
    "quality_checks": [
        {"product_name": "Vitamin C 1000mg", "expect_score_above": 40, "desc": "well-studied supplement should score high"},
        {"product_name": "Creatine Monohydrate", "expect_score_above": 50, "desc": "one of most studied supplements"},
        {"product_name": "Homeopathic Oscillococcinum", "desc": "homeopathic - should score low"},
        {"product_name": "Quantum Energy Healing Crystals", "desc": "pseudoscience - should score very low"},
    ],
}


@dataclass
class TestResult:
    category: str
    query_desc: str
    status_code: int
    latency_ms: int
    passed: bool
    error: str = ""
    product_name: str = ""
    score: int = -1
    ingredients_found: int = 0
    studies_found: int = 0


async def run_single_test(
    client: httpx.AsyncClient,
    semaphore: asyncio.Semaphore,
    category: str,
    test_case: dict,
) -> TestResult:
    async with semaphore:
        desc = test_case.get("desc", test_case.get("product_url", test_case.get("product_name", "?")))[:60]
        body = {}
        if "product_url" in test_case:
            body["product_url"] = test_case["product_url"]
        if "product_name" in test_case:
            body["product_name"] = test_case["product_name"]

        t0 = time.time()
        try:
            resp = await client.post(
                f"{API_BASE}/api/v1/papers/product-claims",
                json=body,
                headers=HEADERS,
                timeout=TIMEOUT,
            )
            latency = int((time.time() - t0) * 1000)
            status = resp.status_code

            # Handle async (202) — poll for results
            if status == 202:
                data = resp.json()
                job_id = data.get("job_id")
                if job_id:
                    # Poll for up to 60s
                    for _ in range(24):
                        await asyncio.sleep(2.5)
                        poll_resp = await client.get(
                            f"{API_BASE}/api/v1/papers/product-claims?job_id={job_id}",
                            headers=HEADERS,
                            timeout=30.0,
                        )
                        if poll_resp.status_code == 200:
                            data = poll_resp.json()
                            if data.get("status") == "complete":
                                status = 200
                                latency = int((time.time() - t0) * 1000)
                                break
                            elif data.get("status") == "error":
                                return TestResult(category, desc, 500, latency, False, f"Job error: {data.get('error')}")
                    else:
                        return TestResult(category, desc, 202, latency, False, "Job timed out (60s)")
                else:
                    data = resp.json()
            else:
                data = resp.json()

            # Check expected error
            if test_case.get("expect_error"):
                passed = status >= 400
                return TestResult(category, desc, status, latency, passed,
                                  "" if passed else f"Expected error but got {status}")

            # Non-200 is a failure (unless we expected error)
            if status != 200:
                return TestResult(category, desc, status, latency, False,
                                  data.get("error", f"HTTP {status}"))

            # Extract results
            product_name = data.get("product", data.get("product_name", ""))
            overall_score = data.get("overall_score", data.get("score", -1))
            claims = data.get("claims", [])
            ingredients_found = data.get("ingredients_found", len(claims))
            total_studies = sum(len(c.get("top_studies", c.get("evidence", []))) for c in claims)

            # Validate expectations
            errors = []

            # Check product name extraction from URL
            if "expect_name_contains" in test_case:
                expected = test_case["expect_name_contains"].lower()
                if expected not in product_name.lower():
                    errors.append(f"Name '{product_name}' doesn't contain '{expected}'")

            # Check ingredients found
            if test_case.get("expect_ingredients") and ingredients_found == 0:
                errors.append("No ingredients found")

            # Check score threshold
            if "expect_score_above" in test_case:
                threshold = test_case["expect_score_above"]
                if overall_score < threshold:
                    errors.append(f"Score {overall_score} < expected {threshold}")

            passed = len(errors) == 0
            return TestResult(
                category, desc, status, latency, passed,
                "; ".join(errors) if errors else "",
                product_name, overall_score, ingredients_found, total_studies,
            )

        except httpx.TimeoutException:
            latency = int((time.time() - t0) * 1000)
            return TestResult(category, desc, 0, latency, False, "Timeout")
        except Exception as e:
            latency = int((time.time() - t0) * 1000)
            return TestResult(category, desc, 0, latency, False, str(e)[:100])


async def main():
    print("\n" + "=" * 70)
    print("  PRODUCT CLAIMS — COMPREHENSIVE STRESS TEST")
    print("=" * 70)

    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    all_results: list[TestResult] = []

    async with httpx.AsyncClient() as client:
        for category, tests in TESTS.items():
            tasks = [
                run_single_test(client, semaphore, category, tc)
                for tc in tests
            ]
            results = await asyncio.gather(*tasks)

            passed = sum(1 for r in results if r.passed)
            total = len(results)
            latencies = [r.latency_ms for r in results if r.latency_ms > 0]
            avg_lat = int(sum(latencies) / len(latencies)) if latencies else 0
            max_lat = max(latencies) if latencies else 0

            icon = "✓" if passed == total else "✗"
            print(f"\n  {icon} {category:18s}: {passed}/{total} passed | avg {avg_lat/1000:.1f}s | max {max_lat/1000:.1f}s")

            for r in results:
                if not r.passed:
                    print(f"     [{r.status_code}] {r.query_desc[:50]:50s} → {r.error[:60]}")
                elif r.product_name and category in ("amazon_urls", "retailer_urls"):
                    print(f"     ✓ {r.query_desc[:40]:40s} → \"{r.product_name[:40]}\" (score={r.score}, {r.ingredients_found} ingredients, {r.studies_found} studies)")

            all_results.extend(results)

    # Summary
    total_passed = sum(1 for r in all_results if r.passed)
    total_tests = len(all_results)
    latencies = [r.latency_ms for r in all_results if r.latency_ms > 0]
    latencies.sort()

    print("\n" + "─" * 70)
    print(f"  TOTAL: {total_passed}/{total_tests} passed, {total_tests - total_passed} failed")
    if latencies:
        p50 = latencies[len(latencies) // 2]
        p95 = latencies[int(len(latencies) * 0.95)]
        print(f"  LATENCY: avg {sum(latencies)/len(latencies)/1000:.1f}s, p50 {p50/1000:.1f}s, p95 {p95/1000:.1f}s, max {max(latencies)/1000:.1f}s")

    # Quality report
    print("\n  QUALITY REPORT:")
    for r in all_results:
        if r.category == "quality_checks" and r.score >= 0:
            print(f"    {r.query_desc[:45]:45s} → Score: {r.score}/100 ({r.ingredients_found} ingredients, {r.studies_found} studies)")
    for r in all_results:
        if r.category == "amazon_urls" and r.product_name:
            print(f"    Amazon: {r.query_desc[:35]:35s} → \"{r.product_name}\"")

    print("\n" + "=" * 70)


if __name__ == "__main__":
    asyncio.run(main())
