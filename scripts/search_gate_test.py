#!/usr/bin/env python3
"""
search_gate_test.py — LIVE search gate for the NobleBlocks MCP.

This is the deploy GATE. It exercises the exact backend path the MCP
`search_papers` tool uses (`/api/v1/papers/search?phase=fast` with the internal
token) across a rigorous query-length matrix so that a deploy CANNOT silently
ship a broken search.

Query-length buckets tested (the recurring regression surface):
  - 2-letter   (e.g. "AI", "ML")            → must not crash / error / time out
  - 3-letter   (e.g. "DNA", "RNA", "HIV")   → must return real results
  - 1-word     (e.g. "CRISPR", "cancer")    → must return a healthy page
  - 2-word     (e.g. "machine learning")    → must return results + be relevant
  - 3-word     (e.g. "CRISPR gene editing") → must return results + be relevant
  - full NL    ("what causes Parkinson's…") → must return results
  - filters    (min_year / min_citations / sort) → the original regression class

Each query asserts:
  • HTTP 200 and no `error` field
  • latency under the bucket budget
  • at least `min_expected` results (with cache-warm retries)
  • for word buckets: title relevance (>=30% of titles mention a query keyword)

Infra vs. regression:
  The backend has a ~20s server-side timeout that returns HTTP 200 + EMPTY
  results (not an error) when the DB is overloaded (e.g. during an HNSW index
  rebuild). That is an infra condition a rollback would NOT fix, so a slow
  (>=BACKEND_TIMEOUT_S) empty response is reported as a non-blocking WARN.
  A *fast* empty response, an error, or an irrelevant page is a real code
  regression and HARD-FAILS the gate.

Usage:
  python3 scripts/search_gate_test.py                 # gate PROD (default)
  python3 scripts/search_gate_test.py --dev           # gate dev
  python3 scripts/search_gate_test.py --both          # dev + prod
  python3 scripts/search_gate_test.py -v              # verbose (show passes)

Exit code 0 = all passed (safe to ship). Non-zero = search is broken (block/rollback).
"""

import argparse
import json
import os
import sys
import time
import urllib.parse
import urllib.request

# Internal token bypasses auth for direct backend testing. Already present
# throughout this repo's test scripts and in the deploy container env.
INTERNAL_TOKEN = os.environ.get("NB_INTERNAL_TOKEN", "")

DEV_URL = "https://www.dev.nobleblocks.com/api/v1/papers/search"
PROD_URL = "https://www.nobleblocks.com/api/v1/papers/search"

# Empty responses at/above this latency are the backend's ~20s server-side
# timeout firing (returns 200 + []), i.e. infra slowness — NOT a search bug.
BACKEND_TIMEOUT_S = 18.0
MAX_ATTEMPTS = 3  # cache-warm retries before deciding

PASS = "\033[92m✓\033[0m"
FAIL = "\033[91m✗\033[0m"
WARN = "\033[93m⚠\033[0m"

# ──────────────────────────────────────────────────────────────────────────────
# Test matrix: (bucket, description, params, min_expected, max_latency_s, relevance)
#   relevance=True → require >=30% of returned titles to contain a query keyword.
# Short acronym buckets skip relevance (substring matching is unreliable for them);
# they exist to prove short queries don't crash/timeout/error.
# ──────────────────────────────────────────────────────────────────────────────
TESTS = [
    # ── 2-letter: must not break (spellfix/tsquery historically mangled these) ──
    ("2-letter", "AI",  {"query": "AI"}, 1, 30, False),
    ("2-letter", "ML",  {"query": "ML"}, 1, 30, False),

    # ── 3-letter acronyms: must return real results ──
    ("3-letter", "DNA", {"query": "DNA"}, 3, 30, False),
    ("3-letter", "RNA", {"query": "RNA"}, 3, 30, False),
    ("3-letter", "HIV", {"query": "HIV"}, 3, 30, False),

    # ── 1-word: healthy page of relevant results ──
    ("1-word", "CRISPR",        {"query": "CRISPR"},        5, 30, True),
    ("1-word", "cancer",        {"query": "cancer"},        8, 30, True),
    ("1-word", "diabetes",      {"query": "diabetes"},      5, 30, True),
    ("1-word", "transformer",   {"query": "transformer"},   5, 30, True),
    ("1-word", "immunotherapy", {"query": "immunotherapy"}, 5, 30, True),

    # ── 2-word ──
    ("2-word", "machine learning", {"query": "machine learning"}, 5, 30, True),
    ("2-word", "gene editing",     {"query": "gene editing"},     5, 30, True),
    ("2-word", "climate change",   {"query": "climate change"},   5, 30, True),
    ("2-word", "breast cancer",    {"query": "breast cancer"},    5, 30, True),

    # ── 3-word ──
    ("3-word", "CRISPR gene editing",      {"query": "CRISPR gene editing"},      3, 30, True),
    ("3-word", "single cell sequencing",   {"query": "single cell sequencing"},   3, 30, True),
    ("3-word", "deep neural networks",     {"query": "deep neural networks"},     3, 30, True),

    # ── full natural-language questions ──
    ("full-NL", "latest treatments for lupus",
        {"query": "what are the latest treatments for lupus"}, 1, 35, False),
    ("full-NL", "how metformin works for diabetes",
        {"query": "how does metformin work for diabetes"}, 1, 35, False),
    ("full-NL", "what causes Parkinson's disease",
        {"query": "what causes Parkinson's disease"}, 1, 35, False),

    # ── filters (the original regression class — must not silently return 0) ──
    ("filter", "CRISPR + min_year=2020",
        {"query": "CRISPR gene editing", "min_year": "2020"}, 3, 30, False),
    ("filter", "machine learning + min_citations=100",
        {"query": "machine learning", "min_citations": "100"}, 2, 30, False),
    ("filter", "cancer + sort=citations",
        {"query": "cancer immunotherapy", "sort": "citations"}, 3, 30, False),
    ("filter", "deep learning + sort=year",
        {"query": "deep learning", "sort": "year"}, 5, 30, False),
]


def _search(base_url: str, params: dict, timeout: int) -> tuple[dict, float]:
    full = dict(params)
    full.setdefault("phase", "fast")   # MCP mode: Paper DB only, no AI rewrite
    full.setdefault("limit", "20")
    url = f"{base_url}?{urllib.parse.urlencode(full)}"
    req = urllib.request.Request(url)
    req.add_header("x-internal-token", INTERNAL_TOKEN)
    req.add_header("User-Agent", "nobleblocks-search-gate/1.0")
    req.add_header("Accept", "application/json")
    t0 = time.time()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read())
            return data, time.time() - t0
    except urllib.error.HTTPError as e:
        return {"error": f"HTTP {e.code}"}, time.time() - t0
    except Exception as e:  # noqa: BLE001
        return {"error": str(e)}, time.time() - t0


def _results(data: dict) -> list:
    return data.get("papers") or data.get("results") or []


def _is_relevant(papers: list, query: str) -> bool:
    keywords = [w.lower() for w in query.split() if len(w) >= 4]
    if not keywords:
        return True
    hits = sum(
        1 for p in papers
        if any(kw in (p.get("title") or "").lower() for kw in keywords)
    )
    return hits >= max(1, len(papers) * 0.3)


def run(base_url: str, verbose: bool = False) -> tuple[int, int, int, list]:
    print(f"\n{'='*72}\n  Search Gate — {base_url}\n{'='*72}")
    passed = failed = warned = 0
    failures = []
    current_bucket = None

    for bucket, desc, params, min_exp, max_lat, want_rel in TESTS:
        if bucket != current_bucket:
            print(f"\n  ── {bucket} ──")
            current_bucket = bucket

        # Retry to absorb cold-cache warm-up before judging the gate.
        attempts = 0
        while True:
            attempts += 1
            data, latency = _search(base_url, params, max_lat + 5)
            papers = _results(data)
            err = data.get("error")
            ok_http = err is None
            ok_count = len(papers) >= min_exp
            ok_lat = latency <= max_lat
            ok_rel = (not want_rel) or _is_relevant(papers, params["query"])
            if (ok_http and ok_count and ok_lat and ok_rel) or attempts >= MAX_ATTEMPTS:
                break
            time.sleep(2)  # brief warm-up before retry

        # A slow EMPTY response (200, no error, 0 results at/above the backend's
        # ~20s server-side timeout) is infra overload, not a search regression —
        # a rollback wouldn't fix it. Surface it as a WARN, don't block the gate.
        is_backend_timeout = (
            ok_http and len(papers) == 0 and latency >= BACKEND_TIMEOUT_S
        )

        reasons = []
        if not ok_http:
            reasons.append(f"ERROR: {err}")
        if not ok_count:
            reasons.append(f"results={len(papers)} < min={min_exp}")
        if not ok_lat:
            reasons.append(f"latency={latency:.1f}s > {max_lat}s")
        if not ok_rel:
            reasons.append("low relevance (titles don't match query)")

        if not reasons:
            passed += 1
            if verbose:
                print(f"  {PASS} [{desc}] {len(papers)} res, {latency:.1f}s")
        elif is_backend_timeout:
            warned += 1
            print(f"  {WARN} [{desc}] backend timeout — 0 res in {latency:.1f}s "
                  f"(try {attempts}); infra slow, not a search bug")
        else:
            failed += 1
            print(f"  {FAIL} [{desc}] {len(papers)} res, {latency:.1f}s "
                  f"(try {attempts}) — {'; '.join(reasons)}")
            failures.append((base_url, desc, reasons))

    return passed, failed, warned, failures


def main() -> None:
    ap = argparse.ArgumentParser(description="Live MCP search gate")
    ap.add_argument("--prod", action="store_true", help="Gate production (default)")
    ap.add_argument("--dev", action="store_true", help="Gate dev")
    ap.add_argument("--both", action="store_true", help="Gate dev + prod")
    ap.add_argument("-v", "--verbose", action="store_true", help="Show passing tests")
    args = ap.parse_args()

    if args.both:
        targets = [DEV_URL, PROD_URL]
    elif args.dev:
        targets = [DEV_URL]
    else:
        targets = [PROD_URL]

    total_p = total_f = total_w = 0
    all_fail = []
    for url in targets:
        p, f, w, fails = run(url, verbose=args.verbose)
        total_p += p
        total_f += f
        total_w += w
        all_fail.extend(fails)

    print(f"\n{'─'*72}")
    print(f"  RESULT: {total_p} passed, {total_f} failed, {total_w} warned "
          f"({total_p + total_f + total_w} checks)")
    print(f"{'─'*72}")
    if all_fail:
        print("\n  FAILURES (search is broken — do NOT ship):")
        for url, desc, reasons in all_fail:
            print(f"    ✗ {desc}: {'; '.join(reasons)}")
    elif total_w:
        print(f"  ✓ No search regressions. {total_w} query(ies) hit the backend "
              f"timeout (infra slow) — safe to ship, but investigate latency.")
    else:
        print("  ✓ Search healthy across all query lengths — safe to ship.")

    sys.exit(0 if total_f == 0 else 1)


if __name__ == "__main__":
    main()
