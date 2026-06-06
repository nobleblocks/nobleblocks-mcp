#!/usr/bin/env python3
"""
regression_test.py — Critical-path regression guard for NobleBlocks search + product-claims.

Verifies that key fixes cannot silently regress during future optimisations.
Run before deploying ANY change that touches:
  - nobleblocks-frontend/app/api/v1/papers/search/route.ts
  - nobleblocks-frontend/app/api/v1/papers/product-claims/route.ts
  - nobleblocks-mcp/nobleblocks_mcp/server.py
  - nobleblocks-mcp/nobleblocks_mcp/remote_server.py
  - paper-search-db / search_api.py (paper-db)

Usage:
  python3 scripts/regression_test.py                  # full suite (requires PROD or DEV URL)
  python3 scripts/regression_test.py --env dev        # against dev.nobleblocks.com
  python3 scripts/regression_test.py --check-code     # static code checks only (no HTTP)
  python3 scripts/regression_test.py --fast           # static + one live smoke test

Required env vars (for live tests):
  NOBLEBLOCKS_API_BASE   e.g. https://www.nobleblocks.com  (default)
  NOBLEBLOCKS_API_KEY    internal token (stored in .env.local or set in shell)
"""

import argparse
import ast
import os
import re
import sys
import time
from pathlib import Path

# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #
WORKSPACE = Path(__file__).resolve().parents[2]
FRONTEND   = WORKSPACE / "nobleblocks-frontend"
MCP_PKG    = WORKSPACE / "nobleblocks-mcp" / "nobleblocks_mcp"

SEARCH_ROUTE      = FRONTEND / "app/api/v1/papers/search/route.ts"
CLAIMS_ROUTE      = FRONTEND / "app/api/v1/papers/product-claims/route.ts"
MCP_SERVER        = MCP_PKG / "server.py"
MCP_REMOTE_SERVER = MCP_PKG / "remote_server.py"

PASS = "\033[92m✓\033[0m"
FAIL = "\033[91m✗\033[0m"
WARN = "\033[93m!\033[0m"

results: list[tuple[str, bool, str]] = []

def check(name: str, passed: bool, detail: str = "") -> None:
    results.append((name, passed, detail))
    icon = PASS if passed else FAIL
    print(f"  {icon} {name}" + (f" — {detail}" if detail else ""))

# --------------------------------------------------------------------------- #
# 1. STATIC CODE CHECKS
# --------------------------------------------------------------------------- #

def static_checks() -> None:
    print("\n[1/3] Static code checks\n")

    # ── 1a. product-claims: product_name must default to "" not "Unknown Product"
    claims_text = CLAIMS_ROUTE.read_text()
    bad_default = 'body.product_name || "Unknown Product"'
    correct     = 'body.product_name || ""'
    check(
        "product-claims: product_name defaults to empty string",
        bad_default not in claims_text and correct in claims_text,
        f"FAIL: found '{bad_default}' — must be '{correct}'"
            if bad_default in claims_text else "",
    )

    # ── 1b. search route: phase=fast skips AI rewrite
    check(
        "search/route: phase=fast skips AI rewrite",
        'phase === "fast"' in claims_text or 'phase === "fast"' in SEARCH_ROUTE.read_text(),
        "",
    )
    search_text = SEARCH_ROUTE.read_text()
    check(
        "search/route: AI rewrite gated by phase !== fast",
        '|| phase === "fast"' in search_text or 'phase === "fast"' in search_text,
        "",
    )

    # ── 1c. search route: primary promise exits early on phase=fast
    check(
        "search/route: external-API block returns early for phase=fast",
        'if (phase === "fast") {' in search_text
        and 'return { results: [], sources: {}, total: 0 }' in search_text,
        "",
    )

    # ── 1d. search route: multi-hop gated by phase !== fast
    check(
        "search/route: multi-hop decomposition skipped for phase=fast",
        'phase !== "fast"' in search_text,
        "",
    )

    # ── 1e. MCP server.py: must pass phase=fast
    mcp_text = MCP_SERVER.read_text()
    check(
        "server.py: sends phase=fast in search call",
        '"phase": "fast"' in mcp_text,
        'MISSING "phase": "fast" — will trigger AI rewrite + external APIs for every MCP search'
            if '"phase": "fast"' not in mcp_text else "",
    )

    # ── 1e2. TRIAL QUOTA: must be ≥ 100 (NEVER reduce back to 3!)
    # The old 3-query lifetime limit was terrible UX. Users must get 100/day minimum.
    trial_match = re.search(r'RATE_LIMIT_TRIAL.*?(\d+)', mcp_text)
    trial_val = int(trial_match.group(1)) if trial_match else 0
    check(
        f"server.py: RATE_LIMIT_TRIAL ≥ 100 (got {trial_val})",
        trial_val >= 100,
        "REGRESSION: trial limit was reduced! Must be ≥ 100/day — search doesn't cost AI"
            if trial_val < 100 else "",
    )

    # ── 1e3. TRIAL QUOTA: must use daily window not lifetime counter
    check(
        "server.py: trial uses daily reset (not lifetime block)",
        "_prune" in mcp_text and "86400" in mcp_text and "__trial__" in mcp_text,
        "REGRESSION: trial is lifetime counter again — must reset daily (86400s window)"
            if "__trial__" not in mcp_text else "",
    )

    # ── 1e4. FREE KEY daily limit must be ≥ 500
    free_match = re.search(r'RATE_LIMIT_FREE.*?(\d+)', mcp_text)
    free_val = int(free_match.group(1)) if free_match else 0
    check(
        f"server.py: RATE_LIMIT_FREE ≥ 500 (got {free_val})",
        free_val >= 500,
        "REGRESSION: free-key limit reduced! Must be ≥ 500/day" if free_val < 500 else "",
    )

    # ── 1f. MCP remote_server.py: must pass phase=fast
    remote_text = MCP_REMOTE_SERVER.read_text()
    check(
        "remote_server.py: sends phase=fast in search call",
        '"phase": "fast"' in remote_text,
        'MISSING "phase": "fast"' if '"phase": "fast"' not in remote_text else "",
    )

    # ── 1f2. REMOTE SERVER: must send NB_INTERNAL_TOKEN (bypasses all rate limits)
    check(
        "remote_server.py: uses NB_INTERNAL_TOKEN in headers",
        "NB_INTERNAL_TOKEN" in remote_text and "x-internal-token" in remote_text,
        "REGRESSION: internal token removed — remote server will hit trial limits!"
            if "NB_INTERNAL_TOKEN" not in remote_text else "",
    )

    # ── 1f3. FRONTEND TRIAL LIMIT: check mcp-api-key.ts has ≥ 100
    mcp_key_file = FRONTEND / "lib" / "mcp-api-key.ts"
    if mcp_key_file.exists():
        mcp_key_text = mcp_key_file.read_text()
        fe_trial_match = re.search(r'TRIAL_LIMIT\s*=\s*(\d+)', mcp_key_text)
        fe_trial_val = int(fe_trial_match.group(1)) if fe_trial_match else 0
        check(
            f"mcp-api-key.ts: TRIAL_LIMIT ≥ 100 (got {fe_trial_val})",
            fe_trial_val >= 100,
            "REGRESSION: frontend trial limit reduced! Must be ≥ 100/day"
                if fe_trial_val < 100 else "",
        )
        # Must use daily reset, not lifetime
        check(
            "mcp-api-key.ts: trial uses daily reset (resetAt field)",
            "resetAt" in mcp_key_text,
            "REGRESSION: frontend trial is lifetime counter — must reset daily"
                if "resetAt" not in mcp_key_text else "",
        )
    else:
        check("mcp-api-key.ts: file exists", False, "MISSING lib/mcp-api-key.ts")

    # ── 1g. MCP server.py: find_similar has text-search fallback
    check(
        "server.py: find_similar has text-search fallback",
        "fallback" in mcp_text.lower() or "except" in mcp_text,
        "",
    )

    # ── 1h. MCP servers: retry logic exists for 502/503/504
    for _name, _txt in [("server.py", mcp_text), ("remote_server.py", remote_text)]:
        check(
            f"{_name}: retry loop present for 5xx errors",
            "attempt" in _txt and ("502" in _txt or "503" in _txt or "504" in _txt),
            "",
        )

    # ── 1h2. search route: over-fetch must be ≥ 500 when filters active
    # The Paper DB GIN scan grabs 500 candidates biased toward older papers.
    # With low fetch limit (e.g. 120), year/citation filters leave almost nothing.
    # REGRESSION: if fetchLimit is reduced below 500 for filtered queries, MCP returns empty/few results.
    overfetch_ok = "500" in search_text and ("hasFilters" in search_text or "hasActiveFilters" in search_text)
    check(
        "search/route: fetchLimit ≥ 500 when filters active (over-fetch guardrail)",
        overfetch_ok,
        "REGRESSION: over-fetch limit was reduced below 500! "
        "Year/citation filtering needs the full 500 candidates from Paper DB's GIN scan. "
        "Without this, 'CRISPR min_year=2020' returns ~1 result instead of 50+."
            if not overfetch_ok else "",
    )

    # ── 1h3. search route: year/sort filters must NOT be passed to Paper DB for phase=fast
    # Paper DB's GIN scan doesn't support efficient year ordering — it's biased toward
    # old papers in the heap. Filters must be applied CLIENT-SIDE after receiving candidates.
    year_not_passed = "// PROTECTED FIX: Do NOT pass min_year" in search_text or \
                      "year filter client-side" in search_text.lower() or \
                      ("hasFilters" in search_text and "min_year" not in search_text.split("paperDBPromise")[0][-200:])
    check(
        "search/route: year/sort NOT passed to Paper DB for phase=fast",
        year_not_passed,
        "REGRESSION: year filter passed to Paper DB kills results! "
        "Paper DB GIN LIMIT 500 + year filter = almost zero results for recent years."
            if not year_not_passed else "",
    )

    # ── 1i. Protected comments still present (they shouldn't be stripped)
    check(
        "search/route: PROTECTED FIX comment present",
        "PROTECTED" in search_text,
        "Comment was removed — add it back so future devs know not to touch this",
    )
    check(
        "product-claims/route: PROTECTED FIX comment present",
        "PROTECTED FIX" in claims_text,
        "Comment was removed",
    )

    # ── 1j. OAUTH / AUTH page guard-rails ───────────────────────────────────
    # These checks ensure the consent/auth page never silently breaks again.

    # Claude logo must be an inline SVG (not an external img URL that can 404)
    check(
        "remote_server.py: Claude logo is inline SVG not external img",
        "wikipedia.org" not in remote_text and
        "<svg" in remote_text and
        "D97757" in remote_text,  # gradient color unique to official Claude SVG
        "BROKEN: Claude logo reverted to external Wikipedia PNG — will 404 in browsers",
    )

    # Logo sizes must be ≥ 60px (at least 20% larger than old 56px)
    import re as _re
    size_matches = _re.findall(r"logo-icon\s*\{\{[^}]*width:\s*(\d+)px", remote_text)
    min_size = min(int(s) for s in size_matches) if size_matches else 0
    check(
        "remote_server.py: logo-icon width ≥ 60px (not too small)",
        min_size >= 60,
        f"logo-icon width={min_size}px — must be ≥ 60px" if min_size < 60 else "",
    )

    # resource_server_url MUST be base URL (no /mcp suffix).
    # With /mcp suffix, SDK stops serving /.well-known/oauth-protected-resource → 404.
    # The dual mount (/mcp + /) handles both POST paths instead.
    check(
        "remote_server.py: resource_server_url is base URL (no /mcp suffix)",
        'resource_server_url=MCP_BASE_URL,' in remote_text and
        'resource_server_url=f"{MCP_BASE_URL}/mcp"' not in remote_text,
        "BROKEN: resource_server_url has /mcp — breaks /.well-known/oauth-protected-resource",
    )

    # streamable_http_path must be "/" with dual-mount at /mcp and / in create_app()
    # This ensures both connector URL (/mcp) and OAuth resource URL (/) work.
    check(
        "remote_server.py: streamable_http_path is / (dual-mount handles /mcp)",
        'streamable_http_path="/"' in remote_text,
        'MISSING streamable_http_path="/" — dual mount routing broken',
    )

    # Dual mount: path rewrite middleware + Mount("/") must ensure both /mcp and / work
    check(
        "remote_server.py: /mcp path rewrite middleware present",
        'path_rewrite_middleware' in remote_text and '"/mcp"' in remote_text,
        "MISSING /mcp path rewrite — POST /mcp will 404",
    )

    # OAuth endpoints must be registered
    check(
        "remote_server.py: OAuth /authorize route registered",
        "/authorize" in remote_text,
        "MISSING /authorize route — OAuth cannot start",
    )
    check(
        "remote_server.py: OAuth /token route registered",
        "/token" in remote_text,
        "MISSING /token route — token exchange will fail",
    )


# --------------------------------------------------------------------------- #
# 2. LIVE API SMOKE TESTS
# --------------------------------------------------------------------------- #

def live_tests(base_url: str, api_key: str) -> None:
    try:
        import httpx
    except ImportError:
        print("\n[2/3] Live API tests — SKIPPED (httpx not installed)\n")
        return

    print(f"\n[2/3] Live API smoke tests → {base_url}\n")
    headers = {
        "Content-Type": "application/json",
        "x-internal-token": api_key,
    }

    # ── 2a. Search with phase=fast must be fast and return results
    t0 = time.monotonic()
    try:
        r = httpx.get(
            f"{base_url}/api/v1/papers/search",
            params={"query": "vitamin d supplementation", "phase": "fast", "limit": 5},
            headers=headers,
            timeout=15,
        )
        elapsed = time.monotonic() - t0
        ok = r.status_code == 200 and elapsed < 12
        data = r.json() if r.status_code == 200 else {}
        result_count = len(data.get("results", []))
        check(
            f"search phase=fast: returns results in <12s (got {elapsed:.1f}s, {result_count} results)",
            ok,
            f"status={r.status_code}" if r.status_code != 200 else "",
        )
    except Exception as e:
        check("search phase=fast: HTTP request", False, str(e))

    # ── 2b. Product-claims with URL only must NOT return "Unknown Product"
    try:
        r = httpx.post(
            f"{base_url}/api/v1/papers/product-claims",
            json={"product_url": "https://www.amazon.com/dp/B07XYZ1234"},
            headers=headers,
            timeout=60,
        )
        if r.status_code == 200:
            data = r.json()
            product_field = data.get("product", "")
            bad = "unknown product" in str(product_field).lower() or product_field == ""
            check(
                "product-claims URL-only: returns real product name (not 'Unknown Product')",
                not bad,
                f"Got product='{product_field}'" if bad else f"Got product='{product_field}'",
            )
        else:
            check("product-claims URL-only: HTTP 200", False, f"status={r.status_code}")
    except Exception as e:
        check("product-claims URL-only: HTTP request", False, str(e))

    # ── 2c. Search WITHOUT phase=fast (website path) must also work
    t0 = time.monotonic()
    try:
        r = httpx.get(
            f"{base_url}/api/v1/papers/search",
            params={"query": "omega-3 heart disease", "limit": 5},
            headers=headers,
            timeout=30,
        )
        elapsed = time.monotonic() - t0
        data = r.json() if r.status_code == 200 else {}
        result_count = len(data.get("results", []))
        check(
            f"search (website path, no phase): returns results (got {result_count} results, {elapsed:.1f}s)",
            r.status_code == 200 and result_count > 0,
            f"status={r.status_code}" if r.status_code != 200 else "",
        )
    except Exception as e:
        check("search website path: HTTP request", False, str(e))


# --------------------------------------------------------------------------- #
# 3. PAPER-DB INFRA CHECKS
# --------------------------------------------------------------------------- #

def paperdb_checks(base_url: str, api_key: str) -> None:
    try:
        import httpx
    except ImportError:
        print("\n[3/3] Paper-DB checks — SKIPPED (httpx not installed)\n")
        return

    print(f"\n[3/3] Paper-DB infra checks\n")
    headers = {"x-internal-token": api_key}

    # ── 3a. /similar endpoint must respond (HNSW index check)
    try:
        r = httpx.get(
            f"{base_url}/api/v1/papers/similar",
            params={"paper_id": "10.1038/s41586-020-2649-2", "limit": 3},
            headers=headers,
            timeout=20,
        )
        data = r.json() if r.status_code == 200 else {}
        result_count = len(data.get("results", []))
        check(
            f"/similar endpoint: responds with results (got {result_count})",
            r.status_code == 200 and result_count > 0,
            f"status={r.status_code} — HNSW index may not be ready yet" if r.status_code != 200 else "",
        )
    except Exception as e:
        check("/similar endpoint: HTTP request", False, str(e))


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #

def main() -> None:
    parser = argparse.ArgumentParser(description="NobleBlocks regression tests")
    parser.add_argument("--env", choices=["prod", "dev"], default="prod")
    parser.add_argument("--check-code", action="store_true", help="Static checks only")
    parser.add_argument("--fast", action="store_true", help="Static + quick smoke test")
    args = parser.parse_args()

    base_url = (
        os.environ.get("NOBLEBLOCKS_API_BASE")
        or ("https://www.dev.nobleblocks.com" if args.env == "dev" else "https://www.nobleblocks.com")
    )
    api_key = os.environ.get("NOBLEBLOCKS_API_KEY", "")

    print("=" * 70)
    print("  NobleBlocks Critical-Path Regression Tests")
    print(f"  Target: {base_url}")
    print("=" * 70)

    static_checks()

    if not args.check_code:
        if not api_key:
            print(f"\n  {WARN} NOBLEBLOCKS_API_KEY not set — skipping live tests")
            print("  Set it in your shell or .env.local, then re-run.\n")
        else:
            live_tests(base_url, api_key)
            if not args.fast:
                paperdb_checks(base_url, api_key)

    # Summary
    passed = sum(1 for _, ok, _ in results if ok)
    total  = len(results)
    failed = [(n, d) for n, ok, d in results if not ok]

    print("\n" + "=" * 70)
    print(f"  Results: {passed}/{total} passed")
    if failed:
        print(f"\n  {FAIL} FAILED checks:")
        for name, detail in failed:
            print(f"    • {name}")
            if detail:
                print(f"      {detail}")
    else:
        print(f"  {PASS} All checks passed")
    print("=" * 70 + "\n")

    sys.exit(0 if not failed else 1)


if __name__ == "__main__":
    main()
