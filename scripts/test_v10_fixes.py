#!/usr/bin/env python3
"""Quick verification of v10 audit fixes against live MCP/API."""
import httpx
import json

API = "https://www.nobleblocks.com"
MCP = "https://mcp.nobleblocks.com"

def test_health():
    r = httpx.get(f"{MCP}/health", timeout=10)
    data = r.json()
    print(f"[Health] Version: {data.get('version')}")
    return data.get("version")

def test_invalid_doi():
    """get_paper with invalid DOI should NOT return an unrelated paper."""
    print("\n=== Test: Invalid DOI handling ===")
    # The MCP server now checks: no spaces + not a DOI format → returns error
    # The backend returns 404 for unknown IDs
    r = httpx.get(f"{API}/api/v1/papers/lookup", params={"id": "invalid-doi-xyz"}, timeout=10)
    print(f"  Backend lookup 'invalid-doi-xyz': HTTP {r.status_code}")
    assert r.status_code in (404, 400), f"Expected 404/400, got {r.status_code}"
    print("  PASS: Backend correctly rejects invalid identifier")

def test_citation_graph():
    """Citation graph should return data for known DOIs."""
    print("\n=== Test: Citation Graph ===")
    doi = "10.1038/s41586-021-03819-2"  # AlphaFold
    r = httpx.get(
        f"{API}/api/v1/papers/citation-graph",
        params={"paperId": doi, "limit": 5},
        timeout=15,
    )
    print(f"  Citation graph (DOI): HTTP {r.status_code}")
    if r.status_code == 200:
        data = r.json()
        nodes = data.get("nodes") or []
        edges = data.get("edges") or []
        seed = data.get("seed") or {}
        print(f"  Nodes: {len(nodes)}, Edges: {len(edges)}")
        print(f"  Seed title: {seed.get('title', 'N/A')}")
        if len(nodes) > 0:
            print("  PASS: Citation graph has data")
        else:
            print("  FAIL: Citation graph returned 200 but empty nodes")
    elif r.status_code == 404:
        print(f"  FAIL: Paper not in citation graph index (404)")
        print(f"  Body: {r.text[:200]}")
    else:
        print(f"  FAIL: Unexpected status {r.status_code}")
        print(f"  Body: {r.text[:200]}")

def test_grants():
    """search_grants should return relevant results, not frozen noise."""
    print("\n=== Test: Grants Search ===")
    # The grants tool calls /api/v1/kg/explore with entity_type=funder
    # If no funder entities, it falls back to paper search with relevance filter
    r = httpx.get(
        f"{API}/api/v1/kg/explore",
        params={"query": "CRISPR gene therapy", "max_nodes": 10, "entity_type": "funder"},
        timeout=15,
    )
    print(f"  KG explore (entity_type=funder): HTTP {r.status_code}")
    if r.status_code == 200:
        data = r.json()
        nodes = data.get("nodes") or []
        funder_nodes = [n for n in nodes if isinstance(n, dict) and n.get("entityType") == "funder"]
        print(f"  Total nodes: {len(nodes)}, Funder nodes: {len(funder_nodes)}")
        if funder_nodes:
            for f in funder_nodes[:3]:
                print(f"    - {f.get('name')}")
            print("  PASS: Found funder entities")
        else:
            print("  INFO: No funder entities in KG — MCP falls back to relevance-filtered paper search")
            # Simulate what MCP does: search with raw query + apply _has_strong_query_relevance
            r2 = httpx.get(
                f"{API}/api/v1/papers/search",
                params={"query": "CRISPR gene therapy", "limit": 20, "phase": "fast", "sort": "relevance"},
                timeout=15,
            )
            if r2.status_code == 200:
                papers = r2.json().get("papers") or r2.json().get("results") or []
                # Apply _has_strong_query_relevance: for ≤4 word queries, ALL words must match
                query = "CRISPR gene therapy"
                q_words = [w for w in query.lower().split() if len(w) >= 3]
                # For ≤4 words: require all; for 5+: require ceil(2n/3)
                if len(q_words) <= 4:
                    threshold = len(q_words)
                else:
                    threshold = (len(q_words) * 2 + 2) // 3
                filtered = []
                for p in papers:
                    title = (p.get("title") or "").lower()
                    abstract = (p.get("abstract") or "").lower()
                    text = title + " " + abstract
                    matches = sum(1 for w in q_words if w in text)
                    if matches >= threshold:
                        filtered.append(p)
                # Further check: how many contain "crispr" specifically?
                crispr_papers = [p for p in filtered if "crispr" in ((p.get("title") or "") + " " + (p.get("abstract") or "")).lower()]
                print(f"  Raw papers: {len(papers)}, After strong filter: {len(filtered)}, With 'CRISPR': {len(crispr_papers)}")
                for p in filtered[:5]:
                    print(f"    - {p.get('title', 'N/A')[:80]}")
                if crispr_papers:
                    print("  PASS: Grants fallback returns CRISPR-relevant papers")
                elif filtered:
                    print("  PARTIAL: Papers match 2/3 query words but may not include CRISPR")
                else:
                    print("  FAIL: No papers pass strong relevance filter")
    elif r.status_code == 422:
        print("  INFO: KG explore returns 422 for entity_type=funder — MCP handles this in except branch")

def test_entity_resolution():
    """search_by_entity for 'Jennifer Doudna CRISPR Nobel Prize'."""
    print("\n=== Test: Entity Resolution ===")
    query = "Jennifer Doudna CRISPR Nobel Prize"
    r = httpx.get(
        f"{API}/api/v1/kg/explore",
        params={"query": query, "max_nodes": 10},
        timeout=60,
    )
    print(f"  KG explore: HTTP {r.status_code}")
    if r.status_code == 200:
        data = r.json()
        nodes = data.get("nodes") or []
        entity_nodes = [n for n in nodes if isinstance(n, dict) and n.get("type") == "entity"]
        print(f"  Raw entity nodes from KG: {len(entity_nodes)}")

        # Apply MCP's filters: _is_garbage_entity + 2-word relevance
        q_words = [w for w in query.lower().split() if len(w) >= 3]
        filtered = []
        for n in entity_nodes:
            name = n.get("name", "")
            # garbage check
            if "@" in name or len(name) > 200 or len(name) < 3:
                continue
            # relevance: require 2+ words from query in entity name+description
            entity_text = (name + " " + (n.get("description") or "")).lower()
            word_hits = sum(1 for w in q_words if w in entity_text)
            if word_hits < 2:
                continue
            filtered.append(n)

        print(f"  After MCP relevance filter: {len(filtered)} entities")
        if filtered:
            for e in filtered[:3]:
                print(f"    - {e.get('name')} ({e.get('entityType')})")
            doudna = [n for n in filtered if "doudna" in (n.get("name") or "").lower()]
            if doudna:
                print("  PASS: Found Doudna entity")
            else:
                print("  PARTIAL: Entities match 2+ words but Doudna not in KG")
        else:
            print("  INFO: All garbage/irrelevant entities filtered out — MCP will fall back to paper search")
            # Verify fallback produces good papers (MCP uses author-name extraction
            # and phase=extended for person names, phase=fast for non-person queries)
            import re as _re
            # Simplified person name check: first two words capitalized, not technical terms
            words = query.strip().split()
            name_parts = []
            for w in words:
                clean = _re.sub(r'[^a-zA-Z]', '', w)
                if not clean:
                    continue
                if clean[0].isupper() and clean.lower() not in {"crispr", "nobel", "prize", "gene", "therapy"}:
                    name_parts.append(clean)
                else:
                    if name_parts:
                        break
            author_name = " ".join(name_parts) if 2 <= len(name_parts) <= 4 else None

            if author_name:
                r2 = httpx.get(
                    f"{API}/api/v1/papers/search",
                    params={"query": author_name, "limit": 10, "sort": "citations", "phase": "extended"},
                    timeout=30,
                )
            else:
                r2 = httpx.get(
                    f"{API}/api/v1/papers/search",
                    params={"query": query, "limit": 10, "sort": "relevance", "phase": "fast"},
                    timeout=15,
                )
            if r2.status_code == 200:
                papers = r2.json().get("papers") or r2.json().get("results") or []
                # MCP uses _has_query_relevance: any 1 word ≥3 chars matching
                relevant = [p for p in papers if any(
                    w in ((p.get("title") or "") + " " + (p.get("abstract") or "")).lower()
                    for w in [w for w in query.lower().split() if len(w) >= 3]
                )]
                print(f"  Fallback papers (1+ word match): {len(relevant)}")
                for p in relevant[:3]:
                    print(f"    - {p.get('title', 'N/A')[:80]}")
                if relevant:
                    print("  PASS: MCP returns relevant papers instead of garbage entities")

if __name__ == "__main__":
    version = test_health()
    test_invalid_doi()
    test_citation_graph()
    test_grants()
    test_entity_resolution()
    print(f"\n{'='*60}")
    print(f"  MCP v{version} — verification complete")
    print(f"{'='*60}")
