"""
NobleBlocks Remote MCP Server — Streamable HTTP + OAuth 2.1
============================================================

This is the server that gets deployed at https://mcp.nobleblocks.com
for listing in the Claude Connectors Directory.

Users connect by clicking "Connect" in Claude → redirected to NobleBlocks
login → authenticated → Claude can search 340M+ papers on behalf of the user.

Run locally:  python -m nobleblocks_mcp.remote_server
Deploy:       Docker → ECS (see Dockerfile.remote)
"""

from __future__ import annotations

import os
import re
import json
import time
import asyncio
import logging
from typing import Any
from contextlib import asynccontextmanager

import httpx
from dotenv import load_dotenv
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import HTMLResponse, RedirectResponse, JSONResponse
from starlette.routing import Route, Mount

from mcp.server.fastmcp import FastMCP
from mcp.server.auth.provider import AuthorizationParams
from mcp.server.auth.settings import AuthSettings, ClientRegistrationOptions
from mcp.types import Icon
from nobleblocks_mcp.oauth_provider import NobleBlocksOAuthProvider

load_dotenv()

logger = logging.getLogger("nobleblocks-mcp")
logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))

# ─── Configuration ─────────────────────────────────────────────────────────────
NB_API_BASE = os.environ.get("NOBLEBLOCKS_API_BASE", "https://www.nobleblocks.com").rstrip("/")
MCP_BASE_URL = os.environ.get("MCP_BASE_URL", "https://mcp.nobleblocks.com").rstrip("/")
NB_INTERNAL_TOKEN = os.environ.get("NB_INTERNAL_TOKEN", "")
HOST = os.environ.get("MCP_HOST", "0.0.0.0")
PORT = int(os.environ.get("MCP_PORT", "8080"))
SERVER_VERSION = os.environ.get("MCP_VERSION", "2.0.0")

HTTP_TIMEOUT = 30.0
MAX_QUERY_LENGTH = 500
MAX_PAPER_ID_LENGTH = 100
DANGEROUS_PATTERNS = re.compile(
    r"(<script|javascript:|on\w+=|eval\(|exec\(|import\(|require\(|__proto__|constructor\[)",
    re.IGNORECASE,
)

# ─── OAuth Provider ───────────────────────────────────────────────────────────
oauth_provider = NobleBlocksOAuthProvider()

# ─── FastMCP Server ───────────────────────────────────────────────────────────
mcp = FastMCP(
    name="NobleBlocks",
    instructions=(
        "NobleBlocks is the research MCP for academic discovery. "
        "Connect directly to 340M+ peer-reviewed papers from PubMed, arXiv, "
        "Crossref, Semantic Scholar, and dozens of other sources — plus a "
        "knowledge graph with 1.3M+ entities (genes, drugs, diseases, "
        "institutions, and concepts) and 109M+ paper connections.\n\n"
        "Use search_papers to find papers on any research question. "
        "Use find_similar to discover semantically related work via vector embeddings. "
        "Use get_paper to fetch full metadata for a specific paper by DOI, PMID, or arXiv ID. "
        "Use get_citation_graph to explore the citation network of a paper. "
        "Use search_by_entity to explore connections between genes, drugs, diseases, "
        "and institutions in the knowledge graph. "
        "Use search_grants to find funding sources and grant-funded research."
    ),
    icons=[Icon(src=f"{MCP_BASE_URL}/icon.png")],
    website_url="https://www.nobleblocks.com",
    auth_server_provider=oauth_provider,
    auth=AuthSettings(
        issuer_url=MCP_BASE_URL,
        # PROTECTED: resource_server_url MUST be base URL (no /mcp suffix).
        # With /mcp suffix, SDK stops serving /.well-known/oauth-protected-resource at root → 404.
        # Dual mount at /mcp and / ensures POST to both paths reaches the handler.
        resource_server_url=MCP_BASE_URL,
        client_registration_options=ClientRegistrationOptions(
            enabled=True,
            valid_scopes=["search", "review", "graph"],
            default_scopes=["search"],
        ),
    ),
    host=HOST,
    port=PORT,
    # PROTECTED: streamable_http_path="/" means handler lives at sub-app root.
    # With dual Mount("/mcp") + Mount("/"), both POST /mcp and POST / reach it.
    streamable_http_path="/",
)


# ─── Security ─────────────────────────────────────────────────────────────────
def sanitize_input(value: str, max_length: int = MAX_QUERY_LENGTH) -> str:
    if not isinstance(value, str):
        return ""
    value = value[:max_length].strip().replace("\x00", "")
    if DANGEROUS_PATTERNS.search(value):
        raise ValueError("Input contains potentially malicious content")
    return value


def _headers(api_key: str = "") -> dict[str, str]:
    h = {"User-Agent": "nobleblocks-mcp/2.0.0", "Accept": "application/json"}
    if NB_INTERNAL_TOKEN:
        h["x-internal-token"] = NB_INTERNAL_TOKEN
    if api_key:
        h["Authorization"] = f"Bearer {api_key}"
    return h


async def _api_get(path: str, params: dict[str, Any], api_key: str = "") -> dict:
    url = f"{NB_API_BASE}{path}"
    # Retry on 502/503/504 (connection saturation / paper-db overload)
    last_exc: Exception | None = None
    for attempt in range(3):
        try:
            async with httpx.AsyncClient(timeout=HTTP_TIMEOUT, headers=_headers(api_key)) as client:
                resp = await client.get(url, params={k: v for k, v in params.items() if v is not None})
                if resp.status_code in (502, 503, 504) and attempt < 2:
                    await asyncio.sleep(1.0 * (attempt + 1))
                    continue
                resp.raise_for_status()
                return resp.json()
        except (httpx.TimeoutException, httpx.ConnectError) as e:
            last_exc = e
            if attempt < 2:
                await asyncio.sleep(1.5 * (attempt + 1))
                continue
            raise
    raise last_exc or RuntimeError("Request failed after retries")


async def _api_post(path: str, body: dict[str, Any], api_key: str = "") -> dict:
    url = f"{NB_API_BASE}{path}"
    async with httpx.AsyncClient(timeout=60.0, headers=_headers(api_key)) as client:
        resp = await client.post(url, json=body)
        resp.raise_for_status()
        return resp.json()


# ─── Paper formatting ─────────────────────────────────────────────────────────
def _compact_paper(p: dict, include_full: bool = False) -> dict:
    if not isinstance(p, dict):
        return {}
    abstract = p.get("abstract") or ""
    if len(abstract) > 600:
        abstract = abstract[:600] + "..."
    out: dict[str, Any] = {
        "id": p.get("paperId") or p.get("id") or p.get("paper_id"),
        "title": p.get("title"),
        "authors": [
            a.get("name") if isinstance(a, dict) else str(a)
            for a in (p.get("authors") or [])[:8]
        ],
        "year": p.get("year") or _extract_year(p.get("publicationDate") or p.get("publication_date")),
        "doi": p.get("doi") or p.get("DOI") or (p.get("externalIds") or {}).get("DOI"),
        "citations": p.get("citationCount") or p.get("citation_count") or 0,
        "venue": p.get("venue") or p.get("source") or (p.get("journal") or {}).get("name"),
        "abstract": abstract,
        "url": _build_url(p),
    }
    if include_full:
        out["references_count"] = p.get("referenceCount") or 0
        out["open_access"] = p.get("isOpenAccess", False)
        out["pdf_url"] = (p.get("openAccessPdf") or {}).get("url")
    return out


def _build_url(p: dict) -> str | None:
    doi = p.get("doi") or p.get("DOI") or (p.get("externalIds") or {}).get("DOI")
    return f"https://doi.org/{doi}" if doi else p.get("url")


def _extract_year(date_str: str | None) -> int | None:
    if not date_str:
        return None
    try:
        return int(str(date_str)[:4])
    except (ValueError, TypeError):
        return None


# ─── Tool Definitions ─────────────────────────────────────────────────────────
# Tool annotations for Claude Directory compliance

@mcp.tool(
    annotations={
        "title": "Search Papers",
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": False,
    }
)
async def search_papers(
    query: str,
    limit: int = 10,
    min_year: int | None = None,
    max_year: int | None = None,
    min_citations: int | None = None,
    source: str | None = None,
    sort: str = "relevance",
) -> str:
    """Search 340M+ academic papers from PubMed, arXiv, Crossref, and dozens of other sources. Returns ranked results with title, authors, year, abstract, citations, and DOI."""
    query = sanitize_input(query)
    if len(query) < 2:
        return json.dumps({"error": "Query must be at least 2 characters"})

    data = await _api_get(
        "/api/v1/papers/search",
        {
            "query": query,
            "limit": min(limit, 50),
            # PROTECTED: phase=fast MUST stay. Removing this triggers AI rewrites
            # + external API calls (S2/OpenAlex/CrossRef) for every MCP search,
            # burning LLM budget and causing latency spikes site-wide.
            # Verified by: python3 nobleblocks-mcp/scripts/regression_test.py
            "phase": "fast",
            "min_year": min_year,
            "max_year": max_year,
            "min_citations": min_citations,
            "source": source,
            "sort": sort,
        },
    )
    papers = data.get("papers") or data.get("results") or []
    result: dict[str, Any] = {
        "query": query,
        "total": data.get("total", len(papers)),
        "results": [_compact_paper(p) for p in papers[:min(limit, 50)]],
        "attribution": "Powered by NobleBlocks (nobleblocks.com) — 300M+ papers across 6 academic databases",
    }
    # Surface spelling correction so AI clients can suggest "Did you mean X?"
    corrected = data.get("correctedQuery")
    if corrected and corrected.lower() != query.lower():
        result["did_you_mean"] = corrected
    return json.dumps(result, indent=2, default=str)


@mcp.tool(
    annotations={
        "title": "Get Paper Details",
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": False,
    }
)
async def get_paper(paper_id: str) -> str:
    """Fetch full metadata for a single paper by DOI, PMID, arXiv ID, OpenAlex ID, or NobleBlocks ID."""
    paper_id = sanitize_input(paper_id, MAX_PAPER_ID_LENGTH)
    if not paper_id:
        return json.dumps({"error": "paper_id is required"})

    data = await _api_get("/api/v1/papers/lookup", {"id": paper_id})
    paper = data.get("paper") or data
    result = {
        **_compact_paper(paper, include_full=True),
        "attribution": "Powered by NobleBlocks (nobleblocks.com)",
    }
    return json.dumps(result, indent=2, default=str)


@mcp.tool(
    annotations={
        "title": "Find Similar Papers",
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": False,
    }
)
async def find_similar(query: str, limit: int = 10) -> str:
    """Find semantically similar papers using vector embeddings. Provide a paper title, abstract, or research question."""
    query = sanitize_input(query)
    if len(query) < 5:
        return json.dumps({"error": "Query must be at least 5 characters"})

    try:
        data = await _api_get(
            "/api/v1/papers/similar",
            {"query": query, "limit": min(limit, 30)},
        )
        papers = data.get("papers") or data.get("results") or []
        result = {
            "query": query,
            "results": [_compact_paper(p) for p in papers],
            "attribution": "Powered by NobleBlocks semantic search (nobleblocks.com)",
        }
        return json.dumps(result, indent=2, default=str)
    except Exception:
        # Fallback: use text search sorted by relevance when vector search unavailable
        data = await _api_get(
            "/api/v1/papers/search",
            {"query": query, "limit": min(limit, 30), "sort": "relevance"},
        )
        papers = data.get("papers") or data.get("results") or []
        result = {
            "query": query,
            "results": [_compact_paper(p) for p in papers],
            "note": "Used relevance-ranked text search (vector search temporarily unavailable)",
            "attribution": "Powered by NobleBlocks (nobleblocks.com)",
        }
        return json.dumps(result, indent=2, default=str)


@mcp.tool(
    annotations={
        "title": "Citation Graph",
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": False,
    }
)
async def get_citation_graph(
    paper_id: str,
    direction: str = "both",
    limit: int = 20,
) -> str:
    """Get the citation network for a paper — references and citing papers."""
    paper_id = sanitize_input(paper_id, MAX_PAPER_ID_LENGTH)
    if not paper_id:
        return json.dumps({"error": "paper_id is required"})

    data = await _api_get(
        "/api/v1/papers/citation-graph",
        {"paperId": paper_id, "limit": min(limit, 50)},
    )
    result: dict[str, Any] = {"paper_id": paper_id}
    if direction in ("references", "both"):
        result["references"] = [_compact_paper(p) for p in (data.get("references") or [])[:limit]]
    if direction in ("citations", "both"):
        result["citations"] = [_compact_paper(p) for p in (data.get("citations") or [])[:limit]]
    result["attribution"] = "Powered by NobleBlocks (nobleblocks.com)"
    return json.dumps(result, indent=2, default=str)


@mcp.tool(
    annotations={
        "title": "Search Knowledge Graph",
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": False,
    }
)
async def search_by_entity(
    query: str,
    max_nodes: int = 20,
) -> str:
    """Explore the NobleBlocks knowledge graph — find connections between genes,
    drugs, diseases, institutions, and concepts. Discover which papers link
    entities together across 1.3M+ entities and 109M+ paper connections."""
    query = sanitize_input(query, MAX_QUERY_LENGTH)
    if len(query) < 2:
        return json.dumps({"error": "Query must be at least 2 characters"})

    max_nodes = max(10, min(max_nodes, 50))
    data = await _api_get("/api/v1/kg/explore", {"query": query, "max_nodes": max_nodes})

    nodes = data.get("nodes") or []
    edges = data.get("edges") or []
    entities = []
    papers = []
    for node in nodes:
        if node.get("type") == "entity":
            entities.append({
                "name": node.get("name"),
                "entity_type": node.get("entityType"),
                "description": node.get("description"),
            })
        elif node.get("type") == "paper":
            papers.append(_compact_paper(node))

    result = {
        "query": query,
        "entities_found": len(entities),
        "papers_found": len(papers),
        "entities": entities[:20],
        "papers": papers[:20],
        "relationships": len(edges),
        "attribution": "NobleBlocks Knowledge Graph (nobleblocks.com) — 1.3M+ entities, 109M+ links",
    }
    return json.dumps(result, indent=2, default=str)


@mcp.tool(
    annotations={
        "title": "Search Grants & Funding",
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": False,
    }
)
async def search_grants(
    query: str,
    limit: int = 20,
) -> str:
    """Search for research grants and funding sources. Find which organizations
    fund research on a given topic, or discover what research a specific funder
    (e.g. NIH, Gates Foundation, ERC, Wellcome Trust) has supported.
    Returns funders, their funded papers, and award IDs."""
    query = sanitize_input(query, MAX_QUERY_LENGTH)
    if len(query) < 2:
        return json.dumps({"error": "Query must be at least 2 characters"})

    limit = max(1, min(limit, 50))
    data = await _api_get("/api/v1/kg/explore", {"query": query, "max_nodes": limit})

    nodes = data.get("nodes") or []
    edges = data.get("edges") or []

    funders = []
    funded_papers = []
    funder_paper_links: dict[str, list[str]] = {}

    for node in nodes:
        if node.get("type") == "entity" and node.get("entityType") == "funder":
            funder_info = {
                "name": node.get("name"),
                "country": (node.get("metadata") or {}).get("country", ""),
                "description": node.get("description"),
            }
            funders.append(funder_info)
            funder_paper_links[node.get("uid", "")] = []
        elif node.get("type") == "paper":
            funded_papers.append(_compact_paper(node))

    # Map FUNDED edges
    node_map = {n.get("uid"): n for n in nodes}
    for edge in edges:
        if edge.get("label") == "FUNDED":
            src = edge.get("source", "")
            tgt = edge.get("target", "")
            paper_node = node_map.get(tgt) or node_map.get(src)
            funder_uid = src if src in funder_paper_links else tgt
            if funder_uid in funder_paper_links and paper_node:
                title = paper_node.get("title") or paper_node.get("name", "")
                if title:
                    funder_paper_links[funder_uid].append(title)

    for funder in funders:
        uid_candidates = [n.get("uid") for n in nodes
                          if n.get("name") == funder["name"] and n.get("entityType") == "funder"]
        if uid_candidates:
            funder["funded_papers"] = funder_paper_links.get(uid_candidates[0], [])

    if not funders and funded_papers:
        result = {
            "query": query,
            "funders_found": 0,
            "papers_with_funding": len(funded_papers),
            "papers": funded_papers[:limit],
            "note": "No specific funders found. Try a funder name (e.g. 'NIH', 'ERC', 'Gates Foundation').",
            "attribution": "NobleBlocks Knowledge Graph (nobleblocks.com)",
        }
        return json.dumps(result, indent=2, default=str)

    result = {
        "query": query,
        "funders_found": len(funders),
        "funded_papers_found": len(funded_papers),
        "funders": funders[:limit],
        "funded_papers": funded_papers[:limit],
        "attribution": "NobleBlocks Knowledge Graph (nobleblocks.com) — Grants & Funding data",
    }
    return json.dumps(result, indent=2, default=str)


# ─── Consent Page ──────────────────────────────────────────────────────────────
GOOGLE_CLIENT_ID = "351535713791-lkhg858q5637b05no5f2pu8hp47470rm.apps.googleusercontent.com"

CONSENT_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Connect NobleBlocks to {client_name}</title>
  <link rel="icon" type="image/x-icon" href="https://mcp.nobleblocks.com/favicon.ico">
  <link rel="apple-touch-icon" href="https://mcp.nobleblocks.com/icon.png">
  <script src="https://accounts.google.com/gsi/client" async defer></script>
  <style>
    * {{ margin: 0; padding: 0; box-sizing: border-box; }}
    body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
           background: #f8fafc; display: flex; justify-content: center; align-items: center;
           min-height: 100vh; padding: 20px; }}
    .card {{ background: white; border-radius: 16px; box-shadow: 0 4px 24px rgba(0,0,0,0.08);
             max-width: 440px; width: 100%; padding: 40px; text-align: center; }}
    .connect-logos {{ display: flex; align-items: center; justify-content: center;
                     gap: 16px; margin-bottom: 20px; }}
    .connect-logos .logo-icon {{ width: 68px; height: 68px; border-radius: 14px;
                                 display: flex; align-items: center; justify-content: center;
                                 overflow: hidden; }}
    .connect-logos .logo-icon img {{ width: 100%; height: 100%; object-fit: contain; transform: scale(1.7); transform-origin: center center; }}
    .connect-logos .logo-icon svg {{ width: 68px; height: 68px; display: block; }}
    .connect-logos .arrows {{ display: flex; align-items: center; gap: 2px; color: #94a3b8; }}
    .connect-logos .arrows svg {{ width: 20px; height: 20px; }}
    h1 {{ font-size: 22px; color: #1a1a2e; margin-bottom: 8px; }}
    .subtitle {{ color: #64748b; font-size: 14px; margin-bottom: 24px; line-height: 1.5; }}
    .scope {{ background: #f1f5f9; border-radius: 8px; padding: 16px; text-align: left;
              margin-bottom: 24px; }}
    .scope h3 {{ font-size: 13px; color: #475569; text-transform: uppercase; margin-bottom: 8px; }}
    .scope li {{ font-size: 14px; color: #334155; margin: 4px 0; list-style: none;
                 padding-left: 20px; position: relative; }}
    .scope li::before {{ content: "\\2713"; position: absolute; left: 0; color: #22c55e; }}
    .divider {{ display: flex; align-items: center; margin: 20px 0; color: #94a3b8; font-size: 13px; }}
    .divider::before, .divider::after {{ content: ""; flex: 1; border-bottom: 1px solid #e2e8f0; }}
    .divider::before {{ margin-right: 12px; }}
    .divider::after {{ margin-left: 12px; }}
    .btn-google {{ display: flex; align-items: center; justify-content: center; width: 100%;
                   padding: 12px; background: #e8eaed; color: #1f2937; border: 1px solid #c8cacf;
                   border-radius: 8px; font-size: 15px; font-weight: 500; cursor: pointer;
                   transition: background 0.2s, box-shadow 0.2s; gap: 10px; }}
    .btn-google:hover {{ background: #dadce0; box-shadow: 0 1px 3px rgba(0,0,0,0.15); }}
    .btn-google:disabled {{ cursor: not-allowed; }}
    .btn-google.connecting {{ background: #4f46e5; color: white; border-color: #4338ca;
                              animation: pulse-btn 1.4s ease-in-out infinite; }}
    .btn-google svg {{ width: 20px; height: 20px; flex-shrink: 0; }}
    .login-section {{ margin-bottom: 16px; }}
    .login-section label {{ display: block; text-align: left; font-size: 13px; color: #475569;
                            margin-bottom: 4px; }}
    .login-section input {{ width: 100%; padding: 10px 14px; border: 1px solid #e2e8f0;
                            border-radius: 8px; font-size: 14px; outline: none; }}
    .login-section input:focus {{ border-color: #6366f1; box-shadow: 0 0 0 3px rgba(99,102,241,0.1); }}
    .error {{ color: #ef4444; font-size: 13px; margin-bottom: 12px; display: none; }}
    .btn {{ display: inline-block; width: 100%; padding: 12px; background: #6366f1; color: white;
            border: none; border-radius: 8px; font-size: 15px; font-weight: 600; cursor: pointer;
            transition: background 0.2s; }}
    .btn:hover {{ background: #4f46e5; }}
    .btn:disabled {{ background: #94a3b8; cursor: not-allowed; }}
    .btn-cancel {{ background: transparent; color: #64748b; border: 1px solid #e2e8f0;
                   margin-top: 8px; }}
    .btn-cancel:hover {{ background: #f8fafc; }}
    @keyframes pulse-btn {{ 0%, 100% {{ opacity: 1; }} 50% {{ opacity: 0.75; }} }}
    .btn.connecting {{ background: #4338ca; animation: pulse-btn 1.4s ease-in-out infinite; }}
    .footer {{ font-size: 12px; color: #94a3b8; margin-top: 16px; }}
    .footer a {{ color: #6366f1; text-decoration: none; }}
  </style>
</head>
<body>
  <div class="card">
    <div class="connect-logos">
      <div class="logo-icon">
        <img src="https://mcp.nobleblocks.com/icon.png" alt="NobleBlocks">
      </div>
      <div class="arrows">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M5 12h14M12 5l7 7-7 7"/></svg>
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M19 12H5M12 19l-7-7 7-7"/></svg>
      </div>
      <div class="logo-icon">
        <svg width="56" height="56" viewBox="0 0 72 72" fill="none" xmlns="http://www.w3.org/2000/svg"><rect width="72" height="72" rx="18" fill="url(#claude_grad)"></rect><path d="M20.1993 44.6036L30.4423 38.8598L30.6111 38.3567L30.4423 38.0766H29.9358L28.2192 37.9722L22.3661 37.8155L17.3008 37.6066L12.3763 37.3456L11.1381 37.0845L9.98438 35.5441L10.0969 34.787L11.1381 34.0821L12.6296 34.2126L15.922 34.4476L20.8746 34.787L24.4484 34.9958L29.7669 35.5441H30.6111L30.7237 35.2047L30.4423 34.9958L30.2172 34.787L25.0957 31.3146L19.552 27.6595L16.6536 25.5447L15.1059 24.4743L14.318 23.4822L13.9803 21.2891L15.3873 19.7227L17.3008 19.8532L17.7792 19.9837L19.7209 21.4719L23.8575 24.6832L29.2604 28.6516L30.0483 29.3043L30.3656 29.0902L30.4142 28.9388L30.0483 28.3383L27.1218 23.0384L23.9982 17.634L22.5912 15.3887L22.2254 14.0572C22.0837 13.4984 22.0002 13.0362 22.0002 12.4646L23.6042 10.2716L24.5047 9.98438L26.6715 10.2716L27.572 11.0548L28.9227 14.1355L31.0895 18.9655L34.4664 25.5447L35.4513 27.5028L35.9859 29.3043L36.1829 29.8526H36.5206V29.5393L36.802 25.8319L37.3085 21.2891L37.815 15.4409L37.9839 13.7961L38.7999 11.8119L40.4321 10.7415L41.6984 11.342L42.7396 12.8301L42.5989 13.7961L41.9798 17.8168L40.7698 24.1088L39.9818 28.3383H40.4321L40.9667 27.79L43.1054 24.9704L46.6792 20.4798L48.255 18.7044L50.1123 16.7463L51.2942 15.8065H53.5454L55.1775 18.2606L54.4459 20.7931L52.1384 23.7172L50.2249 26.1974L47.4812 29.8734L45.7787 32.8289L45.9314 33.0727L46.3415 33.0377L52.5324 31.7062L55.881 31.1058L59.877 30.4269L61.6779 31.2624L61.8749 32.124L61.1714 33.8732L56.8941 34.9175L51.8851 35.9357L44.4262 37.6916L44.3436 37.7581L44.4411 37.9027L47.8048 38.2071L49.24 38.2855H52.7575L59.3141 38.7815L61.0307 39.9042L62.0437 41.2879L61.8749 42.3583L59.2297 43.6898L55.6841 42.8544L47.3827 40.8701L44.5405 40.1652H44.1466V40.4002L46.5104 42.7238L50.8721 46.64L56.3031 51.705L56.5845 52.9582L55.881 53.9503L55.1494 53.8458L50.3656 50.2429L48.5083 48.6242L44.3436 45.0996H44.0622V45.4652L45.0189 46.875L50.1123 54.5246L50.3656 56.8744L49.9997 57.6315L48.6772 58.1014L47.242 57.8404L44.231 53.637L41.1637 48.9375L38.6874 44.708L38.3883 44.8969L36.9145 60.6339L36.2392 61.4433L34.6633 62.0437L33.3407 61.0516L32.6372 59.4329L33.3407 56.2217L34.1849 52.0444L34.8603 48.7287L35.4794 44.6036L35.8575 43.2255L35.8241 43.1334L35.522 43.1841L32.4121 47.4494L27.6846 53.8458L23.9419 57.8404L23.0414 58.2059L21.4937 57.3965L21.6344 55.9606L22.5068 54.6813L27.6846 48.1021L30.8081 44.0031L32.8213 41.6502L32.8017 41.3099L32.6905 41.3004L18.933 50.269L16.4848 50.5823L15.4154 49.5902L15.5561 47.9715L16.0627 47.4494L20.1993 44.6036Z" fill="#FAF9F5"></path><defs><linearGradient id="claude_grad" x1="36" y1="0" x2="36" y2="72" gradientUnits="userSpaceOnUse"><stop stop-color="#D97757"></stop><stop offset="1" stop-color="#DB6843"></stop></linearGradient></defs></svg>
      </div>
    </div>
    <h1>Connect NobleBlocks to {client_name}</h1>
    <p class="subtitle">Search 340M+ academic papers directly from {client_name}</p>

    <div class="scope">
      <h3>This will allow {client_name} to:</h3>
      <ul>
        <li>Search academic papers on your behalf</li>
        <li>Find similar papers using semantic search</li>
        <li>View citation graphs and paper metadata</li>
      </ul>
    </div>

    <button type="button" class="btn-google" id="googleBtn" onclick="handleGoogleLogin()">
      <svg viewBox="0 0 24 24"><path fill="#4285F4" d="M22.56 12.25c0-.78-.07-1.53-.2-2.25H12v4.26h5.92c-.26 1.37-1.04 2.53-2.21 3.31v2.77h3.57c2.08-1.92 3.28-4.74 3.28-8.09z"/><path fill="#34A853" d="M12 23c2.97 0 5.46-.98 7.28-2.66l-3.57-2.77c-.98.66-2.23 1.06-3.71 1.06-2.86 0-5.29-1.93-6.16-4.53H2.18v2.84C3.99 20.53 7.7 23 12 23z"/><path fill="#FBBC05" d="M5.84 14.09c-.22-.66-.35-1.36-.35-2.09s.13-1.43.35-2.09V7.07H2.18C1.43 8.55 1 10.22 1 12s.43 3.45 1.18 4.93l2.85-2.22.81-.62z"/><path fill="#EA4335" d="M12 5.38c1.62 0 3.06.56 4.21 1.64l3.15-3.15C17.45 2.09 14.97 1 12 1 7.7 1 3.99 3.47 2.18 7.07l3.66 2.84c.87-2.6 3.3-4.53 6.16-4.53z"/></svg>
      Continue with Google
    </button>

    <div class="divider">or sign in with email</div>

    <form id="loginForm" onsubmit="return handleLogin(event)">
      <div class="login-section">
        <label for="email">NobleBlocks Email</label>
        <input type="email" id="email" name="email" required placeholder="your@email.com">
      </div>
      <div class="login-section">
        <label for="password">Password</label>
        <input type="password" id="password" name="password" required placeholder="Your password">
      </div>
      <p class="error" id="errorMsg"></p>
      <button type="submit" class="btn" id="connectBtn">Connect</button>
      <button type="button" class="btn btn-cancel" onclick="window.close()">Cancel</button>
    </form>

    <p class="footer">
      By connecting, you agree to the <a href="https://www.nobleblocks.com/terms">Terms</a>
      and <a href="https://www.nobleblocks.com/privacy">Privacy Policy</a>.
      <br>You can revoke access any time in your account settings.
    </p>
  </div>

  <script>
    const AUTH_STATE = '{auth_state}';
    const GOOGLE_CLIENT_ID = '{google_client_id}';
    let tokenClient = null;

    function initGoogleClient() {{
      if (window.google && google.accounts && google.accounts.oauth2) {{
        tokenClient = google.accounts.oauth2.initTokenClient({{
          client_id: GOOGLE_CLIENT_ID,
          scope: 'openid profile email',
          callback: handleGoogleCallback,
        }});
      }} else {{
        setTimeout(initGoogleClient, 200);
      }}
    }}
    initGoogleClient();

    function handleGoogleLogin() {{
      const btn = document.getElementById('googleBtn');
      const errEl = document.getElementById('errorMsg');
      errEl.style.display = 'none';
      if (tokenClient) {{
        tokenClient.requestAccessToken();
      }} else {{
        errEl.textContent = 'Google sign-in is loading. Please try again.';
        errEl.style.display = 'block';
      }}
    }}

    async function handleGoogleCallback(response) {{
      if (response.error || !response.access_token) {{
        const errEl = document.getElementById('errorMsg');
        errEl.textContent = response.error_description || 'Google authentication failed.';
        errEl.style.display = 'block';
        return;
      }}
      const btn = document.getElementById('googleBtn');
      btn.disabled = true;
      btn.classList.add('connecting');
      btn.innerHTML = '<span>Connecting...</span>';
      try {{
        const resp = await fetch('/oauth/login/google', {{
          method: 'POST',
          headers: {{ 'Content-Type': 'application/json' }},
          body: JSON.stringify({{
            access_token: response.access_token,
            auth_state: AUTH_STATE,
          }}),
        }});
        const data = await resp.json();
        if (data.redirect_url) {{
          window.location.href = data.redirect_url;
        }} else {{
          const errEl = document.getElementById('errorMsg');
          errEl.textContent = data.error || 'Login failed.';
          errEl.style.display = 'block';
          btn.disabled = false;
          btn.classList.remove('connecting');
          btn.innerHTML = '<svg viewBox="0 0 24 24" width="20" height="20"><path fill="#4285F4" d="M22.56 12.25c0-.78-.07-1.53-.2-2.25H12v4.26h5.92c-.26 1.37-1.04 2.53-2.21 3.31v2.77h3.57c2.08-1.92 3.28-4.74 3.28-8.09z"/><path fill="#34A853" d="M12 23c2.97 0 5.46-.98 7.28-2.66l-3.57-2.77c-.98.66-2.23 1.06-3.71 1.06-2.86 0-5.29-1.93-6.16-4.53H2.18v2.84C3.99 20.53 7.7 23 12 23z"/><path fill="#FBBC05" d="M5.84 14.09c-.22-.66-.35-1.36-.35-2.09s.13-1.43.35-2.09V7.07H2.18C1.43 8.55 1 10.22 1 12s.43 3.45 1.18 4.93l2.85-2.22.81-.62z"/><path fill="#EA4335" d="M12 5.38c1.62 0 3.06.56 4.21 1.64l3.15-3.15C17.45 2.09 14.97 1 12 1 7.7 1 3.99 3.47 2.18 7.07l3.66 2.84c.87-2.6 3.3-4.53 6.16-4.53z"/></svg> Continue with Google';
        }}
      }} catch (err) {{
        const errEl = document.getElementById('errorMsg');
        errEl.textContent = 'Connection error. Please try again.';
        errEl.style.display = 'block';
        btn.disabled = false;
        btn.classList.remove('connecting');
        btn.innerHTML = '<svg viewBox="0 0 24 24" width="20" height="20"><path fill="#4285F4" d="M22.56 12.25c0-.78-.07-1.53-.2-2.25H12v4.26h5.92c-.26 1.37-1.04 2.53-2.21 3.31v2.77h3.57c2.08-1.92 3.28-4.74 3.28-8.09z"/><path fill="#34A853" d="M12 23c2.97 0 5.46-.98 7.28-2.66l-3.57-2.77c-.98.66-2.23 1.06-3.71 1.06-2.86 0-5.29-1.93-6.16-4.53H2.18v2.84C3.99 20.53 7.7 23 12 23z"/><path fill="#FBBC05" d="M5.84 14.09c-.22-.66-.35-1.36-.35-2.09s.13-1.43.35-2.09V7.07H2.18C1.43 8.55 1 10.22 1 12s.43 3.45 1.18 4.93l2.85-2.22.81-.62z"/><path fill="#EA4335" d="M12 5.38c1.62 0 3.06.56 4.21 1.64l3.15-3.15C17.45 2.09 14.97 1 12 1 7.7 1 3.99 3.47 2.18 7.07l3.66 2.84c.87-2.6 3.3-4.53 6.16-4.53z"/></svg> Continue with Google';
      }}
    }}

    async function handleLogin(e) {{
      e.preventDefault();
      const btn = document.getElementById('connectBtn');
      const errEl = document.getElementById('errorMsg');
      btn.disabled = true;
      btn.classList.add('connecting');
      btn.textContent = 'Connecting...';
      errEl.style.display = 'none';

      try {{
        const resp = await fetch('/oauth/login', {{
          method: 'POST',
          headers: {{ 'Content-Type': 'application/json' }},
          body: JSON.stringify({{
            email: document.getElementById('email').value,
            password: document.getElementById('password').value,
            auth_state: AUTH_STATE,
          }}),
        }});
        const data = await resp.json();
        if (data.redirect_url) {{
          window.location.href = data.redirect_url;
        }} else {{
          errEl.textContent = data.error || 'Login failed. Check your credentials.';
          errEl.style.display = 'block';
          btn.disabled = false;
          btn.classList.remove('connecting');
          btn.textContent = 'Connect';
        }}
      }} catch (err) {{
        errEl.textContent = 'Connection error. Please try again.';
        errEl.style.display = 'block';
        btn.disabled = false;
        btn.classList.remove('connecting');
        btn.textContent = 'Connect';
      }}
    }}
  </script>
</body>
</html>"""


async def consent_page(request: Request) -> HTMLResponse:
    """Render the OAuth consent/login page."""
    auth_state = request.query_params.get("auth_state", "")
    client_name = request.query_params.get("client_name", "Claude")
    return HTMLResponse(
        CONSENT_HTML.format(
            client_name=client_name,
            auth_state=auth_state,
            google_client_id=GOOGLE_CLIENT_ID,
        )
    )


async def oauth_login(request: Request) -> JSONResponse:
    """Handle login form submission — authenticate with NB backend, complete OAuth."""
    from mcp.server.auth.provider import AuthorizeError
    try:
        body = await request.json()
        email = body.get("email", "").strip()
        password = body.get("password", "")
        auth_state = body.get("auth_state", "")

        if not email or not password or not auth_state:
            return JSONResponse({"error": "Missing required fields"}, status_code=400)

        # Authenticate against NobleBlocks backend
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.post(
                f"{NB_API_BASE}/api/v1/auth/login_with_email",
                json={"email": email, "password": password},
            )
            if resp.status_code != 200:
                return JSONResponse(
                    {"error": "Invalid email or password"},
                    status_code=401,
                )
            login_data = resp.json()
            token_obj = login_data.get("token", {})
            nb_token = token_obj.get("token", "") if isinstance(token_obj, dict) else ""

        if not nb_token:
            return JSONResponse({"error": "Login succeeded but no token received"}, status_code=500)

        # Complete OAuth flow
        redirect_url = await oauth_provider.complete_authorization(auth_state, nb_token)
        return JSONResponse({"redirect_url": redirect_url})

    except AuthorizeError as e:
        logger.warning("OAuth login AuthorizeError: %s", e.error_description)
        return JSONResponse(
            {"error": e.error_description or "Authorization session expired. Please try again."},
            status_code=400,
        )
    except Exception as e:
        logger.exception("OAuth login error")
        return JSONResponse({"error": "Internal error"}, status_code=500)


async def oauth_login_google(request: Request) -> JSONResponse:
    """Handle Google login — exchange Google access token for NB token, complete OAuth."""
    from mcp.server.auth.provider import AuthorizeError
    try:
        body = await request.json()
        access_token = body.get("access_token", "").strip()
        auth_state = body.get("auth_state", "")

        if not access_token or not auth_state:
            return JSONResponse({"error": "Missing required fields"}, status_code=400)

        # Authenticate against NobleBlocks backend with Google token
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.post(
                f"{NB_API_BASE}/api/v1/auth/login_with_google",
                json={"accessToken": access_token, "role": "Guest"},
            )
            if resp.status_code != 200:
                error_msg = "Google authentication failed"
                try:
                    err_data = resp.json()
                    if "message" in err_data:
                        error_msg = err_data["message"]
                except Exception:
                    pass
                logger.warning("Google login backend error: status=%d msg=%s", resp.status_code, error_msg)
                return JSONResponse({"error": error_msg}, status_code=401)
            login_data = resp.json()
            token_obj = login_data.get("token", {})
            nb_token = token_obj.get("token", "") if isinstance(token_obj, dict) else ""

        if not nb_token:
            return JSONResponse({"error": "Login succeeded but no token received"}, status_code=500)

        # Complete OAuth flow
        redirect_url = await oauth_provider.complete_authorization(auth_state, nb_token)
        return JSONResponse({"redirect_url": redirect_url})

    except AuthorizeError as e:
        logger.warning("OAuth Google login AuthorizeError: %s", e.error_description)
        return JSONResponse(
            {"error": e.error_description or "Authorization session expired. Please try again."},
            status_code=400,
        )
    except Exception as e:
        logger.exception("OAuth Google login error")
        return JSONResponse({"error": "Internal error"}, status_code=500)


FAVICON_URL = "https://www.nobleblocks.com/favicon.ico"

# Load icon bytes at startup for fast serving
import pathlib
_STATIC_DIR = pathlib.Path(__file__).parent / "static"
_ICON_BYTES = (_STATIC_DIR / "icon.png").read_bytes() if (_STATIC_DIR / "icon.png").exists() else None
_ICON_64_BYTES = (_STATIC_DIR / "favicon-64.png").read_bytes() if (_STATIC_DIR / "favicon-64.png").exists() else None


async def favicon(request: Request):
    """Serve the NobleBlocks favicon."""
    if _ICON_64_BYTES:
        from starlette.responses import Response
        return Response(content=_ICON_64_BYTES, media_type="image/png",
                       headers={"Cache-Control": "public, max-age=86400"})
    return RedirectResponse(url=FAVICON_URL, status_code=301)


async def icon_png(request: Request):
    """Serve the NobleBlocks icon (used by MCP clients for server branding)."""
    if _ICON_BYTES:
        from starlette.responses import Response
        return Response(content=_ICON_BYTES, media_type="image/png",
                       headers={"Cache-Control": "public, max-age=86400"})
    return RedirectResponse(url="https://www.nobleblocks.com/favicon.png", status_code=301)


async def health_check(request: Request) -> JSONResponse:
    """Health check endpoint for ALB/ECS."""
    return JSONResponse({
        "status": "healthy",
        "service": "nobleblocks-mcp",
        "version": SERVER_VERSION,
        "papers": "340M+",
    })


async def register_shim(request: Request):
    """
    Shim for POST /register.

    The MCP Python SDK's RegistrationHandler requires grant_types to include
    BOTH 'authorization_code' AND 'refresh_token'. Claude only sends
    ['authorization_code'], causing a 400 that breaks the entire auth flow.

    This shim patches the body before the SDK handler sees it.
    """
    import json as _json
    from mcp.server.auth.handlers.register import RegistrationHandler
    from mcp.server.auth.settings import ClientRegistrationOptions

    try:
        body = await request.json()
    except Exception:
        body = {}

    logger.info("register_shim: body keys=%s grant_types=%s scope=%s redirect_uris=%s",
                list(body.keys()), body.get("grant_types"), body.get("scope"),
                body.get("redirect_uris"))

    # Inject refresh_token so the SDK validation passes
    gt = set(body.get("grant_types") or ["authorization_code"])
    gt.add("authorization_code")
    gt.add("refresh_token")
    body["grant_types"] = list(gt)

    # Rebuild a patched Request with the modified body
    patched_body = _json.dumps(body).encode()

    async def patched_receive():
        return {"type": "http.request", "body": patched_body, "more_body": False}

    patched_request = Request(request.scope, patched_receive)

    # Delegate to the SDK's handler directly
    handler = RegistrationHandler(
        provider=oauth_provider,
        options=ClientRegistrationOptions(
            enabled=True,
            valid_scopes=["search", "review", "graph"],
            default_scopes=["search"],
        ),
    )
    return await handler.handle(patched_request)


async def info_page(request: Request) -> HTMLResponse:
    """Root page with info about the MCP server."""
    return HTMLResponse("""<!DOCTYPE html>
<html><head><title>NobleBlocks MCP Server</title>
<link rel="icon" type="image/x-icon" href="https://www.nobleblocks.com/favicon.ico">
<link rel="apple-touch-icon" href="https://www.nobleblocks.com/favicon.png">
<style>body{font-family:system-ui;max-width:600px;margin:60px auto;padding:20px;color:#1a1a2e}
h1{margin-bottom:8px}p{color:#475569;line-height:1.6}.url{background:#f1f5f9;padding:8px 16px;
border-radius:8px;font-family:monospace;font-size:14px;margin:16px 0;display:block}
a{color:#6366f1}</style></head>
<body>
<h1>NobleBlocks MCP Server</h1>
<p>Search 340M+ academic papers from Claude, ChatGPT, Cursor, and other AI assistants.</p>
<span class="url">Connector URL: https://mcp.nobleblocks.com/mcp</span>
<p><strong>How to connect:</strong></p>
<ol style="color:#475569;line-height:2">
<li>Open Claude → Settings → Connectors</li>
<li>Click "Add" and paste the URL above</li>
<li>Sign in with your NobleBlocks account</li>
<li>Start asking research questions!</li>
</ol>
<p><a href="https://www.nobleblocks.com/docs/mcp">Full documentation</a> ·
<a href="https://www.nobleblocks.com/privacy">Privacy Policy</a> ·
<a href="https://www.nobleblocks.com/terms">Terms of Service</a></p>
</body></html>""")


# ─── Build the full app ───────────────────────────────────────────────────────
def create_app() -> Starlette:
    """Create the full Starlette app with MCP + OAuth consent routes."""
    # Get the MCP Starlette app (includes /, /authorize, /token, etc.)
    mcp_app = mcp.streamable_http_app()

    # Add our custom routes (consent page, login handler, health)
    custom_routes = [
        Route("/", info_page, methods=["GET"]),
        Route("/health", health_check, methods=["GET"]),
        Route("/favicon.ico", favicon, methods=["GET"]),
        Route("/icon.png", icon_png, methods=["GET"]),
        Route("/consent", consent_page, methods=["GET"]),
        Route("/oauth/login", oauth_login, methods=["POST"]),
        Route("/oauth/login/google", oauth_login_google, methods=["POST"]),
        Route("/register", register_shim, methods=["POST"]),  # shim: injects refresh_token for Claude
    ]

    # Mount MCP app at / — handles POST /, /.well-known/*, /authorize, /token, etc.
    # PROTECTED: streamable_http_path="/" means handler lives at sub-app root.
    # The /mcp path is handled by ASGI middleware that rewrites it to /.
    from starlette.routing import Mount
    routes = custom_routes + [
        Mount("/", app=mcp_app),      # OAuth/well-known + POST / handler
    ]

    # Forward the MCP session manager's lifespan so its task group initializes
    @asynccontextmanager
    async def lifespan(app):
        async with mcp.session_manager.run():
            yield

    inner_app = Starlette(routes=routes, lifespan=lifespan)

    # ASGI middleware: rewrite /mcp → / so both connector URL paths work.
    # PROTECTED: ensures POST /mcp reaches the same handler as POST /.
    async def path_rewrite_middleware(scope, receive, send):
        if scope["type"] == "http" and scope.get("path", "").startswith("/mcp"):
            # Rewrite /mcp → / and /mcp/foo → /foo
            path = scope["path"]
            new_path = "/" + path[4:].lstrip("/") if len(path) > 4 else "/"
            scope = dict(scope, path=new_path, raw_path=new_path.encode())
        await inner_app(scope, receive, send)

    return path_rewrite_middleware


app = create_app()


def main():
    """Run the remote MCP server."""
    import uvicorn
    logger.info(
        "NobleBlocks MCP v2.0.0 (Remote) | API: %s | URL: %s:%d",
        NB_API_BASE, HOST, PORT,
    )
    uvicorn.run(app, host=HOST, port=PORT)


if __name__ == "__main__":
    main()
