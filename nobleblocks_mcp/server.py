"""
NobleBlocks MCP Server
======================

Exposes the NobleBlocks paper search corpus (290M+ papers across PubMed,
OpenAlex, SemanticScholar, arXiv, EuropePMC, Scopus) to AI tools that speak
the Model Context Protocol — Claude Desktop, ChatGPT (via MCP bridges),
Cursor, etc.

Tools exposed:
  - search_papers: keyword/full-text search with filters
  - get_paper:     fetch a single paper by ID
  - find_similar:  semantic similarity search (vector embeddings)

Configure via environment variables:
  NOBLEBLOCKS_API_BASE  Default: https://www.nobleblocks.com
  NOBLEBLOCKS_API_KEY   Optional. Free tier (no key) = 100 queries/day.
                        Pro tier (with key) = higher quotas + credits-based.
"""

from __future__ import annotations

import os
import sys
import json
import logging
from typing import Any

import httpx
from dotenv import load_dotenv
from mcp.server import Server, NotificationOptions
from mcp.server.models import InitializationOptions
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent

load_dotenv()

logger = logging.getLogger("nobleblocks-mcp")
logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"), stream=sys.stderr)

# ─── Configuration ─────────────────────────────────────────────────────────────
API_BASE = os.environ.get("NOBLEBLOCKS_API_BASE", "https://www.nobleblocks.com").rstrip("/")
API_KEY = os.environ.get("NOBLEBLOCKS_API_KEY", "")
USER_AGENT = "nobleblocks-mcp/0.1.0"
HTTP_TIMEOUT = 30.0

# ─── HTTP client ───────────────────────────────────────────────────────────────
def _headers() -> dict[str, str]:
    h = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    if API_KEY:
        h["Authorization"] = f"Bearer {API_KEY}"
    return h


async def _get(path: str, params: dict[str, Any]) -> dict:
    """GET request with error wrapping."""
    url = f"{API_BASE}{path}"
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT, headers=_headers()) as client:
        resp = await client.get(url, params={k: v for k, v in params.items() if v is not None})
        if resp.status_code == 429:
            raise RuntimeError(
                "Rate limit exceeded. Free tier = 100 queries/day. "
                "Set NOBLEBLOCKS_API_KEY for a Pro key with higher quotas."
            )
        resp.raise_for_status()
        return resp.json()


# ─── MCP server setup ──────────────────────────────────────────────────────────
server = Server("nobleblocks")


@server.list_tools()
async def list_tools() -> list[Tool]:
    """Return the tools this server exposes."""
    return [
        Tool(
            name="search_papers",
            description=(
                "Full-text search across 290M+ academic papers from PubMed, OpenAlex, "
                "SemanticScholar, arXiv, EuropePMC, and Scopus. Returns ranked results "
                "with title, authors, year, abstract, citations, and DOI. "
                "Use this when the user asks about scientific topics, medical research, "
                "or wants to find papers on any subject."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Search query (keywords, phrases, or natural language).",
                        "minLength": 2,
                        "maxLength": 500,
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Max results to return (1-50).",
                        "default": 10,
                        "minimum": 1,
                        "maximum": 50,
                    },
                    "min_year": {
                        "type": "integer",
                        "description": "Earliest publication year (e.g., 2020).",
                    },
                    "max_year": {
                        "type": "integer",
                        "description": "Latest publication year.",
                    },
                    "min_citations": {
                        "type": "integer",
                        "description": "Minimum citation count (filter low-impact papers).",
                        "minimum": 0,
                    },
                    "source": {
                        "type": "string",
                        "description": "Restrict to a single source: pubmed, openalex, semanticscholar, arxiv, europepmc, scopus.",
                    },
                    "sort": {
                        "type": "string",
                        "enum": ["relevance", "date", "citations"],
                        "description": "Result ordering. Default is relevance.",
                        "default": "relevance",
                    },
                },
                "required": ["query"],
            },
        ),
        Tool(
            name="get_paper",
            description=(
                "Fetch full metadata for a single paper by its ID. "
                "Returns title, authors, abstract, year, DOI, citations, references, "
                "and full-text URL when available."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "paper_id": {
                        "type": "string",
                        "description": "Paper identifier (DOI, OpenAlex ID, PMID, arXiv ID, or NobleBlocks paper_id).",
                    },
                },
                "required": ["paper_id"],
            },
        ),
        Tool(
            name="find_similar",
            description=(
                "Find papers semantically similar to a given paper or query, using "
                "dense vector embeddings (pgvector). Useful for literature discovery "
                "and finding related work beyond exact keyword matches."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Reference text — paper title, abstract snippet, or research question.",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Max similar papers to return (1-30).",
                        "default": 10,
                        "minimum": 1,
                        "maximum": 30,
                    },
                },
                "required": ["query"],
            },
        ),
    ]


@server.call_tool()
async def call_tool(name: str, arguments: dict[str, Any]) -> list[TextContent]:
    """Dispatch tool calls to the appropriate handler."""
    try:
        if name == "search_papers":
            result = await _tool_search_papers(arguments)
        elif name == "get_paper":
            result = await _tool_get_paper(arguments)
        elif name == "find_similar":
            result = await _tool_find_similar(arguments)
        else:
            return [TextContent(type="text", text=f"Unknown tool: {name}")]
        return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]
    except httpx.HTTPStatusError as e:
        return [TextContent(type="text", text=f"API error: {e.response.status_code} {e.response.text[:200]}")]
    except Exception as e:
        logger.exception("Tool call failed: %s", name)
        return [TextContent(type="text", text=f"Error: {e}")]


# ─── Tool implementations ──────────────────────────────────────────────────────
async def _tool_search_papers(args: dict[str, Any]) -> dict:
    data = await _get(
        "/api/v1/papers/search",
        {
            "query": args["query"],
            "limit": min(int(args.get("limit", 10)), 50),
            "min_year": args.get("min_year"),
            "max_year": args.get("max_year"),
            "min_citations": args.get("min_citations"),
            "source": args.get("source"),
            "sort": args.get("sort", "relevance"),
        },
    )
    # Trim heavy fields for compact output to the AI tool
    papers = data.get("papers") or data.get("results") or []
    return {
        "query": args["query"],
        "total": data.get("total", len(papers)),
        "results": [_compact_paper(p) for p in papers[:int(args.get("limit", 10))]],
    }


async def _tool_get_paper(args: dict[str, Any]) -> dict:
    paper_id = args["paper_id"]
    data = await _get("/api/v1/papers/lookup", {"id": paper_id})
    paper = data.get("paper") or data
    return _compact_paper(paper, include_full=True)


async def _tool_find_similar(args: dict[str, Any]) -> dict:
    data = await _get(
        "/api/v1/papers/similar",
        {
            "query": args["query"],
            "limit": min(int(args.get("limit", 10)), 30),
        },
    )
    papers = data.get("papers") or data.get("results") or []
    return {
        "query": args["query"],
        "results": [_compact_paper(p) for p in papers],
    }


def _compact_paper(p: dict, include_full: bool = False) -> dict:
    """Strip a paper record to AI-friendly fields."""
    if not isinstance(p, dict):
        return {}
    abstract = p.get("abstract") or ""
    if not include_full and len(abstract) > 600:
        abstract = abstract[:600] + "..."
    out = {
        "id": p.get("paperId") or p.get("id") or p.get("paper_id"),
        "title": p.get("title"),
        "authors": [
            a.get("name") if isinstance(a, dict) else str(a)
            for a in (p.get("authors") or [])[:6]
        ],
        "year": p.get("year") or _extract_year(p.get("publicationDate") or p.get("publication_date")),
        "doi": p.get("doi") or p.get("DOI") or (p.get("externalIds") or {}).get("DOI"),
        "citations": p.get("citationCount") or p.get("citation_count"),
        "source": p.get("source") or (p.get("publicationVenue") or {}).get("name"),
        "abstract": abstract,
        "url": p.get("url") or (p.get("openAccessPdf") or {}).get("url"),
    }
    if include_full:
        out["full_text_link"] = p.get("fullTextLink")
        out["references_count"] = p.get("referenceCount")
        out["pdf_url"] = (p.get("openAccessPdf") or {}).get("url")
    return out


def _extract_year(date_str: str | None) -> int | None:
    if not date_str:
        return None
    try:
        return int(str(date_str)[:4])
    except (ValueError, TypeError):
        return None


# ─── Entrypoint ────────────────────────────────────────────────────────────────
async def _serve() -> None:
    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            InitializationOptions(
                server_name="nobleblocks",
                server_version=__import__("nobleblocks_mcp").__version__,
                capabilities=server.get_capabilities(
                    notification_options=NotificationOptions(),
                    experimental_capabilities={},
                ),
            ),
        )


def main() -> None:
    """Console-script entrypoint."""
    import asyncio
    logger.info("Starting NobleBlocks MCP server (API base: %s)", API_BASE)
    asyncio.run(_serve())


if __name__ == "__main__":
    main()
