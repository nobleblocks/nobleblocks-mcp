"""
NobleBlocks MCP Server — Production Grade
==========================================

Exposes the NobleBlocks paper search corpus (340M+ papers across PubMed,
OpenAlex, SemanticScholar, arXiv, EuropePMC, Scopus) to AI tools that speak
the Model Context Protocol — Claude Desktop, ChatGPT (via MCP bridges),
Cursor, VS Code Copilot, etc.

Tools exposed:
  - search_papers:    keyword/full-text search with filters
  - get_paper:        fetch a single paper by ID
  - find_similar:     semantic similarity search (vector embeddings)
  - get_citation_graph: citation network for a paper (refs + citations)
  - create_literature_review: generate a lit review from search results (credits)

Security:
  - Input sanitization (max lengths, no script injection)
  - Rate limiting (100/day free, per-key tracking with sliding window)
  - Audit logging (JSON-L to file)
  - Bearer token auth (NobleBlocks API key)

Configure via environment variables:
  NOBLEBLOCKS_API_BASE   Default: https://www.nobleblocks.com
  NOBLEBLOCKS_API_KEY    Required for Pro tier. Free tier = 100 queries/day.
  LOG_LEVEL              Default: INFO
  AUDIT_LOG_FILE         Default: /tmp/nobleblocks-mcp-audit.jsonl
"""

from __future__ import annotations

import os
import re
import sys
import json
import time
import asyncio
import logging
from datetime import datetime, timezone
from pathlib import Path
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
LOCAL_VERSION = "2.0.28"
USER_AGENT = f"nobleblocks-mcp/{LOCAL_VERSION}"
HTTP_TIMEOUT = 30.0

# Rate limiting
RATE_LIMIT_TRIAL = int(os.environ.get("RATE_LIMIT_TRIAL", "100"))  # per day without key (generous free tier)
RATE_LIMIT_FREE = int(os.environ.get("RATE_LIMIT_FREE", "500"))  # per day (free key)
RATE_LIMIT_PRO = int(os.environ.get("RATE_LIMIT_PRO", "5000"))  # per day (pro key)
RATE_LIMIT_PER_MINUTE = int(os.environ.get("RATE_LIMIT_PER_MINUTE", "60"))

# Audit log
AUDIT_LOG_FILE = os.environ.get("AUDIT_LOG_FILE", "/tmp/nobleblocks-mcp-audit.jsonl")

# ─── Security: Input sanitization ──────────────────────────────────────────────
MAX_QUERY_LENGTH = 500
MAX_PAPER_ID_LENGTH = 100
DANGEROUS_PATTERNS = re.compile(
    r"(<script|javascript:|on\w+=|eval\(|exec\(|import\(|require\(|__proto__|constructor\[)",
    re.IGNORECASE,
)


def sanitize_input(value: str, max_length: int = MAX_QUERY_LENGTH) -> str:
    """Strip dangerous content and enforce length limits."""
    if not isinstance(value, str):
        return ""
    # Truncate
    value = value[:max_length].strip()
    # Strip null bytes
    value = value.replace("\x00", "")
    # Reject if contains dangerous patterns
    if DANGEROUS_PATTERNS.search(value):
        raise ValueError("Input contains potentially malicious content")
    return value


# ─── Rate limiter (in-memory sliding window) ───────────────────────────────────

SIGNUP_URL = "https://www.nobleblocks.com/auth/signup"
API_KEY_URL = "https://www.nobleblocks.com/settings/api-keys"


class RateLimiter:
    """Simple in-memory rate limiter with daily + per-minute windows."""

    def __init__(self):
        self._daily: dict[str, list[float]] = {}  # key -> [timestamps]
        self._minute: dict[str, list[float]] = {}

    def _prune(self, bucket: list[float], window_seconds: float) -> list[float]:
        cutoff = time.time() - window_seconds
        return [t for t in bucket if t > cutoff]

    def check(self, key: str = "anonymous") -> tuple[bool, str]:
        """Returns (allowed, reason). Raises nothing."""
        now = time.time()

        # No API key: generous daily limit (100/day) — no signup friction
        if not API_KEY:
            # Use daily window for trial (resets every 24h)
            trial_bucket = self._daily.get("__trial__", [])
            trial_bucket = self._prune(trial_bucket, 86400)
            if len(trial_bucket) >= RATE_LIMIT_TRIAL:
                return False, (
                    f"Daily limit reached ({RATE_LIMIT_TRIAL} queries/day). "
                    f"Upgrade to NobleBlocks Pro for unlimited access: {API_KEY_URL}"
                )
            trial_bucket.append(now)
            self._daily["__trial__"] = trial_bucket
            return True, ""

        # Per-minute check
        minute_bucket = self._minute.get(key, [])
        minute_bucket = self._prune(minute_bucket, 60)
        if len(minute_bucket) >= RATE_LIMIT_PER_MINUTE:
            return False, f"Rate limit: {RATE_LIMIT_PER_MINUTE} requests/minute exceeded"
        minute_bucket.append(now)
        self._minute[key] = minute_bucket

        # Daily check
        daily_limit = RATE_LIMIT_PRO if API_KEY.startswith("nb_pro_") else RATE_LIMIT_FREE
        daily_bucket = self._daily.get(key, [])
        daily_bucket = self._prune(daily_bucket, 86400)
        if len(daily_bucket) >= daily_limit:
            return False, f"Daily limit ({daily_limit}/day) exceeded. Upgrade at https://www.nobleblocks.com/pricing"
        daily_bucket.append(now)
        self._daily[key] = daily_bucket

        return True, ""


_rate_limiter = RateLimiter()

# ─── Audit logger ──────────────────────────────────────────────────────────────
_audit_file: Path | None = None


def _ensure_audit_log():
    global _audit_file
    if _audit_file is None:
        _audit_file = Path(AUDIT_LOG_FILE)
        _audit_file.parent.mkdir(parents=True, exist_ok=True)


def audit_log(tool: str, args: dict[str, Any], success: bool, duration_ms: float):
    """Append a JSON-L line to the audit log."""
    try:
        _ensure_audit_log()
        entry = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "tool": tool,
            "args": {k: v for k, v in args.items() if k != "api_key"},  # never log keys
            "key_present": bool(API_KEY),
            "success": success,
            "duration_ms": round(duration_ms, 1),
        }
        with open(_audit_file, "a") as f:  # type: ignore[arg-type]
            f.write(json.dumps(entry) + "\n")
    except Exception:
        pass  # Audit logging must never crash the server


# ─── HTTP client ───────────────────────────────────────────────────────────────
def _headers() -> dict[str, str]:
    h = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    if API_KEY:
        h["Authorization"] = f"Bearer {API_KEY}"
    else:
        h["X-MCP-Trial"] = "true"
    return h


async def _get(path: str, params: dict[str, Any]) -> dict:
    """GET request with error wrapping and retry on 502/503/504."""
    url = f"{API_BASE}{path}"
    last_exc: Exception | None = None
    for attempt in range(3):
        try:
            async with httpx.AsyncClient(timeout=HTTP_TIMEOUT, headers=_headers()) as client:
                resp = await client.get(url, params={k: v for k, v in params.items() if v is not None})
                if resp.status_code in (502, 503, 504) and attempt < 2:
                    await asyncio.sleep(1.0 * (attempt + 1))
                    continue
                if resp.status_code == 401:
                    body = resp.json() if resp.headers.get("content-type", "").startswith("application/json") else {}
                    if body.get("trial_expired"):
                        raise RuntimeError(body.get("error", "Trial expired. Sign up at https://www.nobleblocks.com/auth/register"))
                    raise RuntimeError("Invalid API key. Get one at https://www.nobleblocks.com/settings/api-keys")
                if resp.status_code == 429:
                    raise RuntimeError(
                        "Rate limit exceeded. Free tier = 100 queries/day. "
                        "Set NOBLEBLOCKS_API_KEY for Pro access."
                    )
                if resp.status_code == 403:
                    raise RuntimeError("Access denied. Your API key may not have permission for this resource.")
                resp.raise_for_status()
                return resp.json()
        except (httpx.TimeoutException, httpx.ConnectError) as e:
            last_exc = e
            if attempt < 2:
                await asyncio.sleep(1.5 * (attempt + 1))
                continue
            raise
    raise last_exc or RuntimeError("Request failed after retries")


async def _post(path: str, body: dict[str, Any]) -> dict:
    """POST request with error wrapping."""
    url = f"{API_BASE}{path}"
    async with httpx.AsyncClient(timeout=60.0, headers=_headers()) as client:
        resp = await client.post(url, json=body)
        if resp.status_code == 401:
            raise RuntimeError("Invalid API key. Get one at https://www.nobleblocks.com/settings/api-keys")
        if resp.status_code == 402:
            raise RuntimeError("Insufficient credits. Top up at https://www.nobleblocks.com/pricing")
        if resp.status_code == 429:
            raise RuntimeError("Rate limit exceeded.")
        resp.raise_for_status()
        return resp.json()


# ─── MCP server setup ──────────────────────────────────────────────────────────
server = Server("nobleblocks")


TOOL_ANNOTATIONS = {
    "readOnlyHint": True,
    "destructiveHint": False,
    "idempotentHint": True,
    "openWorldHint": False,
}


@server.list_tools()
async def list_tools() -> list[Tool]:
    """Return the tools this server exposes."""
    return [
        Tool(
            name="search_papers",
            description=(
                "Search 340M+ academic papers, patents, clinical trials, and grants from PubMed, arXiv, Crossref, "
                "ClinicalTrials.gov, OpenAlex, and dozens of other sources. Returns ranked results with title, "
                "authors, year, abstract, citations, and DOI. Use this when the user asks about "
                "research, studies, clinical trials, evidence, or wants to find scientific papers on any topic. "
                "Supports filtering by open access, document type, author name, language, citation count, and year range."
            ),
            annotations=TOOL_ANNOTATIONS,
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Search query (keywords, phrases, or natural language question).",
                        "minLength": 2,
                        "maxLength": MAX_QUERY_LENGTH,
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Max results to return (1-50). Default 10.",
                        "default": 10,
                        "minimum": 1,
                        "maximum": 50,
                    },
                    "min_year": {
                        "type": "integer",
                        "description": "Earliest publication year filter (e.g. 2020).",
                    },
                    "max_year": {
                        "type": "integer",
                        "description": "Latest publication year filter.",
                    },
                    "min_citations": {
                        "type": "integer",
                        "description": "Minimum citation count to filter out low-impact papers.",
                        "minimum": 0,
                    },
                    "source": {
                        "type": "string",
                        "description": "Restrict to a source: pubmed, openalex, semanticscholar, arxiv, europepmc, clinicaltrials, crossref, doaj.",
                        "enum": ["pubmed", "openalex", "semanticscholar", "arxiv", "europepmc", "clinicaltrials", "crossref", "doaj"],
                    },
                    "sort": {
                        "type": "string",
                        "enum": ["relevance", "date", "citations"],
                        "description": "Result ordering. Default: relevance.",
                        "default": "relevance",
                    },
                    "open_access": {
                        "type": "boolean",
                        "description": "Filter to only open access papers with free full-text available.",
                    },
                    "doc_type": {
                        "type": "string",
                        "description": "Filter by document type.",
                        "enum": ["journal-article", "review", "preprint", "conference", "book-chapter", "dataset", "patent", "clinical-trial"],
                    },
                    "author_name": {
                        "type": "string",
                        "description": "Filter results by author name (partial match supported).",
                    },
                    "language": {
                        "type": "string",
                        "description": "Filter by paper language (ISO 639-1 code).",
                        "enum": ["en", "zh", "es", "fr", "de", "ja", "ko", "pt", "ru", "ar"],
                    },
                },
                "required": ["query"],
            },
        ),
        Tool(
            name="get_paper",
            description=(
                "Fetch full metadata for a single paper by its identifier. "
                "Supports DOI, PMID, arXiv ID, OpenAlex ID, or NobleBlocks internal ID. "
                "Returns title, authors, abstract, year, DOI, citation count, "
                "and PDF link when available."
            ),
            annotations=TOOL_ANNOTATIONS,
            inputSchema={
                "type": "object",
                "properties": {
                    "paper_id": {
                        "type": "string",
                        "description": (
                            "Paper identifier. Formats: DOI (10.xxxx/xxx), "
                            "PMID (12345678), arXiv (2301.12345), "
                            "OpenAlex (W1234567890), or NobleBlocks ID."
                        ),
                        "maxLength": MAX_PAPER_ID_LENGTH,
                    },
                },
                "required": ["paper_id"],
            },
        ),
        Tool(
            name="find_similar",
            description=(
                "Find papers semantically similar to a given text using dense "
                "vector embeddings (768-dim nomic-embed). Goes beyond keyword matching — "
                "discovers conceptually related work. Provide a paper title, abstract "
                "snippet, or research question as the query."
            ),
            annotations=TOOL_ANNOTATIONS,
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Reference text — paper title, abstract snippet, or research question.",
                        "maxLength": MAX_QUERY_LENGTH,
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Max similar papers to return (1-30). Default 10.",
                        "default": 10,
                        "minimum": 1,
                        "maximum": 30,
                    },
                },
                "required": ["query"],
            },
        ),
        Tool(
            name="get_citation_graph",
            description=(
                "Get the citation network for a paper — who it cites (references) "
                "and who cites it (citing papers). Useful for understanding a paper's "
                "impact and finding the foundational work in a field."
            ),
            annotations=TOOL_ANNOTATIONS,
            inputSchema={
                "type": "object",
                "properties": {
                    "paper_id": {
                        "type": "string",
                        "description": "Paper DOI, PMID, or NobleBlocks ID.",
                        "maxLength": MAX_PAPER_ID_LENGTH,
                    },
                    "direction": {
                        "type": "string",
                        "enum": ["references", "citations", "both"],
                        "description": "Which direction of the citation graph. Default: both.",
                        "default": "both",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Max papers per direction (1-50). Default 20.",
                        "default": 20,
                        "minimum": 1,
                        "maximum": 50,
                    },
                },
                "required": ["paper_id"],
            },
        ),
        Tool(
            name="create_literature_review",
            description=(
                "Generate a structured literature review from a search query. "
                "Searches papers, synthesizes findings, and produces a formatted "
                "review with inline citations. Costs credits from the user's "
                "NobleBlocks account. Requires a Pro API key."
            ),
            annotations=TOOL_ANNOTATIONS,
            inputSchema={
                "type": "object",
                "properties": {
                    "topic": {
                        "type": "string",
                        "description": "Research topic or question for the literature review.",
                        "maxLength": MAX_QUERY_LENGTH,
                    },
                    "num_papers": {
                        "type": "integer",
                        "description": "Number of papers to include (5-50). Default 15.",
                        "default": 15,
                        "minimum": 5,
                        "maximum": 50,
                    },
                    "style": {
                        "type": "string",
                        "enum": ["narrative", "systematic", "scoping"],
                        "description": "Review style. Default: narrative.",
                        "default": "narrative",
                    },
                },
                "required": ["topic"],
            },
        ),
        Tool(
            name="search_by_entity",
            description=(
                "Explore the NobleBlocks knowledge graph — find connections between "
                "genes, drugs, diseases, institutions, researchers, and topics. "
                "See which papers link entities together and discover hidden relationships "
                "across 1.3M+ entities and 109M+ paper connections."
            ),
            annotations=TOOL_ANNOTATIONS,
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": (
                            "Entity name or research concept. Examples: 'BRCA1', "
                            "'metformin', 'Alzheimer disease', 'MIT', 'CRISPR-Cas9'."
                        ),
                        "maxLength": MAX_QUERY_LENGTH,
                    },
                    "max_nodes": {
                        "type": "integer",
                        "description": "Max entities/papers to return (1-50). Default 20.",
                        "default": 20,
                        "minimum": 1,
                        "maximum": 50,
                    },
                },
                "required": ["query"],
            },
        ),
        Tool(
            name="search_grants",
            description=(
                "Search for research grants and funding sources. Find which organizations "
                "fund research on a given topic, or discover what research a specific funder "
                "(e.g. NIH, Gates Foundation, ERC) has supported. Returns funders, their "
                "funded papers, and award IDs."
            ),
            annotations=TOOL_ANNOTATIONS,
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": (
                            "Funder name OR research topic. Examples: 'NIH', "
                            "'Gates Foundation', 'ERC', 'CRISPR funding', "
                            "'cancer immunotherapy grants'."
                        ),
                        "maxLength": MAX_QUERY_LENGTH,
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Max results to return (1-50). Default 20.",
                        "default": 20,
                        "minimum": 1,
                        "maximum": 50,
                    },
                },
                "required": ["query"],
            },
        ),
    ]


@server.call_tool()
async def call_tool(name: str, arguments: dict[str, Any]) -> list[TextContent]:
    """Dispatch tool calls with rate limiting, audit logging, and input sanitization."""
    t0 = time.time()

    # Rate limit check
    key_id = API_KEY[:8] if API_KEY else "anon"
    allowed, reason = _rate_limiter.check(key_id)
    if not allowed:
        audit_log(name, arguments, success=False, duration_ms=0)
        return [TextContent(type="text", text=f"⛔ {reason}")]

    # Trial notice (prepended to response when no key)
    trial_notice = ""
    if not API_KEY:
        trial_bucket = _rate_limiter._daily.get("__trial__", [])
        trial_bucket = _rate_limiter._prune(trial_bucket, 86400)
        remaining = max(0, RATE_LIMIT_TRIAL - len(trial_bucket))
        trial_notice = (
            f"ℹ️ {remaining}/{RATE_LIMIT_TRIAL} queries remaining today. "
            f"Upgrade to NobleBlocks Pro for unlimited queries: {API_KEY_URL}\n\n"
        )

    try:
        # Sanitize all string inputs
        safe_args = _sanitize_args(arguments)

        if name == "search_papers":
            result = await _tool_search_papers(safe_args)
        elif name == "get_paper":
            result = await _tool_get_paper(safe_args)
        elif name == "find_similar":
            result = await _tool_find_similar(safe_args)
        elif name == "get_citation_graph":
            result = await _tool_get_citation_graph(safe_args)
        elif name == "create_literature_review":
            result = await _tool_create_literature_review(safe_args)
        elif name == "search_by_entity":
            result = await _tool_search_by_entity(safe_args)
        elif name == "search_grants":
            result = await _tool_search_grants(safe_args)
        else:
            audit_log(name, arguments, success=False, duration_ms=0)
            return [TextContent(type="text", text=f"Unknown tool: {name}")]

        duration = (time.time() - t0) * 1000
        audit_log(name, safe_args, success=True, duration_ms=duration)
        output = json.dumps(result, indent=2, default=str)
        return [TextContent(type="text", text=f"{trial_notice}{output}")]

    except ValueError as e:
        duration = (time.time() - t0) * 1000
        audit_log(name, arguments, success=False, duration_ms=duration)
        return [TextContent(type="text", text=f"Input validation error: {e}")]
    except httpx.HTTPStatusError as e:
        duration = (time.time() - t0) * 1000
        audit_log(name, arguments, success=False, duration_ms=duration)
        return [TextContent(type="text", text=f"API error: {e.response.status_code} — {e.response.text[:200]}")]
    except RuntimeError as e:
        duration = (time.time() - t0) * 1000
        audit_log(name, arguments, success=False, duration_ms=duration)
        return [TextContent(type="text", text=str(e))]
    except Exception as e:
        duration = (time.time() - t0) * 1000
        logger.exception("Tool call failed: %s", name)
        audit_log(name, arguments, success=False, duration_ms=duration)
        return [TextContent(type="text", text="Internal error. Please try again or contact info@nobleblocks.com")]


def _sanitize_args(args: dict[str, Any]) -> dict[str, Any]:
    """Deep-sanitize all string arguments."""
    safe = {}
    for k, v in args.items():
        if isinstance(v, str):
            max_len = MAX_PAPER_ID_LENGTH if "id" in k else MAX_QUERY_LENGTH
            safe[k] = sanitize_input(v, max_len)
        elif isinstance(v, (int, float, bool)):
            safe[k] = v
        elif v is None:
            safe[k] = v
        else:
            # Reject complex types — no dicts/lists from user input
            safe[k] = str(v)[:MAX_QUERY_LENGTH]
    return safe


# ─── Tool implementations ──────────────────────────────────────────────────────

async def _tool_search_papers(args: dict[str, Any]) -> dict:
    """Search 340M+ papers, patents, clinical trials, and grants."""
    query = args.get("query", "")
    if len(query) < 2:
        raise ValueError("Query must be at least 2 characters")

    limit = min(int(args.get("limit", 20)), 50)
    open_access = args.get("open_access")
    data = await _get(
        "/api/v1/papers/search",
        {
            "query": query,
            "limit": limit,
            # PROTECTED: phase=fast MUST stay. Removing this triggers AI rewrites
            # + external API calls (S2/OpenAlex/CrossRef) for every MCP search,
            # burning LLM budget and causing latency spikes site-wide.
            # Verified by: python3 nobleblocks-mcp/scripts/regression_test.py
            "phase": "fast",
            "min_year": args.get("min_year"),
            "max_year": args.get("max_year"),
            "min_citations": args.get("min_citations"),
            "source": args.get("source"),
            "sort": args.get("sort", "relevance"),
            "open_access": "true" if open_access else None,
            "doc_type": args.get("doc_type"),
            "author_name": args.get("author_name"),
            "language": args.get("language"),
        },
    )
    papers = data.get("papers") or data.get("results") or []

    # Fallback: if fast phase returned 0 results, retry with full search
    if not papers:
        data = await _get(
            "/api/v1/papers/search",
            {
                "query": query,
                "limit": limit,
                "min_year": args.get("min_year"),
                "max_year": args.get("max_year"),
                "min_citations": args.get("min_citations"),
                "source": args.get("source"),
                "sort": args.get("sort", "relevance"),
                "open_access": "true" if open_access else None,
                "doc_type": args.get("doc_type"),
                "author_name": args.get("author_name"),
                "language": args.get("language"),
            },
        )
        papers = data.get("papers") or data.get("results") or []

    # Filter out Figshare datasets from citation-sorted results (inflated citation counts)
    sort = args.get("sort", "relevance")
    if sort == "citations" and not args.get("doc_type"):
        papers = [p for p in papers if not _is_figshare_dataset(p)]

    results = [_compact_paper(p) for p in papers[:limit]]
    # Correct total: must be consistent with results (fixes Arabic query mismatch)
    raw_total = data.get("total", len(papers))
    total = max(raw_total, len(results)) if results else 0

    return {
        "query": query,
        "total": total,
        "results": results,
        "source": "NobleBlocks Academic Database",
        "database_coverage": "340M+ papers from PubMed, arXiv, Crossref, Semantic Scholar, OpenAlex, DOAJ, ClinicalTrials.gov",
    }


async def _tool_get_paper(args: dict[str, Any]) -> dict:
    """Fetch a single paper by ID."""
    paper_id = args.get("paper_id", "")
    if not paper_id:
        raise ValueError("paper_id is required")

    data = await _get("/api/v1/papers/lookup", {"id": paper_id})
    paper = data.get("paper") or data
    result = _compact_paper(paper, include_full=True)

    # Enrich: if citations is 0 and we have a title, try search to get real count
    if result.get("citations", 0) == 0 and result.get("title"):
        try:
            search_data = await _get(
                "/api/v1/papers/search",
                {"query": result["title"], "limit": 1, "phase": "fast"},
            )
            search_papers = search_data.get("papers") or search_data.get("results") or []
            if search_papers:
                sp = search_papers[0]
                enriched_citations = _first_non_none(
                    sp.get("citationCount"), sp.get("citation_count"),
                    sp.get("cited_by_count"), sp.get("numCitations"),
                )
                if enriched_citations and enriched_citations > 0:
                    result["citations"] = enriched_citations
                # Also enrich venue and references if missing
                if not result.get("venue"):
                    result["venue"] = sp.get("venue") or sp.get("source") or (sp.get("journal") if isinstance(sp.get("journal"), str) else (sp.get("journal") or {}).get("name"))
                if not result.get("references_count"):
                    result["references_count"] = sp.get("referenceCount") or sp.get("reference_count") or 0
        except Exception:
            pass  # Enrichment is best-effort

    return {
        **result,
        "attribution": "Powered by NobleBlocks (nobleblocks.com)",
    }


async def _tool_find_similar(args: dict[str, Any]) -> dict:
    """Semantic similarity search."""
    query = args.get("query", "")
    if len(query) < 5:
        raise ValueError("Query must be at least 5 characters for similarity search")

    limit = min(int(args.get("limit", 10)), 30)
    vector_error = None
    try:
        data = await _get(
            "/api/v1/papers/similar",
            {
                "query": query,
                "limit": limit,
            },
        )
        papers = data.get("papers") or data.get("results") or []
        if papers:
            return {
                "query": query,
                "results": [_compact_paper(p) for p in papers],
                "search_method": "vector_embedding",
                "attribution": "Powered by NobleBlocks semantic search (nobleblocks.com)",
            }
        vector_error = "Vector search returned empty results"
    except Exception as e:
        vector_error = str(e)[:200]

    # Fallback: use text search sorted by relevance
    data = await _get(
        "/api/v1/papers/search",
        {"query": query, "limit": limit, "phase": "fast", "sort": "relevance"},
    )
    papers = data.get("papers") or data.get("results") or []
    return {
        "query": query,
        "results": [_compact_paper(p) for p in papers],
        "search_method": "text_fallback",
        "note": f"Used relevance-ranked text search (vector search issue: {vector_error})",
        "attribution": "Powered by NobleBlocks (nobleblocks.com)",
    }


async def _tool_get_citation_graph(args: dict[str, Any]) -> dict:
    """Fetch citation network for a paper."""
    paper_id = args.get("paper_id", "")
    if not paper_id:
        raise ValueError("paper_id is required")

    direction = args.get("direction", "both")
    limit = min(int(args.get("limit", 20)), 50)

    data = await _get(
        "/api/v1/papers/citation-graph",
        {
            "paperId": paper_id,
            "limit": limit,
        },
    )
    result: dict[str, Any] = {"paper_id": paper_id}

    if direction in ("references", "both"):
        refs = data.get("references") or []
        result["references"] = [_compact_paper(p) for p in refs[:limit]]

    if direction in ("citations", "both"):
        cites = data.get("citations") or []
        result["citations"] = [_compact_paper(p) for p in cites[:limit]]

    result["attribution"] = "Powered by NobleBlocks (nobleblocks.com)"
    return result


async def _tool_create_literature_review(args: dict[str, Any]) -> dict:
    """Generate a literature review (costs credits)."""
    if not API_KEY:
        raise RuntimeError(
            "Literature review generation requires a Pro API key. "
            "Sign up at https://www.nobleblocks.com/pricing"
        )

    # Accept both "topic" and "query" (Claude sometimes sends "query")
    topic = args.get("topic") or args.get("query", "")
    if len(topic) < 5:
        raise ValueError("Topic must be at least 5 characters")

    # Accept both "num_papers" and "max_papers" (Claude sometimes sends "max_papers")
    num_papers = int(args.get("num_papers") or args.get("max_papers", 15))

    data = await _post(
        "/api/v1/notebooks/from-search",
        {
            "searchQuery": topic,
            "papers": [],  # let the API search and pick
            "documentType": "literature_review",
            "maxWords": 2000,
            "tone": "academic",
            "numPapers": min(num_papers, 50),
            "style": args.get("style", "narrative"),
        },
    )
    return {
        "title": data.get("title", topic),
        "content_preview": (data.get("content", {}).get("text") or "")[:2000],
        "word_count": data.get("wordCount", 0),
        "credits_used": data.get("creditsUsed", 1),
        "full_url": f"https://www.nobleblocks.com/notebooks/{data.get('id', '')}",
        "attribution": "Generated by NobleBlocks AI Writer (nobleblocks.com)",
    }


async def _tool_search_by_entity(args: dict[str, Any]) -> dict:
    """Search the knowledge graph for entities and their linked papers."""
    query = args.get("query", "")
    if len(query) < 2:
        raise ValueError("Query must be at least 2 characters")

    max_nodes = min(int(args.get("max_nodes", 20)), 50)

    try:
        data = await _get("/api/v1/kg/explore", {"query": query, "max_nodes": max_nodes})
    except Exception as e:
        logger.warning("search_by_entity KG error for '%s': %s", query, e)
        # Fallback: use paper search with relevance sorting
        fallback_data = await _get(
            "/api/v1/papers/search",
            {"query": query, "limit": max_nodes, "sort": "relevance", "phase": "fast"},
        )
        fallback_papers = fallback_data.get("papers") or fallback_data.get("results") or []
        return {
            "query": query,
            "entities_found": 0,
            "papers_found": len(fallback_papers),
            "entities": [],
            "papers": [_compact_paper(p) for p in fallback_papers[:max_nodes]],
            "relationships": 0,
            "note": "Knowledge graph unavailable for this query. Showing relevant papers instead.",
            "attribution": "NobleBlocks (nobleblocks.com)",
        }

    # Format nodes into a compact response
    nodes = data.get("nodes", [])
    edges = data.get("edges", [])

    # Handle case where API returns a string/non-list for nodes
    if not isinstance(nodes, list):
        nodes = []
    if not isinstance(edges, list):
        edges = []

    entities = []
    papers = []
    for node in nodes:
        # Skip non-dict nodes (API sometimes returns string values)
        if not isinstance(node, dict):
            continue
        if node.get("type") == "entity":
            # Filter out garbage entities: email footers, ZIP codes, partial addresses
            name = node.get("name", "")
            if _is_garbage_entity(name):
                continue
            entities.append({
                "name": name,
                "entity_type": node.get("entityType"),
                "description": node.get("description"),
            })
        elif node.get("type") == "paper":
            paper = _compact_paper(node)
            # Enrich paper with metadata from node if _compact_paper missed it
            if not paper.get("year") and node.get("year"):
                paper["year"] = node["year"]
            if not paper.get("doi") and node.get("doi"):
                paper["doi"] = node["doi"]
            if not paper.get("authors") and node.get("authors"):
                raw = node["authors"]
                paper["authors"] = [a.get("name") if isinstance(a, dict) else str(a) for a in raw][:8] if isinstance(raw, list) else []
            papers.append(paper)

    return {
        "query": query,
        "entities_found": len(entities),
        "papers_found": len(papers),
        "entities": entities[:20],
        "papers": papers[:20],
        "relationships": len(edges),
        "attribution": "NobleBlocks Knowledge Graph (nobleblocks.com) — 1.3M+ entities, 109M+ links",
    }


async def _tool_search_grants(args: dict[str, Any]) -> dict:
    """Search for grants/funding sources in the knowledge graph."""
    query = args.get("query", "")
    if len(query) < 2:
        raise ValueError("Query must be at least 2 characters")

    limit = min(int(args.get("limit", 20)), 50)

    # Use the KG explore endpoint which already enriches with funder data
    data = await _get("/api/v1/kg/explore", {"query": query, "max_nodes": limit})

    nodes = data.get("nodes", [])
    edges = data.get("edges", [])

    # Ensure we have lists
    if not isinstance(nodes, list):
        nodes = []
    if not isinstance(edges, list):
        edges = []

    # Extract funders and their funded papers
    funders = []
    funded_papers = []
    funder_paper_links: dict[str, list[str]] = {}  # funder_uid -> [paper_titles]

    for node in nodes:
        if not isinstance(node, dict):
            continue
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

    # Map FUNDED edges to link funders to papers
    node_map = {n.get("uid"): n for n in nodes if isinstance(n, dict)}
    for edge in edges:
        if not isinstance(edge, dict):
            continue
        if edge.get("label") == "FUNDED":
            src = edge.get("source", "")
            tgt = edge.get("target", "")
            paper_node = node_map.get(tgt) or node_map.get(src)
            funder_uid = src if src in funder_paper_links else tgt
            if funder_uid in funder_paper_links and paper_node:
                title = paper_node.get("title") or paper_node.get("name", "")
                if title:
                    funder_paper_links[funder_uid].append(title)

    # Enrich funders with their paper list
    for i, funder in enumerate(funders):
        uid_candidates = [n.get("uid") for n in nodes
                          if isinstance(n, dict) and n.get("name") == funder["name"] and n.get("entityType") == "funder"]
        if uid_candidates:
            funder["funded_papers"] = funder_paper_links.get(uid_candidates[0], [])

    # If no funders found directly, papers themselves may contain funding info
    if not funders and funded_papers:
        return {
            "query": query,
            "funders_found": 0,
            "papers_with_funding": len(funded_papers),
            "papers": funded_papers[:limit],
            "note": "No specific funders found for this query. Try searching for a funder name (e.g. 'NIH', 'ERC', 'Gates Foundation').",
            "attribution": "NobleBlocks Knowledge Graph (nobleblocks.com)",
        }

    return {
        "query": query,
        "funders_found": len(funders),
        "funded_papers_found": len(funded_papers),
        "funders": funders[:limit],
        "funded_papers": funded_papers[:limit],
        "attribution": "NobleBlocks Knowledge Graph (nobleblocks.com) — Grants & Funding data",
    }


# ─── Paper formatting ──────────────────────────────────────────────────────────

def _compact_paper(p: Any, include_full: bool = False) -> dict:
    """Strip a paper record to AI-friendly fields. Never expose full text."""
    if not isinstance(p, dict):
        return {}
    abstract = p.get("abstract") or ""
    # Cap abstract length — never expose full text through MCP
    if len(abstract) > 600:
        abstract = abstract[:600] + "..."

    # Robust citation count extraction — check multiple field names
    # NOTE: Can't use `or` chaining because 0 is falsy in Python!
    citations = _first_non_none(
        p.get("citationCount"),
        p.get("citation_count"),
        p.get("cited_by_count"),
        p.get("citations_count"),
        p.get("numCitations"),
    )
    if citations is None:
        citations = 0
    # If citations is a list (citation objects), use length
    if isinstance(citations, list):
        citations = len(citations)

    # Robust author extraction
    raw_authors = p.get("authors") or []
    if isinstance(raw_authors, str):
        # Sometimes authors come as a comma-separated string
        authors = [a.strip() for a in raw_authors.split(",") if a.strip()][:8]
    else:
        authors = [
            a.get("name") if isinstance(a, dict) else str(a)
            for a in (raw_authors or [])[:8]
        ]

    # KG nodes use "name" instead of "title"
    title = p.get("title") or p.get("name")

    out: dict[str, Any] = {
        "id": p.get("paperId") or p.get("id") or p.get("paper_id") or p.get("uid"),
        "title": title,
        "authors": authors,
        "year": p.get("year") or _extract_year(p.get("publicationDate") or p.get("publication_date")),
        "doi": p.get("doi") or p.get("DOI") or ((p.get("externalIds") or {}).get("DOI") if isinstance(p.get("externalIds"), dict) else None),
        "citations": citations,
        "venue": p.get("venue") or p.get("source") or (p.get("journal") if isinstance(p.get("journal"), str) else (p.get("journal") or {}).get("name")),
        "abstract": abstract,
        "url": _build_url(p),
    }
    if include_full:
        out["references_count"] = p.get("referenceCount") or p.get("reference_count") or 0
        out["open_access"] = p.get("isOpenAccess", False)
        out["pdf_url"] = (p.get("openAccessPdf") or {}).get("url")
    return out


def _first_non_none(*values):
    """Return the first value that is not None (0 and '' are valid)."""
    for v in values:
        if v is not None:
            return v
    return None


# Regex patterns for garbage entities extracted from PDF footers
_GARBAGE_ENTITY_RE = re.compile(
    r"(\d{5})|"                       # ZIP codes (5-digit)
    r"(@[a-zA-Z0-9.-]+\.[a-z]{2,})|"  # Email addresses
    r"(^\d+\s)|"                       # Leading numbers
    r"(\.edu\b|\.org\b|\.com\b)|"     # Bare domain fragments
    r"(\band\.$)"                      # Trailing "and."
)


def _is_garbage_entity(name: str) -> bool:
    """Return True if the entity name looks like a PDF footer/address, not a real entity."""
    if not name or len(name) < 3:
        return True
    if len(name) > 200:
        return True
    # Contains email
    if "@" in name:
        return True
    # Mostly digits (ZIP/postal code)
    digits = sum(c.isdigit() for c in name)
    if digits > len(name) * 0.5:
        return True
    # Ends with period + country or state
    if _GARBAGE_ENTITY_RE.search(name):
        return True
    return False


def _is_figshare_dataset(p: dict) -> bool:
    """Detect Figshare datasets which often have inflated citation counts."""
    if not isinstance(p, dict):
        return False
    doi = (p.get("doi") or p.get("DOI") or "").lower()
    if "figshare" in doi:
        return True
    venue = (p.get("venue") or p.get("source") or "").lower()
    if "figshare" in venue:
        return True
    # Also filter generic "dataset" typed records when they dominate citation sorts
    doc_type = (p.get("type") or p.get("doc_type") or p.get("documentType") or "").lower()
    if doc_type == "dataset":
        return True
    return False


def _build_url(p: dict) -> str | None:
    """Build a canonical URL for the paper."""
    doi = p.get("doi") or p.get("DOI") or ((p.get("externalIds") or {}).get("DOI") if isinstance(p.get("externalIds"), dict) else None)
    if doi:
        return f"https://doi.org/{doi}"
    # Fallback: build NobleBlocks URL from uid/nobleId if available
    noble_id = p.get("nobleId") or p.get("noble_id")
    if noble_id:
        return f"https://www.nobleblocks.com/publications/{noble_id}"
    return p.get("url")


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
                server_version="2.0.0",
                capabilities=server.get_capabilities(
                    notification_options=NotificationOptions(),
                    experimental_capabilities={},
                ),
            ),
        )


def main() -> None:
    """Console-script entrypoint."""
    import asyncio
    logger.info("NobleBlocks MCP v%s | API: %s | Key: %s", LOCAL_VERSION, API_BASE, "Pro" if API_KEY else "Free tier")
    # Non-blocking update check
    _check_for_updates()
    asyncio.run(_serve())


def _check_for_updates() -> None:
    """Warn user if a newer version is available on the remote server."""
    try:
        import urllib.request
        req = urllib.request.Request(
            "https://mcp.nobleblocks.com/health",
            headers={"User-Agent": USER_AGENT},
        )
        with urllib.request.urlopen(req, timeout=3) as resp:
            data = json.loads(resp.read())
            remote_version = data.get("version", "")
            if remote_version and remote_version != LOCAL_VERSION:
                logger.warning(
                    "⚠ A newer version is available: v%s (you have v%s). "
                    "Update with: pip install --upgrade nobleblocks-mcp",
                    remote_version, LOCAL_VERSION,
                )
    except Exception:
        pass  # Non-critical — don't block startup


if __name__ == "__main__":
    main()
