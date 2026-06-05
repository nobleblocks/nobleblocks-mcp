"""
NobleBlocks Remote MCP Server — Streamable HTTP + OAuth 2.1
============================================================

This is the server that gets deployed at https://mcp.nobleblocks.com/mcp
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
from nobleblocks_mcp.oauth_provider import NobleBlocksOAuthProvider

load_dotenv()

logger = logging.getLogger("nobleblocks-mcp")
logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))

# ─── Configuration ─────────────────────────────────────────────────────────────
NB_API_BASE = os.environ.get("NOBLEBLOCKS_API_BASE", "https://www.nobleblocks.com").rstrip("/")
MCP_BASE_URL = os.environ.get("MCP_BASE_URL", "https://mcp.nobleblocks.com").rstrip("/")
HOST = os.environ.get("MCP_HOST", "0.0.0.0")
PORT = int(os.environ.get("MCP_PORT", "8080"))

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
        "NobleBlocks gives you access to 340M+ academic papers from PubMed, "
        "arXiv, Crossref, and dozens of other sources — plus a biomedical knowledge "
        "graph with 1.3M+ entities (genes, drugs, diseases, institutions). "
        "Use the search tool for any research question. Use find_similar to "
        "discover related work. Use get_citation_graph for impact analysis. "
        "Use search_by_entity to explore connections in the knowledge graph."
    ),
    auth_server_provider=oauth_provider,
    auth=AuthSettings(
        issuer_url=MCP_BASE_URL,
        service_documentation_url="https://www.nobleblocks.com/docs/mcp",
        resource_server_url=MCP_BASE_URL,
        client_registration_options=ClientRegistrationOptions(
            enabled=True,
            valid_scopes=["search", "review", "graph"],
            default_scopes=["search"],
        ),
    ),
    host=HOST,
    port=PORT,
    streamable_http_path="/mcp",
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
    if api_key:
        h["Authorization"] = f"Bearer {api_key}"
    return h


async def _api_get(path: str, params: dict[str, Any], api_key: str = "") -> dict:
    url = f"{NB_API_BASE}{path}"
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT, headers=_headers(api_key)) as client:
        resp = await client.get(url, params={k: v for k, v in params.items() if v is not None})
        resp.raise_for_status()
        return resp.json()


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
            "min_year": min_year,
            "max_year": max_year,
            "min_citations": min_citations,
            "source": source,
            "sort": sort,
        },
    )
    papers = data.get("papers") or data.get("results") or []
    result = {
        "query": query,
        "total": data.get("total", len(papers)),
        "results": [_compact_paper(p) for p in papers[:min(limit, 50)]],
        "attribution": "Powered by NobleBlocks (nobleblocks.com) — 300M+ papers across 6 academic databases",
    }
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


# ─── Consent Page ──────────────────────────────────────────────────────────────
CONSENT_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Connect NobleBlocks to {client_name}</title>
  <style>
    * {{ margin: 0; padding: 0; box-sizing: border-box; }}
    body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
           background: #f8fafc; display: flex; justify-content: center; align-items: center;
           min-height: 100vh; padding: 20px; }}
    .card {{ background: white; border-radius: 16px; box-shadow: 0 4px 24px rgba(0,0,0,0.08);
             max-width: 440px; width: 100%; padding: 40px; text-align: center; }}
    .logo {{ width: 64px; height: 64px; margin: 0 auto 16px; background: #1a1a2e;
             border-radius: 12px; display: flex; align-items: center; justify-content: center;
             color: white; font-weight: bold; font-size: 18px; }}
    h1 {{ font-size: 22px; color: #1a1a2e; margin-bottom: 8px; }}
    .subtitle {{ color: #64748b; font-size: 14px; margin-bottom: 24px; line-height: 1.5; }}
    .scope {{ background: #f1f5f9; border-radius: 8px; padding: 16px; text-align: left;
              margin-bottom: 24px; }}
    .scope h3 {{ font-size: 13px; color: #475569; text-transform: uppercase; margin-bottom: 8px; }}
    .scope li {{ font-size: 14px; color: #334155; margin: 4px 0; list-style: none;
                 padding-left: 20px; position: relative; }}
    .scope li::before {{ content: "✓"; position: absolute; left: 0; color: #22c55e; }}
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
    .footer {{ font-size: 12px; color: #94a3b8; margin-top: 16px; }}
    .footer a {{ color: #6366f1; text-decoration: none; }}
  </style>
</head>
<body>
  <div class="card">
    <div class="logo">NB</div>
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

    <form id="loginForm" onsubmit="return handleLogin(event)">
      <div class="login-section">
        <label for="email">NobleBlocks Email</label>
        <input type="email" id="email" name="email" required placeholder="you@university.edu">
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
    async function handleLogin(e) {{
      e.preventDefault();
      const btn = document.getElementById('connectBtn');
      const errEl = document.getElementById('errorMsg');
      btn.disabled = true;
      btn.textContent = 'Connecting...';
      errEl.style.display = 'none';

      try {{
        const resp = await fetch('/oauth/login', {{
          method: 'POST',
          headers: {{ 'Content-Type': 'application/json' }},
          body: JSON.stringify({{
            email: document.getElementById('email').value,
            password: document.getElementById('password').value,
            auth_state: '{auth_state}',
          }}),
        }});
        const data = await resp.json();
        if (data.redirect_url) {{
          window.location.href = data.redirect_url;
        }} else {{
          errEl.textContent = data.error || 'Login failed. Check your credentials.';
          errEl.style.display = 'block';
          btn.disabled = false;
          btn.textContent = 'Connect';
        }}
      }} catch (err) {{
        errEl.textContent = 'Connection error. Please try again.';
        errEl.style.display = 'block';
        btn.disabled = false;
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
        )
    )


async def oauth_login(request: Request) -> JSONResponse:
    """Handle login form submission — authenticate with NB backend, complete OAuth."""
    try:
        body = await request.json()
        email = body.get("email", "").strip()
        password = body.get("password", "")
        auth_state = body.get("auth_state", "")

        if not email or not password or not auth_state:
            return JSONResponse({"error": "Missing required fields"}, status_code=400)

        # Authenticate against NobleBlocks backend
        async with httpx.AsyncClient(timeout=10.0) as client:
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
            nb_token = login_data.get("accessToken") or login_data.get("access_token", "")

        if not nb_token:
            return JSONResponse({"error": "Login succeeded but no token received"}, status_code=500)

        # Complete OAuth flow
        redirect_url = await oauth_provider.complete_authorization(auth_state, nb_token)
        return JSONResponse({"redirect_url": redirect_url})

    except Exception as e:
        logger.exception("OAuth login error")
        return JSONResponse({"error": "Internal error"}, status_code=500)


async def health_check(request: Request) -> JSONResponse:
    """Health check endpoint for ALB/ECS."""
    return JSONResponse({
        "status": "healthy",
        "service": "nobleblocks-mcp",
        "version": "2.0.0",
        "papers": "340M+",
    })


async def info_page(request: Request) -> HTMLResponse:
    """Root page with info about the MCP server."""
    return HTMLResponse("""<!DOCTYPE html>
<html><head><title>NobleBlocks MCP Server</title>
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
    # Get the MCP Starlette app (includes /mcp, /authorize, /token, etc.)
    mcp_app = mcp.streamable_http_app()

    # Add our custom routes (consent page, login handler, health)
    custom_routes = [
        Route("/", info_page, methods=["GET"]),
        Route("/health", health_check, methods=["GET"]),
        Route("/consent", consent_page, methods=["GET"]),
        Route("/oauth/login", oauth_login, methods=["POST"]),
    ]

    # Mount MCP app and add custom routes
    from starlette.routing import Mount
    routes = custom_routes + [Mount("/", app=mcp_app)]

    return Starlette(routes=routes)


app = create_app()


def main():
    """Run the remote MCP server."""
    import uvicorn
    logger.info(
        "NobleBlocks MCP v2.0.0 (Remote) | API: %s | URL: %s:%d/mcp",
        NB_API_BASE, HOST, PORT,
    )
    uvicorn.run(app, host=HOST, port=PORT)


if __name__ == "__main__":
    main()
