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
GOOGLE_CLIENT_ID = "351535713791-lkhg858q5637b05no5f2pu8hp47470rm.apps.googleusercontent.com"

CONSENT_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Connect NobleBlocks to {client_name}</title>
  <script src="https://accounts.google.com/gsi/client" async defer></script>
  <style>
    * {{ margin: 0; padding: 0; box-sizing: border-box; }}
    body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
           background: #f8fafc; display: flex; justify-content: center; align-items: center;
           min-height: 100vh; padding: 20px; }}
    .card {{ background: white; border-radius: 16px; box-shadow: 0 4px 24px rgba(0,0,0,0.08);
             max-width: 440px; width: 100%; padding: 40px; text-align: center; }}
    .logo {{ width: 64px; height: 64px; margin: 0 auto 16px; }}
    .logo img {{ width: 100%; height: 100%; object-fit: contain; }}
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
                   padding: 12px; background: white; color: #1f2937; border: 1px solid #e2e8f0;
                   border-radius: 8px; font-size: 15px; font-weight: 500; cursor: pointer;
                   transition: background 0.2s, box-shadow 0.2s; gap: 10px; }}
    .btn-google:hover {{ background: #f9fafb; box-shadow: 0 1px 3px rgba(0,0,0,0.08); }}
    .btn-google:disabled {{ opacity: 0.6; cursor: not-allowed; }}
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
    .footer {{ font-size: 12px; color: #94a3b8; margin-top: 16px; }}
    .footer a {{ color: #6366f1; text-decoration: none; }}
  </style>
</head>
<body>
  <div class="card">
    <div class="logo">
      <img src="data:image/png;base64,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" alt="NobleBlocks" width="64" height="64">
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
          btn.innerHTML = '<svg viewBox="0 0 24 24" width="20" height="20"><path fill="#4285F4" d="M22.56 12.25c0-.78-.07-1.53-.2-2.25H12v4.26h5.92c-.26 1.37-1.04 2.53-2.21 3.31v2.77h3.57c2.08-1.92 3.28-4.74 3.28-8.09z"/><path fill="#34A853" d="M12 23c2.97 0 5.46-.98 7.28-2.66l-3.57-2.77c-.98.66-2.23 1.06-3.71 1.06-2.86 0-5.29-1.93-6.16-4.53H2.18v2.84C3.99 20.53 7.7 23 12 23z"/><path fill="#FBBC05" d="M5.84 14.09c-.22-.66-.35-1.36-.35-2.09s.13-1.43.35-2.09V7.07H2.18C1.43 8.55 1 10.22 1 12s.43 3.45 1.18 4.93l2.85-2.22.81-.62z"/><path fill="#EA4335" d="M12 5.38c1.62 0 3.06.56 4.21 1.64l3.15-3.15C17.45 2.09 14.97 1 12 1 7.7 1 3.99 3.47 2.18 7.07l3.66 2.84c.87-2.6 3.3-4.53 6.16-4.53z"/></svg> Continue with Google';
        }}
      }} catch (err) {{
        const errEl = document.getElementById('errorMsg');
        errEl.textContent = 'Connection error. Please try again.';
        errEl.style.display = 'block';
        btn.disabled = false;
        btn.innerHTML = '<svg viewBox="0 0 24 24" width="20" height="20"><path fill="#4285F4" d="M22.56 12.25c0-.78-.07-1.53-.2-2.25H12v4.26h5.92c-.26 1.37-1.04 2.53-2.21 3.31v2.77h3.57c2.08-1.92 3.28-4.74 3.28-8.09z"/><path fill="#34A853" d="M12 23c2.97 0 5.46-.98 7.28-2.66l-3.57-2.77c-.98.66-2.23 1.06-3.71 1.06-2.86 0-5.29-1.93-6.16-4.53H2.18v2.84C3.99 20.53 7.7 23 12 23z"/><path fill="#FBBC05" d="M5.84 14.09c-.22-.66-.35-1.36-.35-2.09s.13-1.43.35-2.09V7.07H2.18C1.43 8.55 1 10.22 1 12s.43 3.45 1.18 4.93l2.85-2.22.81-.62z"/><path fill="#EA4335" d="M12 5.38c1.62 0 3.06.56 4.21 1.64l3.15-3.15C17.45 2.09 14.97 1 12 1 7.7 1 3.99 3.47 2.18 7.07l3.66 2.84c.87-2.6 3.3-4.53 6.16-4.53z"/></svg> Continue with Google';
      }}
    }}

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
            google_client_id=GOOGLE_CLIENT_ID,
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
            token_obj = login_data.get("token", {})
            nb_token = token_obj.get("token", "") if isinstance(token_obj, dict) else ""

        if not nb_token:
            return JSONResponse({"error": "Login succeeded but no token received"}, status_code=500)

        # Complete OAuth flow
        redirect_url = await oauth_provider.complete_authorization(auth_state, nb_token)
        return JSONResponse({"redirect_url": redirect_url})

    except Exception as e:
        logger.exception("OAuth login error")
        return JSONResponse({"error": "Internal error"}, status_code=500)


async def oauth_login_google(request: Request) -> JSONResponse:
    """Handle Google login — exchange Google access token for NB token, complete OAuth."""
    try:
        body = await request.json()
        access_token = body.get("access_token", "").strip()
        auth_state = body.get("auth_state", "")

        if not access_token or not auth_state:
            return JSONResponse({"error": "Missing required fields"}, status_code=400)

        # Authenticate against NobleBlocks backend with Google token
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(
                f"{NB_API_BASE}/api/v1/auth/login_with_google",
                json={"accessToken": access_token},
            )
            if resp.status_code != 200:
                error_msg = "Google authentication failed"
                try:
                    err_data = resp.json()
                    if "message" in err_data:
                        error_msg = err_data["message"]
                except Exception:
                    pass
                return JSONResponse({"error": error_msg}, status_code=401)
            login_data = resp.json()
            token_obj = login_data.get("token", {})
            nb_token = token_obj.get("token", "") if isinstance(token_obj, dict) else ""

        if not nb_token:
            return JSONResponse({"error": "Login succeeded but no token received"}, status_code=500)

        # Complete OAuth flow
        redirect_url = await oauth_provider.complete_authorization(auth_state, nb_token)
        return JSONResponse({"redirect_url": redirect_url})

    except Exception as e:
        logger.exception("OAuth Google login error")
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
        Route("/oauth/login/google", oauth_login_google, methods=["POST"]),
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
