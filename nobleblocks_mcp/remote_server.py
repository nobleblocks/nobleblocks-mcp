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
      <img src="data:image/png;base64,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" alt="NobleBlocks" width="240" height="40">
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
            nb_token = login_data.get("accessToken") or login_data.get("access_token", "")

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
            nb_token = login_data.get("accessToken") or login_data.get("access_token", "")

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
