"""
NobleBlocks OAuth 2.1 Provider for MCP
=======================================

Implements the OAuthAuthorizationServerProvider protocol so Claude (and other
MCP clients) can authenticate users via their NobleBlocks account.

Flow:
  1. Claude redirects user to our /authorize endpoint
  2. User sees a consent page and logs in (or is already logged in)
  3. We issue an authorization code → redirect back to Claude
  4. Claude exchanges the code for an access token via /token
  5. Claude includes the Bearer token on every MCP request
  6. We validate the token and execute tools on behalf of the user
"""

from __future__ import annotations

import os
import secrets
import time
import logging
from datetime import datetime, timezone, timedelta
from urllib.parse import urlencode

import httpx
from mcp.server.auth.provider import (
    OAuthAuthorizationServerProvider,
    AuthorizationCode,
    AccessToken,
    RefreshToken,
    AuthorizationParams,
    AuthorizeError,
    AuthorizationErrorCode,
    TokenError,
    TokenErrorCode,
    RegistrationError,
    RegistrationErrorCode,
    construct_redirect_uri,
)
from mcp.shared.auth import OAuthClientInformationFull, OAuthToken

logger = logging.getLogger("nobleblocks-mcp.oauth")

# ─── Configuration ─────────────────────────────────────────────────────────────
NB_API_BASE = os.environ.get("NOBLEBLOCKS_API_BASE", "https://www.nobleblocks.com").rstrip("/")
MCP_BASE_URL = os.environ.get("MCP_BASE_URL", "https://mcp.nobleblocks.com").rstrip("/")
CONSENT_PAGE_URL = os.environ.get("CONSENT_PAGE_URL", f"{MCP_BASE_URL}/consent")

# How long tokens live
AUTH_CODE_TTL = 300  # 5 minutes
ACCESS_TOKEN_TTL = 3600 * 24  # 24 hours
REFRESH_TOKEN_TTL = 3600 * 24 * 30  # 30 days

# Known clients — Claude's client_id is registered by Anthropic
# We also support dynamic client registration for other MCP clients
KNOWN_CLIENTS: dict[str, dict] = {}


class NobleBlocksOAuthProvider(
    OAuthAuthorizationServerProvider[AuthorizationCode, AccessToken, RefreshToken]
):
    """OAuth 2.1 provider that authenticates against NobleBlocks user accounts."""

    def __init__(self):
        # In-memory stores (replace with Redis/DB for multi-instance production)
        self._auth_codes: dict[str, AuthorizationCode] = {}
        self._access_tokens: dict[str, AccessToken] = {}
        self._refresh_tokens: dict[str, RefreshToken] = {}
        self._clients: dict[str, OAuthClientInformationFull] = {}
        # Map auth codes to NB session tokens
        self._code_to_nb_token: dict[str, str] = {}
        # Map access tokens to NB session tokens
        self._token_to_nb_token: dict[str, str] = {}
        # Pending auth requests (state → params)
        self._pending_auth: dict[str, dict] = {}

    # ── Client Registration ──────────────────────────────────────────────────

    async def get_client(self, client_id: str) -> OAuthClientInformationFull | None:
        """Look up a registered client by ID."""
        return self._clients.get(client_id)

    async def register_client(
        self, client_info: OAuthClientInformationFull
    ) -> None:
        """Register a new OAuth client (Dynamic Client Registration)."""
        if client_info.client_id in self._clients:
            raise RegistrationError(
                error=RegistrationErrorCode.INVALID_CLIENT_METADATA,
                error_description="Client already registered",
            )
        self._clients[client_info.client_id] = client_info
        logger.info("Registered OAuth client: %s (%s)", client_info.client_id, client_info.client_name)

    # ── Authorization ────────────────────────────────────────────────────────

    async def authorize(
        self,
        client: OAuthClientInformationFull,
        params: AuthorizationParams,
    ) -> str:
        """
        Start the authorization flow.
        Returns a URL to redirect the user to for consent/login.

        The consent page will:
        1. Show "Connect NobleBlocks to Claude"
        2. Ask user to log in (or auto-detect existing session)
        3. POST back to /oauth/callback with the NB session token
        4. We issue an auth code and redirect to Claude's redirect_uri
        """
        # Generate a unique state for this auth request
        auth_state = secrets.token_urlsafe(32)

        # Store the pending request
        self._pending_auth[auth_state] = {
            "client_id": client.client_id,
            "client_name": client.client_name or "Unknown App",
            "params": params,
            "created_at": time.time(),
        }

        # Redirect to our consent page
        consent_url = f"{CONSENT_PAGE_URL}?" + urlencode({
            "auth_state": auth_state,
            "client_name": client.client_name or "Unknown App",
            "scopes": ",".join(params.scopes) if params.scopes else "search",
        })
        return consent_url

    async def complete_authorization(
        self, auth_state: str, nb_access_token: str
    ) -> str:
        """
        Called after the user consents and logs in.
        Validates the NB session, issues an auth code, and returns the
        redirect URL back to Claude.
        """
        pending = self._pending_auth.pop(auth_state, None)
        if not pending:
            raise AuthorizeError(
                error=AuthorizationErrorCode.INVALID_REQUEST,
                error_description="Invalid or expired authorization state",
            )

        # Verify the NB access token is valid
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(
                f"{NB_API_BASE}/api/v1/user/me",
                headers={"Authorization": f"Bearer {nb_access_token}"},
            )
            if resp.status_code != 200:
                raise AuthorizeError(
                    error=AuthorizationErrorCode.ACCESS_DENIED,
                    error_description="Invalid NobleBlocks session",
                )

        params: AuthorizationParams = pending["params"]

        # Generate authorization code
        code_str = secrets.token_urlsafe(48)
        auth_code = AuthorizationCode(
            code=code_str,
            scopes=params.scopes or ["search"],
            expires_at=datetime.now(timezone.utc) + timedelta(seconds=AUTH_CODE_TTL),
            client_id=pending["client_id"],
            code_challenge=params.code_challenge,
            redirect_uri=params.redirect_uri,
            redirect_uri_provided_explicitly=params.redirect_uri_provided_explicitly,
            resource=params.resource,
        )

        self._auth_codes[code_str] = auth_code
        self._code_to_nb_token[code_str] = nb_access_token

        # Build redirect back to Claude
        redirect_url = construct_redirect_uri(
            str(params.redirect_uri),
            code=code_str,
            state=params.state,
        )
        return redirect_url

    # ── Token Exchange ───────────────────────────────────────────────────────

    async def load_authorization_code(
        self,
        client: OAuthClientInformationFull,
        authorization_code: str,
    ) -> AuthorizationCode | None:
        code = self._auth_codes.get(authorization_code)
        if code and code.client_id == client.client_id:
            if code.expires_at > datetime.now(timezone.utc):
                return code
            # Expired — clean up
            self._auth_codes.pop(authorization_code, None)
            self._code_to_nb_token.pop(authorization_code, None)
        return None

    async def exchange_authorization_code(
        self,
        client: OAuthClientInformationFull,
        authorization_code: AuthorizationCode,
    ) -> OAuthToken:
        code_str = authorization_code.code

        # Remove used code (one-time use)
        self._auth_codes.pop(code_str, None)
        nb_token = self._code_to_nb_token.pop(code_str, "")

        # Issue access token
        access_token_str = secrets.token_urlsafe(48)
        access_token = AccessToken(
            token=access_token_str,
            client_id=client.client_id,
            scopes=authorization_code.scopes,
            expires_at=datetime.now(timezone.utc) + timedelta(seconds=ACCESS_TOKEN_TTL),
            resource=authorization_code.resource,
        )
        self._access_tokens[access_token_str] = access_token
        self._token_to_nb_token[access_token_str] = nb_token

        # Issue refresh token
        refresh_token_str = secrets.token_urlsafe(48)
        refresh_token = RefreshToken(
            token=refresh_token_str,
            client_id=client.client_id,
            scopes=authorization_code.scopes,
            expires_at=datetime.now(timezone.utc) + timedelta(seconds=REFRESH_TOKEN_TTL),
        )
        self._refresh_tokens[refresh_token_str] = refresh_token

        return OAuthToken(
            access_token=access_token_str,
            token_type="Bearer",
            expires_in=ACCESS_TOKEN_TTL,
            refresh_token=refresh_token_str,
            scope=" ".join(authorization_code.scopes) if authorization_code.scopes else "search",
        )

    # ── Refresh Token ────────────────────────────────────────────────────────

    async def load_refresh_token(
        self,
        client: OAuthClientInformationFull,
        refresh_token: str,
    ) -> RefreshToken | None:
        rt = self._refresh_tokens.get(refresh_token)
        if rt and rt.client_id == client.client_id:
            if rt.expires_at > datetime.now(timezone.utc):
                return rt
            self._refresh_tokens.pop(refresh_token, None)
        return None

    async def exchange_refresh_token(
        self,
        client: OAuthClientInformationFull,
        refresh_token: RefreshToken,
        scopes: list[str],
    ) -> OAuthToken:
        # Rotate refresh token (revoke old, issue new)
        self._refresh_tokens.pop(refresh_token.token, None)

        # Find the NB token associated with the old access token for this client
        nb_token = ""
        for at_str, at in list(self._access_tokens.items()):
            if at.client_id == client.client_id:
                nb_token = self._token_to_nb_token.get(at_str, "")
                break

        # Issue new access token
        access_token_str = secrets.token_urlsafe(48)
        access_token = AccessToken(
            token=access_token_str,
            client_id=client.client_id,
            scopes=scopes or refresh_token.scopes,
            expires_at=datetime.now(timezone.utc) + timedelta(seconds=ACCESS_TOKEN_TTL),
        )
        self._access_tokens[access_token_str] = access_token
        if nb_token:
            self._token_to_nb_token[access_token_str] = nb_token

        # Issue new refresh token
        new_refresh_str = secrets.token_urlsafe(48)
        new_refresh = RefreshToken(
            token=new_refresh_str,
            client_id=client.client_id,
            scopes=scopes or refresh_token.scopes,
            expires_at=datetime.now(timezone.utc) + timedelta(seconds=REFRESH_TOKEN_TTL),
        )
        self._refresh_tokens[new_refresh_str] = new_refresh

        return OAuthToken(
            access_token=access_token_str,
            token_type="Bearer",
            expires_in=ACCESS_TOKEN_TTL,
            refresh_token=new_refresh_str,
            scope=" ".join(scopes or refresh_token.scopes),
        )

    # ── Token Validation ─────────────────────────────────────────────────────

    async def load_access_token(self, token: str) -> AccessToken | None:
        at = self._access_tokens.get(token)
        if at and at.expires_at > datetime.now(timezone.utc):
            return at
        if at:
            self._access_tokens.pop(token, None)
            self._token_to_nb_token.pop(token, None)
        return None

    # ── Revocation ───────────────────────────────────────────────────────────

    async def revoke_token(
        self, token: AccessToken | RefreshToken
    ) -> None:
        if isinstance(token, AccessToken):
            self._access_tokens.pop(token.token, None)
            self._token_to_nb_token.pop(token.token, None)
        elif isinstance(token, RefreshToken):
            self._refresh_tokens.pop(token.token, None)

    # ── Helper: get NB token for MCP request ─────────────────────────────────

    def get_nb_token(self, mcp_access_token: str) -> str | None:
        """Get the NobleBlocks API token associated with an MCP access token."""
        return self._token_to_nb_token.get(mcp_access_token)
