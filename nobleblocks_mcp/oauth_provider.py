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
        # Persistent store (DynamoDB) — survives restarts and works multi-instance.
        # Falls back to in-memory dicts if DynamoDB is unavailable (local dev).
        self._use_dynamo = False
        self._store = None
        try:
            from .dynamo_store import DynamoTokenStore, ensure_table_exists
            ensure_table_exists()
            self._store = DynamoTokenStore()
            self._use_dynamo = True
            logger.info("OAuth token store: DynamoDB")
        except Exception as e:
            logger.warning("DynamoDB unavailable, falling back to in-memory: %s", e)

        # In-memory fallback (used if DynamoDB is unavailable)
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

    # ── Persistence helpers ──────────────────────────────────────────────────

    def _save_auth_code(self, code_str: str, auth_code: AuthorizationCode):
        self._auth_codes[code_str] = auth_code
        if self._use_dynamo:
            self._store.put("auth_code", code_str, {
                "code": auth_code.code,
                "scopes": auth_code.scopes,
                "expires_at": str(auth_code.expires_at),
                "client_id": auth_code.client_id,
                "code_challenge": auth_code.code_challenge,
                "redirect_uri": str(auth_code.redirect_uri) if auth_code.redirect_uri else None,
                "redirect_uri_provided_explicitly": auth_code.redirect_uri_provided_explicitly,
                "resource": str(auth_code.resource) if auth_code.resource else None,
            }, ttl_epoch=int(auth_code.expires_at))

    def _load_auth_code(self, code_str: str) -> AuthorizationCode | None:
        if code_str in self._auth_codes:
            return self._auth_codes[code_str]
        if self._use_dynamo:
            data = self._store.get("auth_code", code_str)
            if data:
                from datetime import datetime
                from pydantic import AnyHttpUrl
                expires_raw = data["expires_at"]
                if isinstance(expires_raw, str):
                    try:
                        expires_ts = float(expires_raw)
                    except ValueError:
                        expires_ts = datetime.fromisoformat(expires_raw).timestamp()
                else:
                    expires_ts = float(expires_raw)
                ac = AuthorizationCode(
                    code=data["code"],
                    scopes=data.get("scopes", []),
                    expires_at=expires_ts,
                    client_id=data["client_id"],
                    code_challenge=data.get("code_challenge"),
                    redirect_uri=AnyHttpUrl(data["redirect_uri"]) if data.get("redirect_uri") else None,
                    redirect_uri_provided_explicitly=data.get("redirect_uri_provided_explicitly", False),
                    resource=str(data["resource"]) if data.get("resource") else None,
                )
                self._auth_codes[code_str] = ac
                return ac
        return None

    def _delete_auth_code(self, code_str: str):
        self._auth_codes.pop(code_str, None)
        if self._use_dynamo:
            self._store.delete("auth_code", code_str)

    def _save_access_token(self, token_str: str, access_token: AccessToken):
        self._access_tokens[token_str] = access_token
        if self._use_dynamo:
            self._store.put("access_token", token_str, {
                "token": access_token.token,
                "client_id": access_token.client_id,
                "scopes": access_token.scopes,
                "expires_at": str(access_token.expires_at),
                "resource": str(access_token.resource) if access_token.resource else None,
            }, ttl_epoch=int(access_token.expires_at))

    def _load_access_token(self, token_str: str) -> AccessToken | None:
        if token_str in self._access_tokens:
            return self._access_tokens[token_str]
        if self._use_dynamo:
            data = self._store.get("access_token", token_str)
            if not data:
                logger.warning("_load_access_token: DynamoDB returned None for token=%.12s...", token_str)
            if data:
                from datetime import datetime
                expires_raw = data["expires_at"]
                if isinstance(expires_raw, str):
                    try:
                        expires_ts = int(expires_raw)
                    except ValueError:
                        expires_ts = int(datetime.fromisoformat(expires_raw).timestamp())
                else:
                    expires_ts = int(expires_raw)
                at = AccessToken(
                    token=data["token"],
                    client_id=data["client_id"],
                    scopes=data.get("scopes", []),
                    expires_at=expires_ts,
                    resource=str(data["resource"]) if data.get("resource") else None,
                )
                self._access_tokens[token_str] = at
                return at
        return None

    def _delete_access_token(self, token_str: str):
        self._access_tokens.pop(token_str, None)
        if self._use_dynamo:
            self._store.delete("access_token", token_str)

    def _save_refresh_token(self, token_str: str, refresh_token: RefreshToken):
        self._refresh_tokens[token_str] = refresh_token
        if self._use_dynamo:
            self._store.put("refresh_token", token_str, {
                "token": refresh_token.token,
                "client_id": refresh_token.client_id,
                "scopes": refresh_token.scopes,
                "expires_at": str(refresh_token.expires_at),
            }, ttl_epoch=int(refresh_token.expires_at))

    def _load_refresh_token(self, token_str: str) -> RefreshToken | None:
        if token_str in self._refresh_tokens:
            return self._refresh_tokens[token_str]
        if self._use_dynamo:
            data = self._store.get("refresh_token", token_str)
            if data:
                from datetime import datetime
                expires_raw = data["expires_at"]
                if isinstance(expires_raw, str):
                    try:
                        expires_ts = int(expires_raw)
                    except ValueError:
                        expires_ts = int(datetime.fromisoformat(expires_raw).timestamp())
                else:
                    expires_ts = int(expires_raw)
                rt = RefreshToken(
                    token=data["token"],
                    client_id=data["client_id"],
                    scopes=data.get("scopes", []),
                    expires_at=expires_ts,
                )
                self._refresh_tokens[token_str] = rt
                return rt
        return None

    def _delete_refresh_token(self, token_str: str):
        self._refresh_tokens.pop(token_str, None)
        if self._use_dynamo:
            self._store.delete("refresh_token", token_str)

    def _save_nb_token_mapping(self, map_type: str, key: str, nb_token: str):
        if map_type == "code":
            self._code_to_nb_token[key] = nb_token
        else:
            self._token_to_nb_token[key] = nb_token
        if self._use_dynamo:
            # No TTL — these get cleaned up when the parent token is deleted
            self._store.put(f"nb_map_{map_type}", key, {"nb_token": nb_token})

    def _load_nb_token_mapping(self, map_type: str, key: str) -> str:
        local = self._code_to_nb_token if map_type == "code" else self._token_to_nb_token
        if key in local:
            return local[key]
        if self._use_dynamo:
            data = self._store.get(f"nb_map_{map_type}", key)
            if data:
                local[key] = data["nb_token"]
                return data["nb_token"]
        return ""

    def _delete_nb_token_mapping(self, map_type: str, key: str):
        local = self._code_to_nb_token if map_type == "code" else self._token_to_nb_token
        local.pop(key, None)
        if self._use_dynamo:
            self._store.delete(f"nb_map_{map_type}", key)

    def _save_client(self, client_id: str, client_info: OAuthClientInformationFull):
        self._clients[client_id] = client_info
        if self._use_dynamo:
            self._store.put("client", client_id, client_info.model_dump(mode="json"))

    def _load_client(self, client_id: str) -> OAuthClientInformationFull | None:
        if client_id in self._clients:
            return self._clients[client_id]
        if self._use_dynamo:
            data = self._store.get("client", client_id)
            if data:
                ci = OAuthClientInformationFull.model_validate(data)
                self._clients[client_id] = ci
                return ci
        return None

    def _save_pending_auth(self, state: str, data: dict):
        self._pending_auth[state] = data
        if self._use_dynamo:
            ttl = int(time.time()) + AUTH_CODE_TTL
            # Serialize params for storage
            stored = {**data}
            if "params" in stored:
                p = stored["params"]
                stored["params"] = {
                    "state": p.state,
                    "scopes": p.scopes,
                    "code_challenge": p.code_challenge,
                    "redirect_uri": str(p.redirect_uri) if p.redirect_uri else None,
                    "redirect_uri_provided_explicitly": p.redirect_uri_provided_explicitly,
                    "resource": str(p.resource) if p.resource else None,
                }
            self._store.put("pending_auth", state, stored, ttl_epoch=ttl)

    def _load_and_delete_pending_auth(self, state: str) -> dict | None:
        data = self._pending_auth.pop(state, None)
        if not data and self._use_dynamo:
            data = self._store.get("pending_auth", state)
            if data and "params" in data and isinstance(data["params"], dict):
                from pydantic import AnyHttpUrl
                p = data["params"]
                data["params"] = AuthorizationParams(
                    state=p.get("state"),
                    scopes=p.get("scopes"),
                    code_challenge=p.get("code_challenge"),
                    redirect_uri=AnyHttpUrl(p["redirect_uri"]) if p.get("redirect_uri") else None,
                    redirect_uri_provided_explicitly=p.get("redirect_uri_provided_explicitly", False),
                    resource=AnyHttpUrl(p["resource"]) if p.get("resource") else None,
                )
        if self._use_dynamo:
            self._store.delete("pending_auth", state)
        return data

    # ── Client Registration ──────────────────────────────────────────────────

    async def get_client(self, client_id: str) -> OAuthClientInformationFull | None:
        """Look up a registered client by ID."""
        return self._load_client(client_id)

    async def register_client(
        self, client_info: OAuthClientInformationFull
    ) -> None:
        """Register a new OAuth client (Dynamic Client Registration)."""
        if self._load_client(client_info.client_id):
            raise RegistrationError(
                error="invalid_client_metadata",
                error_description="Client already registered",
            )
        self._save_client(client_info.client_id, client_info)
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
        self._save_pending_auth(auth_state, {
            "client_id": client.client_id,
            "client_name": client.client_name or "Unknown App",
            "params": params,
            "created_at": time.time(),
        })

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
        pending = self._load_and_delete_pending_auth(auth_state)
        if not pending:
            raise AuthorizeError(
                error="invalid_request",
                error_description="Invalid or expired authorization state",
            )

        # Verify the NB access token is valid
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(
                f"{NB_API_BASE}/api/v1/user/2fa/status",
                headers={"Authorization": f"Bearer {nb_access_token}"},
            )
            if resp.status_code != 200:
                raise AuthorizeError(
                    error="access_denied",
                    error_description="Invalid NobleBlocks session",
                )

        params: AuthorizationParams = pending["params"]

        # Generate authorization code
        code_str = secrets.token_urlsafe(48)
        auth_code = AuthorizationCode(
            code=code_str,
            scopes=params.scopes or ["search"],
            expires_at=(datetime.now(timezone.utc) + timedelta(seconds=AUTH_CODE_TTL)).timestamp(),
            client_id=pending["client_id"],
            code_challenge=params.code_challenge,
            redirect_uri=params.redirect_uri,
            redirect_uri_provided_explicitly=params.redirect_uri_provided_explicitly,
            resource=params.resource,
        )

        self._save_auth_code(code_str, auth_code)
        self._save_nb_token_mapping("code", code_str, nb_access_token)

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
        code = self._load_auth_code(authorization_code)
        if code and code.client_id == client.client_id:
            if code.expires_at > time.time():
                return code
            # Expired — clean up
            self._delete_auth_code(authorization_code)
            self._delete_nb_token_mapping("code", authorization_code)
        return None

    async def exchange_authorization_code(
        self,
        client: OAuthClientInformationFull,
        authorization_code: AuthorizationCode,
    ) -> OAuthToken:
        code_str = authorization_code.code

        # Remove used code (one-time use)
        self._delete_auth_code(code_str)
        nb_token = self._load_nb_token_mapping("code", code_str)
        self._delete_nb_token_mapping("code", code_str)

        # Issue access token
        access_token_str = secrets.token_urlsafe(48)
        access_token = AccessToken(
            token=access_token_str,
            client_id=client.client_id,
            scopes=authorization_code.scopes,
            expires_at=int((datetime.now(timezone.utc) + timedelta(seconds=ACCESS_TOKEN_TTL)).timestamp()),
            resource=authorization_code.resource,
        )
        self._save_access_token(access_token_str, access_token)
        self._save_nb_token_mapping("token", access_token_str, nb_token)

        # Issue refresh token
        refresh_token_str = secrets.token_urlsafe(48)
        refresh_token = RefreshToken(
            token=refresh_token_str,
            client_id=client.client_id,
            scopes=authorization_code.scopes,
            expires_at=int((datetime.now(timezone.utc) + timedelta(seconds=REFRESH_TOKEN_TTL)).timestamp()),
        )
        self._save_refresh_token(refresh_token_str, refresh_token)

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
        rt = self._load_refresh_token(refresh_token)
        if rt and rt.client_id == client.client_id:
            if rt.expires_at > time.time():
                return rt
            self._delete_refresh_token(refresh_token)
        return None

    async def exchange_refresh_token(
        self,
        client: OAuthClientInformationFull,
        refresh_token: RefreshToken,
        scopes: list[str],
    ) -> OAuthToken:
        # Rotate refresh token (revoke old, issue new)
        self._delete_refresh_token(refresh_token.token)

        # Find the NB token associated with the old access token for this client
        nb_token = ""
        for at_str, at in list(self._access_tokens.items()):
            if at.client_id == client.client_id:
                nb_token = self._load_nb_token_mapping("token", at_str)
                break

        # Issue new access token
        access_token_str = secrets.token_urlsafe(48)
        access_token = AccessToken(
            token=access_token_str,
            client_id=client.client_id,
            scopes=scopes or refresh_token.scopes,
            expires_at=int((datetime.now(timezone.utc) + timedelta(seconds=ACCESS_TOKEN_TTL)).timestamp()),
        )
        self._save_access_token(access_token_str, access_token)
        if nb_token:
            self._save_nb_token_mapping("token", access_token_str, nb_token)

        # Issue new refresh token
        new_refresh_str = secrets.token_urlsafe(48)
        new_refresh = RefreshToken(
            token=new_refresh_str,
            client_id=client.client_id,
            scopes=scopes or refresh_token.scopes,
            expires_at=int((datetime.now(timezone.utc) + timedelta(seconds=REFRESH_TOKEN_TTL)).timestamp()),
        )
        self._save_refresh_token(new_refresh_str, new_refresh)

        return OAuthToken(
            access_token=access_token_str,
            token_type="Bearer",
            expires_in=ACCESS_TOKEN_TTL,
            refresh_token=new_refresh_str,
            scope=" ".join(scopes or refresh_token.scopes),
        )

    # ── Token Validation ─────────────────────────────────────────────────────

    async def load_access_token(self, token: str) -> AccessToken | None:
        in_memory = token in self._access_tokens
        at = self._load_access_token(token)
        if at and at.expires_at > time.time():
            logger.info("load_access_token: FOUND token=%.12s... memory=%s expires_in=%.0fs",
                        token, in_memory, at.expires_at - time.time())
            return at
        if at:
            logger.warning("load_access_token: EXPIRED token=%.12s... expires_at=%s now=%s",
                           token, at.expires_at, time.time())
            self._delete_access_token(token)
            self._delete_nb_token_mapping("token", token)
        else:
            logger.warning("load_access_token: NOT FOUND token=%.12s... memory=%s dynamo=%s",
                           token, in_memory, self._use_dynamo)
        return None

    # ── Revocation ───────────────────────────────────────────────────────────

    async def revoke_token(
        self, token: AccessToken | RefreshToken
    ) -> None:
        if isinstance(token, AccessToken):
            self._delete_access_token(token.token)
            self._delete_nb_token_mapping("token", token.token)
        elif isinstance(token, RefreshToken):
            self._delete_refresh_token(token.token)

    # ── Helper: get NB token for MCP request ─────────────────────────────────

    def get_nb_token(self, mcp_access_token: str) -> str | None:
        """Get the NobleBlocks API token associated with an MCP access token."""
        t = self._load_nb_token_mapping("token", mcp_access_token)
        return t if t else None
