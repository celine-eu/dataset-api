from __future__ import annotations

import logging
from functools import lru_cache
from typing import Any, Optional

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import jwt, JWTError
import httpx

from celine.dataset.core.config import settings
from celine.dataset.security.models import AuthenticatedUser

logger = logging.getLogger(__name__)
bearer_scheme = HTTPBearer(auto_error=False)


# ---------------------------------------------------------------------
# JWKS handling
# ---------------------------------------------------------------------


@lru_cache
def _jwks_url() -> str:
    issuer = settings.oidc_issuer.rstrip("/")
    return f"{issuer}/protocol/openid-connect/certs"


@lru_cache
def _issuer() -> str:
    return settings.oidc_issuer.rstrip("/")


async def _get_jwks() -> dict:
    async with httpx.AsyncClient(timeout=5.0) as client:
        resp = await client.get(_jwks_url())
        resp.raise_for_status()
        return resp.json()


# ---------------------------------------------------------------------
# Core JWT validation
# ---------------------------------------------------------------------
async def _decode_token(token: str) -> dict[str, Any]:
    try:
        jwks = await _get_jwks()
        claims = jwt.decode(
            token,
            jwks,
            algorithms=["RS256"],
            issuer=_issuer(),
            options={"verify_aud": False},  # IMPORTANT
        )

        token_aud = claims.get("aud")
        expected = _expected_audiences()

        if token_aud is None:
            raise HTTPException(401, "Token missing audience")

        if isinstance(token_aud, str):
            token_aud = [token_aud]

        if not any(aud in expected for aud in token_aud):
            raise HTTPException(401, "Invalid audience")

        return claims

    except JWTError as exc:
        logger.debug("JWT validation failed: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token",
        )


def _expected_audiences() -> list[str]:
    auds = []

    if settings.oidc_audience:
        auds.append(settings.oidc_audience)

    if settings.oidc_client_id:
        auds.append(settings.oidc_client_id)

    # Keycloak default
    auds.append("account")

    return list(dict.fromkeys(auds))  # dedupe, preserve order


def _normalize_user(claims: dict[str, Any]) -> AuthenticatedUser:
    aud = claims.get("aud", [])
    if isinstance(aud, str):
        aud = [aud]

    realm_roles = claims.get("realm_access", {}).get("roles", [])
    client_roles = (
        claims.get("resource_access", {})
        .get(settings.oidc_client_id, {})
        .get("roles", [])
    )

    return AuthenticatedUser(
        sub=claims["sub"],
        username=claims.get("preferred_username"),
        email=claims.get("email"),
        roles=sorted(set(realm_roles + client_roles)),
        groups=claims.get("groups", []),
        issuer=claims.get("iss"),
        audiences=aud,
        claims=claims,
    )


# ---------------------------------------------------------------------
# Dependencies
# ---------------------------------------------------------------------
async def get_optional_user(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(bearer_scheme),
) -> Optional[AuthenticatedUser]:
    if credentials is None:
        return None

    claims = await _decode_token(credentials.credentials)
    return _normalize_user(claims)


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(HTTPBearer()),
) -> AuthenticatedUser:
    claims = await _decode_token(credentials.credentials)
    return _normalize_user(claims)
