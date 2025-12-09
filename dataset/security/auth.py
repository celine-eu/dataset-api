# dataset/security/auth.py

from __future__ import annotations

import logging
from typing import Any, Optional

import httpx
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import jwt, JWTError

from dataset.core.config import settings

logger = logging.getLogger(__name__)


bearer_scheme = HTTPBearer(auto_error=False)

_JWKS_CACHE: dict[str, Any] = {}


async def _fetch_jwks(issuer: str) -> dict[str, Any]:
    if issuer in _JWKS_CACHE:
        return _JWKS_CACHE[issuer]

    # Keycloak JWKS endpoint
    jwks_url = f"{issuer.rstrip('/')}/protocol/openid-connect/certs"

    async with httpx.AsyncClient(timeout=5.0) as client:
        resp = await client.get(jwks_url)
        resp.raise_for_status()
        jwks = resp.json()

    _JWKS_CACHE[issuer] = jwks
    return jwks


async def _decode_keycloak_jwt(token: str) -> dict[str, Any]:
    if jwt is None:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="JWT validation library not installed (python-jose).",
        )

    if not settings.keycloak_issuer:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Keycloak issuer not configured.",
        )

    issuer = str(settings.keycloak_issuer).rstrip("/")
    jwks = await _fetch_jwks(issuer)

    try:
        header = jwt.get_unverified_header(token)
        kid = header.get("kid")
    except JWTError as exc:
        logger.warning("Invalid JWT header: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token header.",
        )

    key = None
    for k in jwks.get("keys", []):
        if k.get("kid") == kid:
            key = k
            break

    if key is None:
        logger.warning("No matching JWK for kid=%s", kid)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token key not recognized.",
        )

    # jose can use the JWK dict directly
    try:
        claims = jwt.decode(
            token,
            key,
            algorithms=[key.get("alg", "RS256")],
            audience=settings.keycloak_audience,
            issuer=issuer,
        )
    except JWTError as exc:
        logger.warning("JWT validation failed: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token."
        )

    return claims


async def get_optional_user(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(bearer_scheme),
) -> Optional[dict[str, Any]]:
    """
    Try to authenticate the user, but return None if no Authorization header.

    Used so that "open data" datasets can be queried without a token.
    """
    if credentials is None:
        return None

    token = credentials.credentials
    return await _decode_keycloak_jwt(token)


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(HTTPBearer()),
) -> dict[str, Any]:
    """
    Strict version: always requires a valid token.
    """
    return await _decode_keycloak_jwt(credentials.credentials)
