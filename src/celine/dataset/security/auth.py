from __future__ import annotations

import logging
from typing import Optional

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from celine.dataset.core.config import settings
from celine.dataset.security.models import AuthenticatedUser

# Use celine.sdk for JWT validation
from celine.sdk.auth import JwtUser

logger = logging.getLogger(__name__)
bearer_scheme = HTTPBearer(auto_error=False)


# ---------------------------------------------------------------------
# Core JWT validation using celine.sdk
# ---------------------------------------------------------------------


async def _decode_and_validate_token(token: str) -> JwtUser:
    """
    Decode and validate JWT token using celine.sdk.auth.

    Args:
        token: JWT token string

    Returns:
        JwtUser with validated claims

    Raises:
        HTTPException: 401 if token is invalid
    """
    try:
        # Use celine.sdk.auth.JwtUser for validation
        user = JwtUser.from_token(
            token,
            verify=True,  # Always verify signatures
            jwks_uri=settings.oidc_jwks_uri,
            issuer=settings.oidc_issuer,
            audience=_get_expected_audiences(),
            algorithms=["RS256"],
        )
        return user

    except ValueError as exc:
        # JwtUser raises ValueError for validation errors
        logger.debug("JWT validation failed: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token",
        ) from exc
    except Exception as exc:
        logger.error("Unexpected JWT validation error: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token validation failed",
        ) from exc


def _get_expected_audiences() -> list[str] | str | None:
    """
    Get expected audience(s) for token validation.

    Returns:
        List of audience strings, single string, or None to skip aud validation
    """
    audiences = []

    if settings.oidc_audience:
        audiences.append(settings.oidc_audience)

    if settings.oidc_client_id:
        audiences.append(settings.oidc_client_id)

    # Keycloak default
    audiences.append("account")

    # Return list if we have audiences, None to skip validation
    return audiences if audiences else None


def _normalize_user(jwt_user: JwtUser) -> AuthenticatedUser:
    """
    Convert JwtUser from celine.sdk to our AuthenticatedUser model.

    Args:
        jwt_user: JwtUser from celine.sdk

    Returns:
        AuthenticatedUser with dataset-specific fields
    """
    # Extract audience(s)
    aud = jwt_user.claims.get("aud", [])
    if isinstance(aud, str):
        aud = [aud]

    # Extract realm roles
    realm_roles = jwt_user.claims.get("realm_access", {}).get("roles", [])

    # Extract client-specific roles
    client_roles = (
        jwt_user.claims.get("resource_access", {})
        .get(settings.oidc_client_id, {})
        .get("roles", [])
    )

    # Extract groups
    groups = jwt_user.claims.get("groups", [])

    # Extract scopes
    scopes = jwt_user.claims.get("scope", "")
    if isinstance(scopes, str):
        scopes = scopes.split()
    elif not isinstance(scopes, list):
        scopes = []

    return AuthenticatedUser(
        sub=jwt_user.sub,
        username=jwt_user.preferred_username or jwt_user.email,
        email=jwt_user.email,
        roles=sorted(set(realm_roles + client_roles)),
        groups=groups,
        issuer=jwt_user.iss,
        scopes=scopes,
        audiences=aud,
        claims=jwt_user.claims,
    )


# ---------------------------------------------------------------------
# FastAPI Dependencies
# ---------------------------------------------------------------------


async def get_optional_user(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(bearer_scheme),
) -> Optional[AuthenticatedUser]:
    """
    FastAPI dependency that returns authenticated user if token is present.

    Returns None if no token provided (for public endpoints).

    Args:
        credentials: Optional HTTP Bearer credentials

    Returns:
        AuthenticatedUser if token is valid, None otherwise
    """
    if credentials is None:
        return None

    jwt_user = await _decode_and_validate_token(credentials.credentials)
    return _normalize_user(jwt_user)


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(HTTPBearer()),
) -> AuthenticatedUser:
    """
    FastAPI dependency that requires authenticated user.

    Raises 401 if no token or invalid token.

    Args:
        credentials: HTTP Bearer credentials (required)

    Returns:
        AuthenticatedUser with validated claims

    Raises:
        HTTPException: 401 if authentication fails
    """
    jwt_user = await _decode_and_validate_token(credentials.credentials)
    return _normalize_user(jwt_user)
