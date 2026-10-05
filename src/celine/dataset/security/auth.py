from __future__ import annotations

import logging
from typing import Optional

from fastapi import Depends, Header, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from celine.dataset.core.config import get_settings
from celine.dataset.security.audit import CATALOGUE_IMPORT, SERVICE, token_refused
from celine.dataset.security.edr import dataspace_mode
from celine.dataset.security.models import AuthenticatedUser

# Use celine.sdk for JWT validation
from celine.sdk.audit import audit_denied
from celine.sdk.auth import JwtUser
from celine.dataset.security.groups import (
    PLATFORM_ADMIN_ROLE,
    is_platform_admin,
    organization_groups_held,
    platform_roles,
)

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
            oidc=get_settings().oidc,
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


def _normalize_user(jwt_user: JwtUser, token: Optional[str]) -> AuthenticatedUser:
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

    # The two levels, kept apart (see security/groups.py): `roles` is the
    # platform level (realm roles only), `groups` the organization level. The
    # top-level `groups` claim is not read: a realm group grants nothing.
    roles = platform_roles(jwt_user.claims)
    groups = organization_groups_held(jwt_user.claims)

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
        roles=roles,
        groups=groups,
        issuer=jwt_user.iss,
        scopes=scopes,
        audiences=aud,
        claims=jwt_user.claims,
        token=token,
    )


# ---------------------------------------------------------------------
# FastAPI Dependencies
# ---------------------------------------------------------------------


async def _verified_user(token: str, request: Request | None) -> AuthenticatedUser:
    """Validate a presented token; a refusal is audited before it is raised (GS-02)."""
    try:
        jwt_user = await _decode_and_validate_token(token)
    except HTTPException as exc:
        token_refused(request, exc)
        raise
    return _normalize_user(jwt_user, token=jwt_user.token)


async def get_optional_user(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(bearer_scheme),
    edc_contract_agreement_id: Optional[str] = Header(default=None),
    request: Request = None,  # type: ignore[assignment] — injected by FastAPI
) -> Optional[AuthenticatedUser]:
    """
    FastAPI dependency that returns authenticated user if token is present.

    Returns None if no token provided (for public endpoints), **and for a
    request in dataspace mode**, whose bearer token is not a Keycloak one.

    An EDR token is signed with the provider EDC's vault key, so Keycloak can
    never hold its `kid`. Validating it here refused every EDC transfer with
    `401 Token validation failed` before the route body ran — the dataspace path
    was unreachable at every instance, which is what this branch fixes. The
    token is not ignored: `security/edr.py::verify_edr_token` verifies it in the
    route, against the provider connector's published key set, and dataspace
    mode never falls back to the user path when that fails.

    **What a caller gains by asserting `Edc-Contract-Agreement-Id` is nothing**,
    and what it loses is its identity. `None` is not a privileged state — it is
    the one every caller already reaches by sending no `Authorization` header at
    all, and the floor it stands on is anonymous authority:
    `enforce_dataset_access` refuses a dataset whose access level requires auth,
    the policy engine evaluates `Subject.anonymous()`, and a dataset carrying
    row-filter specs raises 401 rather than serving unfiltered rows. So the
    branch can only cost a caller authority, never grant it — which is what
    makes it safe on every endpoint sharing this dependency, including one added
    by an extension through `celine.dataset.ext`.

    Args:
        credentials: Optional HTTP Bearer credentials
        edc_contract_agreement_id: the EDC agreement header, which selects
            dataspace mode when `edr_enabled` is on

    Returns:
        AuthenticatedUser if token is valid, None otherwise
    """
    if credentials is None:
        return None

    if dataspace_mode(edc_contract_agreement_id):
        return None

    return await _verified_user(credentials.credentials, request)


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(HTTPBearer()),
    request: Request = None,  # type: ignore[assignment] — injected by FastAPI
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
    return await _verified_user(credentials.credentials, request)


#: What may write the catalogue: the pair the shipped Rego grants `restricted` on.
#: `svc-dataset-api` holds the scope, so an import job authenticates as the service.
CATALOGUE_ADMIN_SCOPE = "dataset.admin"
#: The platform level only. An organization's `admins` and a realm group grant nothing.
CATALOGUE_ADMIN_ROLE = PLATFORM_ADMIN_ROLE


async def require_catalogue_admin(
    user: AuthenticatedUser = Depends(get_current_user),
    request: Request = None,  # type: ignore[assignment] — injected by FastAPI
) -> AuthenticatedUser:
    """Admit a caller that may overwrite or delete catalogue entries.

    The import sets `expose` and `access_level` — the very fields every other gate
    reads — so an unauthenticated import would be a way around all of them.

    Raises:
        HTTPException: 401 without a valid token, 403 without the scope or role
    """
    if CATALOGUE_ADMIN_SCOPE in user.scopes or is_platform_admin(user.claims):
        return user
    logger.warning("Catalogue admin refused for %s", user.sub)
    audit_denied(
        CATALOGUE_IMPORT,
        caller=user,
        reason="not_catalogue_admin",
        service=SERVICE,
        request=request,
    )
    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail=f"Requires the {CATALOGUE_ADMIN_SCOPE} scope or the "
        f"{CATALOGUE_ADMIN_ROLE} role",
    )
