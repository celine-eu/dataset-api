# dataset/security/auth.py
from __future__ import annotations

import logging
from functools import lru_cache
from typing import Any, Awaitable, Callable, Optional

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from fastapi_keycloak import FastAPIKeycloak, OIDCUser

from celine.dataset.core.config import settings

logger = logging.getLogger(__name__)

bearer_scheme = HTTPBearer(auto_error=False)

DEFAULT_AUDIENCE = "account"


# ---------------------------------------------------------------------
# Keycloak initialization
# ---------------------------------------------------------------------
def _parse_issuer(issuer: str) -> tuple[str, str]:
    issuer = issuer.rstrip("/")
    marker = "/realms/"
    if marker not in issuer:
        raise ValueError("Invalid Keycloak issuer URL")
    server_url, realm = issuer.split(marker, 1)
    return server_url, realm


@lru_cache
def get_keycloak() -> FastAPIKeycloak:
    if not settings.keycloak_issuer:
        raise RuntimeError("Keycloak issuer not configured")

    server_url, realm = _parse_issuer(str(settings.keycloak_issuer))

    if not settings.keycloak_client_id or not settings.keycloak_client_secret:
        raise RuntimeError("Keycloak client credentials not configured")

    admin_secret = (
        settings.keycloak_admin_client_secret or settings.keycloak_client_secret
    )

    try:
        kc = FastAPIKeycloak(
            server_url=server_url,
            realm=realm,
            client_id=settings.keycloak_client_id,
            client_secret=settings.keycloak_client_secret,
            admin_client_secret=admin_secret,
            callback_uri=str(settings.keycloak_callback_uri),
        )
    except Exception as exc:
        logger.exception("Failed to initialize Keycloak")
        raise RuntimeError("Keycloak initialization failed") from exc

    logger.info(
        "Keycloak initialized (realm=%s, client_id=%s)",
        realm,
        settings.keycloak_client_id,
    )
    return kc


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
def _audience() -> str:
    return settings.keycloak_audience or DEFAULT_AUDIENCE


def _user_to_dict(user: OIDCUser) -> dict[str, Any]:
    data = user.model_dump() if hasattr(user, "model_dump") else user.dict()

    groups = data.get("groups") or []
    data["group_names"] = [g.get("name") for g in groups if "name" in g]
    data["group_paths"] = [g.get("path") for g in groups if "path" in g]

    return data


# ---------------------------------------------------------------------
# Dependencies
# ---------------------------------------------------------------------
async def get_optional_user(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(bearer_scheme),
) -> Optional[dict[str, Any]]:
    """
    Optional authentication:
    - No token → None
    - Invalid token → 401
    """
    if credentials is None:
        return None

    token = credentials.credentials
    kc = get_keycloak()

    try:
        if not kc.token_is_valid(token, audience=_audience()):
            raise HTTPException(status_code=401, detail="Invalid token")

        decoded = kc._decode_token(token, audience=_audience())
        return _user_to_dict(OIDCUser.parse_obj(decoded))

    except HTTPException:
        raise
    except Exception as exc:
        logger.warning("Optional auth failed: %s", exc)
        raise HTTPException(status_code=401, detail="Invalid token")


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(HTTPBearer()),
) -> dict[str, Any]:
    """
    Mandatory authentication.
    """
    token = credentials.credentials
    kc = get_keycloak()

    try:
        if not kc.token_is_valid(token, audience=_audience()):
            raise HTTPException(status_code=401, detail="Invalid token")

        decoded = kc._decode_token(token, audience=_audience())
        return _user_to_dict(OIDCUser.parse_obj(decoded))

    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Authentication failed")
        raise HTTPException(status_code=401, detail="Invalid token") from exc


# ---------------------------------------------------------------------
# Dataset disclosure enforcement
# ---------------------------------------------------------------------
def requires_auth(access_level: Optional[str]) -> bool:
    """
    Default policy:
      open / public → anonymous allowed
      everything else → auth required
    """
    return (access_level or "open").lower() not in {"open", "public", "green"}


def dataset_user_dependency(
    *, access_level: Optional[str]
) -> Callable[..., Awaitable[Optional[dict[str, Any]]]]:
    """
    Dependency factory used by dataset routes.
    """

    async def _dep(
        credentials: Optional[HTTPAuthorizationCredentials] = Depends(bearer_scheme),
    ) -> Optional[dict[str, Any]]:
        if requires_auth(access_level):
            if credentials is None:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Authentication required for this dataset",
                )
            return await get_current_user(credentials)

        return await get_optional_user(credentials)

    return _dep
