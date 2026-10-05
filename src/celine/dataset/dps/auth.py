"""Who may signal this data plane (DPS-02).

EDC's `oauth2_client_credentials` profile (`Oauth2CredentialsSignalingAuthorization`)
has the control plane fetch a client-credentials token and send it as a bearer.
The token is validated like any other token this service accepts (issuer,
audience and signature, through `celine.sdk`). Then the calling client must be
one this data plane was told to trust.
"""
from __future__ import annotations

import logging
from typing import Optional

from celine.sdk.audit import audit_denied
from fastapi import Header, HTTPException, Request

from celine.dataset.core.config import get_settings
from celine.dataset.dps.settings import get_dps_settings
from celine.dataset.security.audit import DPS_SIGNAL, SERVICE

logger = logging.getLogger(__name__)


def _validated_claims(token: str) -> dict:
    from celine.sdk.auth import JwtUser

    return JwtUser.from_token(token, oidc=get_settings().oidc).claims or {}


def _refused(request: Request | None, reason: str, claims: dict | None = None) -> None:
    # The caller is named only from a token that verified (GS-02).
    audit_denied(DPS_SIGNAL, caller=claims, reason=reason, service=SERVICE, request=request)


async def signaling_caller(
    authorization: Optional[str] = Header(default=None),
    request: Request = None,  # type: ignore[assignment] — injected by FastAPI
) -> str:
    """The client id of an admitted control plane, or 401/403."""
    token = (authorization or "").strip()
    if token[:7].lower() == "bearer ":
        token = token[7:].strip()
    if not token:
        _refused(request, "no_token")
        raise HTTPException(401, "signalling requires a bearer token")
    try:
        claims = _validated_claims(token)
    except Exception as exc:  # noqa: BLE001 — any validation failure is a 401
        logger.info("DPS signalling token refused: %s", exc)
        _refused(request, "invalid_token")
        raise HTTPException(401, "signalling token is not valid") from exc

    # Keycloak puts the client in `azp`; `client_id` is the RFC 9068 name.
    caller = claims.get("azp") or claims.get("client_id")
    if not caller or caller not in get_dps_settings().control_plane_clients:
        logger.warning("DPS signalling refused for client %r", caller)
        _refused(request, "client_not_admitted", claims)
        raise HTTPException(403, "this client may not signal this data plane")
    return str(caller)
