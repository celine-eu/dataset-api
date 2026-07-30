"""EDR PEP — dataspace-aware access enforcement.

**This service is the EDR endpoint.** Upstream EDC removed the data-plane proxy
(`data-plane-public-api-v2`, deprecated 2025-02), so nothing sits in front to
validate the token before a request arrives here, and the token carries no
`exp` (EDC 0.16 mints `jti/aud/iss/sub/iat` and nothing else). Two consequences
shape everything below:

1. **The signature is verified here or nowhere.** `aud` is the consumer's
   identity and is the one fact that must never come from a header.
2. **Every request asks the control plane**, because a token that cannot expire
   is only as good as the last time somebody checked whether the agreement
   behind it still stands.

ds decides; this module carries the question and enforces the answer. It
resolves no consent, no agreement state and no purpose vocabulary of its own —
one round trip returns the verdict *and* the row-filter spec to apply.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Optional

import httpx
import jwt
from fastapi import HTTPException

from celine.dataset.core.config import get_settings

logger = logging.getLogger(__name__)

_jwks_cache: dict[str, Any] = {}


@dataclass
class EDRRequestContext:
    """A dataspace request, after its token has been verified.

    `consumer_id` comes from the verified `aud`. `agreement_id`, `transfer_id`
    and `purpose` are client-asserted headers — safe only because ds refuses an
    agreement that does not belong to `consumer_id`, and a purpose the agreement
    does not permit. A caller can lie only within what it already holds.
    """

    agreement_id: str
    consumer_id: str
    transfer_id: Optional[str] = None
    purpose: list[str] = field(default_factory=list)


@dataclass
class DataPlaneDecision:
    """ds's answer: whether rows may flow, and which."""

    allowed: bool
    reason: Optional[str] = None
    datasets: list[dict[str, Any]] = field(default_factory=list)
    cache_ttl: Optional[int] = None

    def row_filter_for(self, dataset_id: str) -> Optional[dict[str, Any]]:
        for entry in self.datasets:
            if entry.get("dataset_id") == dataset_id:
                return entry.get("row_filter")
        return None

    def reason_for(self, dataset_id: str) -> Optional[str]:
        for entry in self.datasets:
            if entry.get("dataset_id") == dataset_id:
                return entry.get("reason")
        return self.reason


async def verify_edr_consumer(authorization: Optional[str]) -> str:
    """The consumer DID this request proves, from the EDR token's `aud`.

    Every key in the published set is tried rather than the one matching `kid`:
    EDC stamps its **vault alias** into the header while the JWK may carry its
    own name. The set is one or two keys, so trying them all costs nothing and
    survives a rotation that renames either.
    """
    token = (authorization or "").removeprefix("Bearer ").strip()
    if not token:
        raise HTTPException(401, "Dataspace mode requires the EDR token")

    claims = None
    for key in await _verification_keys():
        try:
            claims = jwt.decode(
                token,
                key=key,
                algorithms=["ES256", "RS256"],
                options={"verify_aud": False, "verify_exp": False},
            )
            break
        except Exception:  # noqa: BLE001 — try the next key, refuse if none fit
            continue

    if claims is None:
        raise HTTPException(401, "EDR token is not valid")

    audience = claims.get("aud")
    if isinstance(audience, list):
        audience = audience[0] if audience else None
    if not audience:
        raise HTTPException(401, "EDR token names no audience")
    return str(audience)


async def _verification_keys() -> list[Any]:
    """The provider's EDR signing keys, published by ds.

    ds serves the public half of the vault key EDC signs with, so this service
    never needs the EDC vault or its management credential.
    """
    from jwt import PyJWK

    if _jwks_cache.get("keys"):
        return _jwks_cache["keys"]

    base = _connector_base()
    async with httpx.AsyncClient(timeout=5.0) as client:
        response = await client.get(
            f"{base}/internal/edr-jwks", headers=await _service_headers()
        )
    response.raise_for_status()

    keys = []
    for entry in response.json().get("keys", []):
        try:
            keys.append(PyJWK.from_dict({**entry, "alg": entry.get("alg", "ES256")}).key)
        except Exception:  # noqa: BLE001 — an unusable key is not a fatal one
            logger.warning("Unusable JWK in the EDR key set: %s", entry.get("kid"))
    if not keys:
        raise HTTPException(503, "ds published no usable EDR verification key")
    _jwks_cache["keys"] = keys
    return keys


async def authorize_dataplane(
    *,
    context: EDRRequestContext,
    dataset_ids: list[str],
) -> DataPlaneDecision:
    """Ask ds whether these rows may flow, and under which filter.

    One call, one decision. This service assembles nothing: agreement validity,
    the agreement↔consumer binding, purpose admissibility and the consented
    subject set are all ds's to answer.

    **ds unreachable is a denial, never an allow.** The control plane failing to
    respond is precisely when a data plane must not improvise.
    """
    base = _connector_base()
    payload = {
        "consumer_did": context.consumer_id,
        "agreement_id": context.agreement_id,
        "transfer_id": context.transfer_id,
        "purpose": context.purpose,
        "dataset_ids": dataset_ids,
    }
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.post(
                f"{base}/internal/dataplane/authorize",
                json=payload,
                headers=await _service_headers(),
            )
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        logger.error("ds-connector returned %s for the data-plane check", exc.response.status_code)
        raise HTTPException(502, f"ds-connector error: {exc.response.status_code}") from exc
    except httpx.RequestError as exc:
        logger.error("ds-connector unreachable for the data-plane check: %s", exc)
        raise HTTPException(502, "ds-connector unreachable") from exc

    body = response.json()
    return DataPlaneDecision(
        allowed=body.get("decision") == "allow",
        reason=body.get("reason"),
        datasets=body.get("datasets") or [],
        cache_ttl=(body.get("cache") or {}).get("ttl_seconds"),
    )


def _connector_base() -> str:
    base = get_settings().connector_internal_url
    if not base:
        raise HTTPException(
            503, "Dataspace mode is enabled but CONNECTOR_INTERNAL_URL is not configured"
        )
    return base.rstrip("/")


async def _service_headers() -> dict[str, str]:
    """Authenticate to ds's `/internal/*` API as this service.

    `svc-ds-dataset-api` holds `connector.internal`. Previously these calls were
    unauthenticated, which ds refuses outright — the connector dropped its
    `X-Api-Key` fallback because that key was also EDC's management credential.
    """
    from celine.sdk.auth import OidcClientCredentialsProvider

    settings = get_settings()
    oidc = getattr(settings, "oidc", None)
    if oidc is None or not getattr(oidc, "client_id", None):
        logger.warning("No OIDC client configured — /internal/* calls will be refused")
        return {}
    provider = OidcClientCredentialsProvider(
        base_url=oidc.base_url,
        client_id=oidc.client_id,
        client_secret=oidc.client_secret,
    )
    return {"Authorization": f"Bearer {(await provider.get_token()).access_token}"}


async def audit_query(
    *,
    dataset_id: str,
    consumer_id: Optional[str],
    agreement_id: Optional[str],
    transfer_id: Optional[str],
    row_count: int,
    authorized_subject_ids: Optional[list[str]] = None,
    subject_id: Optional[str] = None,
) -> None:
    """Record a `QueryExecuted` disclosure with ds — the accountability half.

    `authorize_dataplane` is the *decision*; this is the *disclosure*. ds only
    learns a query actually ran, and how many rows it returned, when the PEP says
    so: the connector emits the `QueryExecuted` provenance event **solely** from
    this call (`POST /internal/audit/query`). Without it a disclosure leaves no
    accountability record — who received which rows under which agreement.

    `authorized_subject_ids` are the row filter's `principals` — registry-native
    identifiers, never DIDs (a DID is derived from an unsalted email hash, so it
    is re-identifiable by anyone later holding the payload).

    **Best-effort.** A failure here must not fail a query the control plane
    already authorised and served, but it is logged: a silently dropped
    disclosure is the worst outcome for an accountability record.
    """
    payload = {
        "dataset_id": dataset_id,
        "consumer_id": consumer_id,
        "user_id": subject_id,
        "subject_id": subject_id,
        "agreement_id": agreement_id,
        "transfer_id": transfer_id,
        "row_count": row_count,
        "authorized_subject_ids": authorized_subject_ids,
    }
    try:
        base = _connector_base()
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.post(
                f"{base}/internal/audit/query",
                json=payload,
                headers=await _service_headers(),
            )
        response.raise_for_status()
    except (httpx.HTTPError, HTTPException) as exc:
        logger.warning(
            "QueryExecuted disclosure not recorded for %s: %s", dataset_id, exc
        )
