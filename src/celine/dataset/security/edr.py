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

**Which control plane is asked is resolved per request, not per process.** EDC
puts the provider in the same token it puts the consumer in — `iss` is the
provider's participant id, `aud` the consumer's — so one instance can be the data
plane of several participants at once. `connector_internal_urls` maps the first
to a connector; `connector_internal_url` remains the single-connector default,
and an empty map leaves this module behaving exactly as it did when it had one.

This is a *practicality*, not a new topology: a second participant whose data
lives in its own warehouse still needs a second instance, because
`DatasetEntry.backend_config` names a table and never a connection. What it buys
is a test or a validation pass that exercises two participants without standing
up two data planes.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Optional

import httpx
import jwt
from fastapi import HTTPException
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from celine.dataset.core.config import get_settings

logger = logging.getLogger(__name__)

#: EDR verification keys, **keyed by the connector base URL that published
#: them**. One instance may face several connectors, and a single unkeyed cache
#: is how the first provider's key set came to be tried against every other
#: provider's token — a `401 EDR token is not valid` that no rotation fixes.
_jwks_cache: dict[str, list[Any]] = {}


@dataclass(frozen=True)
class VerifiedEDRToken:
    """The two identities an EDR token proves, both from the verified claims.

    `consumer_id` is `aud` — who is asking. `provider_id` is `iss` — whose
    control plane minted the authorisation, and therefore which connector this
    request's decision and disclosure belong to. `None` means the token named no
    issuer, which is only reachable on a single-connector deployment (a mapped
    deployment refuses before verification, because it cannot choose a key set).
    """

    consumer_id: str
    provider_id: Optional[str] = None


@dataclass
class EDRRequestContext:
    """A dataspace request, after its token has been verified.

    `consumer_id` comes from the verified `aud`, `provider_id` from the verified
    `iss` — or, on the DPS path, from the signalled flow's `participant_id`,
    which is this data plane's own record and better than a claim.
    `agreement_id`, `transfer_id` and `purpose` are client-asserted headers —
    safe only because ds refuses an agreement that does not belong to
    `consumer_id`, and a purpose the agreement does not permit. A caller can lie
    only within what it already holds.

    `provider_id` is what selects the connector for `authorize_dataplane` and
    `audit_query`. `None` means "the default connector", which is what every
    single-connector deployment resolves to.
    """

    agreement_id: str
    consumer_id: str
    transfer_id: Optional[str] = None
    purpose: list[str] = field(default_factory=list)
    provider_id: Optional[str] = None


class DataplaneRowFilter(BaseModel):
    """The row filter as ds puts it on the wire.

    It travels **whole** — handler, args and both allow-lists — never reduced to
    a column and a list of ids. The handler is what knows how a person maps to
    values in the column, and a decision stripped of it forces this end to guess
    which one it was.

    **An unknown field is refused, not ignored.** These fields are narrowings.
    The dangerous direction of drift is one-way: a control plane that adds one
    an older data plane skips over serves rows it was told to withhold, and
    nothing on either side notices. Refusing means an upgrade on ds's side ahead
    of this one stops the data plane rather than widening it, which is the side
    of that trade worth being on. ds makes the same choice in
    `ds.governance.dataplane` (`extra="forbid"`).
    """

    model_config = ConfigDict(extra="forbid")

    handler: str
    #: Governance's own `args`, verbatim and uninterpreted by ds: `{"column": …}`
    #: for every handler in use, plus whatever else a handler defines.
    args: dict[str, Any] = Field(default_factory=dict)
    #: Identifiers native to *this* system — usernames a handler can resolve.
    #: Never subject DIDs.
    principals: list[str] = Field(default_factory=list)
    #: Typed data keys, `"<type>:<value>"` — the values this holder already
    #: stores those same subjects' rows under, registered with the consent by
    #: the organisation that collected it. Personal data: they may reach a
    #: predicate and nothing else.
    keys: list[str] = Field(default_factory=list)


@dataclass
class DataPlaneDecision:
    """ds's answer: whether rows may flow, and which."""

    allowed: bool
    reason: Optional[str] = None
    datasets: list[dict[str, Any]] = field(default_factory=list)
    cache_ttl: Optional[int] = None

    def row_filter_for(self, dataset_id: str) -> Optional[DataplaneRowFilter]:
        """This dataset's filter, parsed — or `None` if it carries none.

        `None` means *no filter applies*: the agreement gated the dataset and
        every row may leave. It never means "a filter was intended and could not
        be built", which is a denial, because the two are indistinguishable once
        the predicate is gone.

        Parsed here rather than at the response, so a filter for a dataset this
        query never touches cannot refuse a query that is otherwise fine.
        """
        for entry in self.datasets:
            if entry.get("dataset_id") != dataset_id:
                continue
            raw = entry.get("row_filter")
            if raw is None:
                return None
            try:
                return DataplaneRowFilter.model_validate(raw)
            except ValidationError as exc:
                # A narrowing this service cannot read is not an allow. 502
                # rather than 403: the consumer did nothing wrong and can do
                # nothing about it — the two ends of the contract disagree.
                logger.error(
                    "ds sent a row filter this data plane cannot read for %s: %s",
                    dataset_id,
                    exc,
                )
                raise HTTPException(
                    502,
                    "ds-connector sent a row filter this data plane cannot apply "
                    f"for {dataset_id}",
                ) from exc
        return None

    def reason_for(self, dataset_id: str) -> Optional[str]:
        for entry in self.datasets:
            if entry.get("dataset_id") == dataset_id:
                return entry.get("reason")
        return self.reason


def dataspace_mode(edc_contract_agreement_id: Optional[str]) -> bool:
    """Does this request take the dataspace path rather than the user path?

    **The one definition**, because three places need the same answer and a
    disagreement between them would be invisible in all three: both routes, and
    `security/auth.py::get_optional_user`, which has to know *before* the route
    body runs that the `Authorization` header holds an EDR token rather than a
    Keycloak one.

    Drift is the hazard it exists to prevent. A dependency gating on less than a
    route leaves the route in dataspace mode behind a Keycloak refusal — the
    defect that made the EDR path unreachable at every instance. A dependency
    gating on more strips identity from requests that then take the ordinary
    path as anonymous.

    `edr_enabled` off makes the header inert, so an instance that is not in the
    dataspace is never put into dataspace mode by a client-asserted header.
    """
    return bool(get_settings().edr_enabled and edc_contract_agreement_id)


async def verify_edr_token(authorization: Optional[str]) -> VerifiedEDRToken:
    """The consumer and the provider this request proves, from the EDR token.

    Every key in the published set is tried rather than the one matching `kid`:
    EDC stamps its **vault alias** into the header while the JWK may carry its
    own name. The set is one or two keys, so trying them all costs nothing and
    survives a rotation that renames either.

    **Which set** is chosen by `iss`, read from the *unverified* token. That is
    the only way round a cycle — selecting the key set needs the issuer, and
    verifying the issuer needs the key set — and it is safe because the claim is
    used for nothing else: a forged `iss` selects a key set that will not verify
    the token, so the request is refused one step later than it would have been.
    `aud` is still taken from the verified claims, which is this module's
    standing invariant.
    """
    token = (authorization or "").removeprefix("Bearer ").strip()
    if not token:
        raise HTTPException(401, "Dataspace mode requires the EDR token")

    provider = _unverified_issuer(token)
    base = _connector_base(provider)

    claims = None
    for key in await _verification_keys(base):
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

    # From the verified claims now, not from the unverified read above: the
    # provider decides which control plane is asked for the decision and told of
    # the disclosure, so it must be the one the signature vouches for.
    issuer = claims.get("iss")
    return VerifiedEDRToken(
        consumer_id=str(audience),
        provider_id=str(issuer) if issuer else None,
    )


def _unverified_issuer(token: str) -> Optional[str]:
    """`iss` from the unverified token — enough to choose a key set, no more.

    Best-effort: a token that is not a JWT at all yields `None`, which resolves
    the default connector and then fails verification there, exactly as it did
    before this function existed.
    """
    try:
        claims = jwt.decode(token, options={"verify_signature": False})
    except Exception:  # noqa: BLE001 — not a JWT; the verification below refuses it
        return None
    issuer = claims.get("iss")
    return str(issuer) if issuer else None


async def _verification_keys(connector_base: str) -> list[Any]:
    """The provider's EDR signing keys, published by ds at `connector_base`.

    ds serves the public half of the vault key EDC signs with, so this service
    never needs the EDC vault or its management credential.

    Cached per connector. A cache shared across connectors would hand one
    provider's keys to another provider's token, and since every key in the set
    is tried the failure is a flat `401` with nothing to say which connector was
    asked.
    """
    from jwt import PyJWK

    cached = _jwks_cache.get(connector_base)
    if cached:
        return cached

    async with httpx.AsyncClient(timeout=5.0) as client:
        response = await client.get(
            f"{connector_base}/internal/edr-jwks", headers=await _service_headers()
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
    _jwks_cache[connector_base] = keys
    return keys


def clear_jwks_cache() -> None:
    """Forget every connector's key set. For tests and for a key rotation."""
    _jwks_cache.clear()


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

    The connector asked is the one that speaks for `context.provider_id` — the
    token's issuer. Asking any other would be asking about an agreement it does
    not hold.
    """
    base = _connector_base(context.provider_id)
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


def _connector_base(participant_id: Optional[str] = None) -> str:
    """The connector that speaks for `participant_id`.

    Three rules, and the third is the one worth stating:

    - **No map configured** — `connector_internal_url`, whoever is asking. A
      single-connector deployment needs no new configuration and behaves exactly
      as before.
    - **Mapped** — that participant's connector.
    - **Mapped, but not this participant** — refused. The tempting alternative,
      falling back to `connector_internal_url`, reinstates the quiet failure this
      resolution exists to remove: the authorisation call reaches a control plane
      that has never heard of the agreement, and its denial reads as a consent
      problem rather than as a missing map entry. So **listing any connector
      means listing them all**, including the default one.
    """
    settings = get_settings()
    mapped = settings.connector_internal_urls or {}

    if mapped and participant_id is not None:
        base = mapped.get(participant_id)
        if base:
            return base.rstrip("/")
        logger.warning(
            "No connector is configured for provider %s; %d are",
            participant_id,
            len(mapped),
        )
        raise HTTPException(
            401, "This data plane does not serve the provider that issued the token"
        )

    base = settings.connector_internal_url
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
    provider_id: Optional[str] = None,
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

    `provider_id` names the connector that gets the record — the same one that
    gave the decision. A disclosure filed with the wrong control plane is not a
    disclosure: the provenance event would be emitted by a participant who
    disclosed nothing, and the one who did would have no record of it.
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
        base = _connector_base(provider_id)
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
