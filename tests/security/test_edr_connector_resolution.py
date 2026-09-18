"""One instance, several connectors: which control plane answers for a request.

The EDR token names both ends. `aud` is the consumer — this module already read
it — and `iss` is the provider, which is what decides *whose* control plane
verifies the signature, gives the decision and receives the disclosure. These
tests hold the three pieces that were process-global:

- `_connector_base(participant_id)` — the map, its fallback, and the refusal
  that stops an unknown provider being sent to the default connector;
- `_jwks_cache` — keyed by connector, so one provider's keys are never offered
  to another provider's token;
- `verify_edr_token` — the issuer chooses the key set, and a forged issuer buys
  nothing because the signature is still checked against the set it chose.

The token shape is EDC's, not this repository's invention: EDC
`DataPlaneAuthorizationServiceImpl.createTokenParams` mints
`jti / aud=consumer / iss=sub=provider / iat` and no `exp`. `dps.tokens.TokenIssuer`
already reproduces it exactly, so it mints the fixtures here rather than a second
hand-rolled shape that could drift from the first.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

import jwt
import pytest
from fastapi import HTTPException

from celine.dataset.core.config import get_settings
from celine.dataset.dps.tokens import TokenIssuer
from celine.dataset.security import edr as edr_mod
from celine.dataset.security.edr import (
    EDRRequestContext,
    _connector_base,
    audit_query,
    authorize_dataplane,
    clear_jwks_cache,
    verify_edr_token,
)

PROVIDER_A = "did:web:provider-a.example.org"
PROVIDER_B = "did:web:provider-b.example.org"
CONSUMER = "did:web:consumer.example.org"

CONNECTOR_A = "http://connector-a.example.org:30001"
CONNECTOR_B = "http://connector-b.example.org:30001"


# ---------------------------------------------------------------------------
# Fixtures: two providers, each with its own signing key and its own connector
# ---------------------------------------------------------------------------


@dataclass
class Participant:
    """A provider, its EDR signing key and the connector that publishes it."""

    did: str
    connector: str
    issuer: TokenIssuer

    def token(self, *, audience: str = CONSUMER, issuer: Optional[str] = None) -> str:
        """An EDR token. `issuer` overrides `iss` without changing the key."""
        token, _ = self.issuer.issue(issuer=issuer or self.did, audience=audience)
        return token

    def jwks(self) -> dict[str, Any]:
        """What this participant's connector serves at `/internal/edr-jwks`."""
        public = self.issuer._public_key  # noqa: SLF001 — the test is the key's owner
        return {"keys": [jwt.algorithms.ECAlgorithm.to_jwk(public, as_dict=True)]}


@pytest.fixture()
def participants() -> dict[str, Participant]:
    return {
        PROVIDER_A: Participant(PROVIDER_A, CONNECTOR_A, TokenIssuer(ephemeral=True)),
        PROVIDER_B: Participant(PROVIDER_B, CONNECTOR_B, TokenIssuer(ephemeral=True)),
    }


class _Response:
    def __init__(self, payload: Any, status: int = 200) -> None:
        self._payload = payload
        self.status_code = status

    def json(self) -> Any:
        return self._payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:  # pragma: no cover - not exercised here
            raise AssertionError("unexpected error response in a test double")


class _RecordingHttp:
    """Stands in for `httpx.AsyncClient`, recording every URL it is asked for.

    The point of most of these tests is *which URL* was called, so the double
    records rather than returning something interesting.
    """

    def __init__(self, routes: dict[str, Any]) -> None:
        self.routes = routes
        self.gets: list[str] = []
        self.posts: list[tuple[str, dict]] = []

    def factory(self, *args: Any, **kwargs: Any) -> "_RecordingHttp":
        return self

    async def __aenter__(self) -> "_RecordingHttp":
        return self

    async def __aexit__(self, *exc: Any) -> None:
        return None

    async def get(self, url: str, **kwargs: Any) -> _Response:
        self.gets.append(url)
        if url not in self.routes:
            raise AssertionError(f"no test route for {url}")
        return _Response(self.routes[url])

    async def post(self, url: str, json: dict | None = None, **kwargs: Any) -> _Response:
        self.posts.append((url, json or {}))
        if url not in self.routes:
            raise AssertionError(f"no test route for {url}")
        return _Response(self.routes[url])


def httpx_shim(factory: Any) -> Any:
    """A stand-in for `httpx` as `edr.py` sees it, replacing nothing else.

    Patching `httpx.AsyncClient` on the real module reaches every other importer
    in the process — including annotations the SDK's generated clients evaluate
    at import time, which then fail with a `TypeError` far from here. Only this
    module's `httpx` is swapped, and the real exception classes are carried over
    because `edr.py` catches them by identity.
    """
    import httpx as real_httpx
    from types import SimpleNamespace

    return SimpleNamespace(
        AsyncClient=factory,
        HTTPError=real_httpx.HTTPError,
        HTTPStatusError=real_httpx.HTTPStatusError,
        RequestError=real_httpx.RequestError,
    )


@pytest.fixture()
def http(monkeypatch, participants) -> _RecordingHttp:
    """Both connectors answering, and nothing else reachable."""
    routes: dict[str, Any] = {}
    for participant in participants.values():
        routes[f"{participant.connector}/internal/edr-jwks"] = participant.jwks()
        routes[f"{participant.connector}/internal/dataplane/authorize"] = {
            "decision": "allow",
            "datasets": [],
        }
        routes[f"{participant.connector}/internal/audit/query"] = {}
    double = _RecordingHttp(routes)
    monkeypatch.setattr(edr_mod, "httpx", httpx_shim(double.factory))
    return double


@pytest.fixture(autouse=True)
def _fresh_cache():
    """The key cache is a module global; no test may inherit another's."""
    clear_jwks_cache()
    yield
    clear_jwks_cache()


@pytest.fixture()
def one_connector(monkeypatch):
    """The deployment as it has always been: one connector, no map."""
    settings = get_settings()
    monkeypatch.setattr(settings, "connector_internal_url", CONNECTOR_A)
    monkeypatch.setattr(settings, "connector_internal_urls", {})
    return settings


@pytest.fixture()
def two_connectors(monkeypatch):
    """Two participants served by one instance — the map lists both."""
    settings = get_settings()
    monkeypatch.setattr(settings, "connector_internal_url", CONNECTOR_A)
    monkeypatch.setattr(
        settings,
        "connector_internal_urls",
        {PROVIDER_A: CONNECTOR_A, PROVIDER_B: CONNECTOR_B},
    )
    return settings


# ---------------------------------------------------------------------------
# _connector_base — the map and its three rules
# ---------------------------------------------------------------------------


def test_no_map_means_the_single_connector_whoever_is_asking(one_connector) -> None:
    """An existing deployment needs no new configuration and does not change."""
    assert _connector_base() == CONNECTOR_A
    assert _connector_base(PROVIDER_A) == CONNECTOR_A
    assert _connector_base(PROVIDER_B) == CONNECTOR_A


def test_a_mapped_participant_resolves_to_its_own_connector(two_connectors) -> None:
    assert _connector_base(PROVIDER_A) == CONNECTOR_A
    assert _connector_base(PROVIDER_B) == CONNECTOR_B


def test_an_unmapped_participant_is_refused_not_sent_to_the_default(
    two_connectors,
) -> None:
    """The decision this whole change exists for.

    Falling back to `connector_internal_url` would send the authorisation call
    to a control plane that has never heard of the agreement, and its denial
    would read as a consent problem rather than a missing map entry.
    """
    with pytest.raises(HTTPException) as exc:
        _connector_base("did:web:stranger.example.org")
    assert exc.value.status_code == 401
    assert "does not serve" in exc.value.detail


def test_an_unnamed_participant_falls_back_to_the_default(two_connectors) -> None:
    """`None` means "no provider was determined" — the DPS path before a flow,
    and a token that named no issuer. It is not an unknown provider."""
    assert _connector_base(None) == CONNECTOR_A


def test_no_connector_at_all_is_a_503(monkeypatch) -> None:
    settings = get_settings()
    monkeypatch.setattr(settings, "connector_internal_url", None)
    monkeypatch.setattr(settings, "connector_internal_urls", {})
    with pytest.raises(HTTPException) as exc:
        _connector_base()
    assert exc.value.status_code == 503


def test_a_mapped_url_loses_its_trailing_slash(monkeypatch) -> None:
    """Paths are appended with a literal `/internal/...`, so a trailing slash
    would produce `//internal/...` — served by some proxies and not others."""
    settings = get_settings()
    monkeypatch.setattr(settings, "connector_internal_url", None)
    monkeypatch.setattr(settings, "connector_internal_urls", {PROVIDER_B: CONNECTOR_B + "/"})
    assert _connector_base(PROVIDER_B) == CONNECTOR_B


# ---------------------------------------------------------------------------
# The key cache is per connector
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_each_connector_publishes_its_own_key_set(
    two_connectors, http, participants
) -> None:
    """Two providers, two key sets, no collision.

    Before this, `_jwks_cache` held one unkeyed entry: whichever provider spoke
    first filled it, and every later provider's token was tried against those
    keys and refused with a flat 401 that no rotation could fix.
    """
    a = await verify_edr_token(f"Bearer {participants[PROVIDER_A].token()}")
    b = await verify_edr_token(f"Bearer {participants[PROVIDER_B].token()}")

    assert a.provider_id == PROVIDER_A
    assert b.provider_id == PROVIDER_B
    assert a.consumer_id == b.consumer_id == CONSUMER
    assert http.gets == [
        f"{CONNECTOR_A}/internal/edr-jwks",
        f"{CONNECTOR_B}/internal/edr-jwks",
    ]


@pytest.mark.asyncio
async def test_a_key_set_is_fetched_once_per_connector(
    two_connectors, http, participants
) -> None:
    for _ in range(3):
        await verify_edr_token(f"Bearer {participants[PROVIDER_B].token()}")
    assert http.gets == [f"{CONNECTOR_B}/internal/edr-jwks"]


# ---------------------------------------------------------------------------
# verify_edr_token
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_verified_claims_give_both_identities(
    two_connectors, http, participants
) -> None:
    verified = await verify_edr_token(f"Bearer {participants[PROVIDER_B].token()}")
    assert verified.consumer_id == CONSUMER
    assert verified.provider_id == PROVIDER_B


@pytest.mark.asyncio
async def test_a_forged_issuer_buys_nothing(two_connectors, http, participants) -> None:
    """`iss` is read unverified to pick a key set — and only to pick one.

    A token signed with B's key but claiming A as its issuer selects A's key
    set, which does not verify it. The claim moved the request to a different
    refusal, not past one.
    """
    forged = participants[PROVIDER_B].token(issuer=PROVIDER_A)
    with pytest.raises(HTTPException) as exc:
        await verify_edr_token(f"Bearer {forged}")
    assert exc.value.status_code == 401
    assert exc.value.detail == "EDR token is not valid"
    assert http.gets == [f"{CONNECTOR_A}/internal/edr-jwks"]


@pytest.mark.asyncio
async def test_an_unknown_issuer_is_refused_before_any_connector_is_asked(
    two_connectors, http, participants
) -> None:
    stranger = TokenIssuer(ephemeral=True)
    token, _ = stranger.issue(issuer="did:web:stranger.example.org", audience=CONSUMER)
    with pytest.raises(HTTPException) as exc:
        await verify_edr_token(f"Bearer {token}")
    assert exc.value.status_code == 401
    assert http.gets == []


@pytest.mark.asyncio
async def test_a_missing_token_is_still_the_first_refusal(two_connectors, http) -> None:
    with pytest.raises(HTTPException) as exc:
        await verify_edr_token(None)
    assert exc.value.status_code == 401
    assert exc.value.detail == "Dataspace mode requires the EDR token"
    assert http.gets == []


@pytest.mark.asyncio
async def test_something_that_is_not_a_jwt_reaches_the_default_and_fails_there(
    one_connector, http
) -> None:
    """The unverified read is best-effort: no `iss` to be had means the default
    connector, and the verification that follows refuses it anyway."""
    with pytest.raises(HTTPException) as exc:
        await verify_edr_token("Bearer not-a-token")
    assert exc.value.status_code == 401
    assert exc.value.detail == "EDR token is not valid"
    assert http.gets == [f"{CONNECTOR_A}/internal/edr-jwks"]


@pytest.mark.asyncio
async def test_a_single_connector_deployment_still_reports_the_issuer(
    one_connector, http, participants
) -> None:
    """`provider_id` is filled from the token even with no map, so the audit and
    the decision travel to the same place the keys came from."""
    verified = await verify_edr_token(f"Bearer {participants[PROVIDER_A].token()}")
    assert verified.provider_id == PROVIDER_A


# ---------------------------------------------------------------------------
# The decision and the disclosure follow the provider
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_decision_is_asked_of_the_providers_own_connector(
    two_connectors, http
) -> None:
    await authorize_dataplane(
        context=EDRRequestContext(
            agreement_id="agr-1",
            consumer_id=CONSUMER,
            provider_id=PROVIDER_B,
        ),
        dataset_ids=["datasets.example.readings"],
    )
    assert [url for url, _ in http.posts] == [
        f"{CONNECTOR_B}/internal/dataplane/authorize"
    ]


@pytest.mark.asyncio
async def test_the_disclosure_is_filed_with_the_connector_that_decided(
    two_connectors, http
) -> None:
    """A QueryExecuted event at the wrong connector is not a record of anything:
    the participant who disclosed would hold no trace, and one who disclosed
    nothing would hold an event."""
    await audit_query(
        dataset_id="datasets.example.readings",
        consumer_id=CONSUMER,
        agreement_id="agr-1",
        transfer_id="tr-1",
        row_count=3,
        provider_id=PROVIDER_B,
    )
    assert [url for url, _ in http.posts] == [f"{CONNECTOR_B}/internal/audit/query"]


@pytest.mark.asyncio
async def test_an_unroutable_disclosure_does_not_fail_the_query(
    two_connectors, http
) -> None:
    """`audit_query` is best-effort by contract. An unmapped provider makes it
    unroutable, and that must still not undo a query ds already authorised."""
    await audit_query(
        dataset_id="datasets.example.readings",
        consumer_id=CONSUMER,
        agreement_id="agr-1",
        transfer_id=None,
        row_count=3,
        provider_id="did:web:stranger.example.org",
    )
    assert http.posts == []
