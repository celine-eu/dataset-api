"""Dataspace mode is decided before Keycloak is asked, and only then.

`get_optional_user` is a FastAPI dependency, so it runs *before* the route body.
It used to validate any `Authorization: Bearer …` against Keycloak, and an EDR
token is signed with the provider EDC's vault key — Keycloak can never hold its
`kid`. Every EDC transfer was therefore refused `401 Token validation failed`
before the dataspace path was entered, at every instance.

These tests hold the gate that fixes it, from both sides:

- it opens exactly when `edr_enabled` **and** `Edc-Contract-Agreement-Id`, which
  is the same predicate the routes use to enter dataspace mode;
- outside that, a bearer token is still validated exactly as it was — the gate
  is a gate, not a way to be rid of Keycloak by sending one more header.

What the gate hands the route is `None`, which is not a privileged state: it is
what every caller already gets by sending no `Authorization` header at all. A
caller asserting the header can only lose its identity, and dataspace mode then
demands an EDR token that verifies against the provider connector's key set.
"""
from __future__ import annotations

from typing import Optional

import pytest
from fastapi import HTTPException
from fastapi.security import HTTPAuthorizationCredentials

from celine.dataset.core.config import get_settings
from celine.dataset.dps.tokens import TokenIssuer
from celine.dataset.security import auth as auth_mod
from celine.dataset.security.auth import get_optional_user
from celine.dataset.security.edr import dataspace_mode

PROVIDER = "did:web:provider.example.org"
CONSUMER = "did:web:consumer.example.org"
AGREEMENT = "agr-1"


@pytest.fixture()
def keycloak(monkeypatch):
    """Keycloak, refusing everything and counting how often it was asked.

    Refusing is what it really does with an EDR token: `JwtUser.from_token`
    fetches the JWKS and finds no key for a `kid` minted by EDC's vault. The
    count is the assertion that matters — a test that only checked the status
    could not tell "the gate opened" from "Keycloak happened to accept it".
    """

    calls: list[str] = []

    async def refuse(token: str):
        calls.append(token)
        raise HTTPException(status_code=401, detail="Token validation failed")

    monkeypatch.setattr(auth_mod, "_decode_and_validate_token", refuse)
    return calls


@pytest.fixture()
def edr_token() -> str:
    """An EDR token in EDC's shape — `iss`/`sub` the provider, `aud` the consumer."""
    token, _ = TokenIssuer(ephemeral=True).issue(issuer=PROVIDER, audience=CONSUMER)
    return token


def _credentials(token: str) -> HTTPAuthorizationCredentials:
    return HTTPAuthorizationCredentials(scheme="Bearer", credentials=token)


async def _resolve(token: Optional[str], agreement: Optional[str]):
    return await get_optional_user(
        credentials=_credentials(token) if token is not None else None,
        edc_contract_agreement_id=agreement,
    )


# ---------------------------------------------------------------------------
# The predicate — one definition, three callers
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("edr_enabled", "agreement", "expected"),
    [
        (True, AGREEMENT, True),
        (True, None, False),
        (True, "", False),
        (False, AGREEMENT, False),
        (False, None, False),
    ],
)
def test_dataspace_mode_truth_table(monkeypatch, edr_enabled, agreement, expected):
    """`edr_enabled` **and** the header. Neither half alone.

    The empty-string row is not pedantry: a proxy that forwards a header it was
    never given sends `Edc-Contract-Agreement-Id:` with nothing after it, and an
    empty agreement id cannot name an agreement. It takes the ordinary path.
    """
    monkeypatch.setattr(get_settings(), "edr_enabled", edr_enabled)
    assert dataspace_mode(agreement) is expected


# ---------------------------------------------------------------------------
# The gate opens
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_an_edr_token_does_not_reach_keycloak(monkeypatch, keycloak, edr_token):
    """The defect, directly: this call used to raise 401 and never return.

    The route gets `None` and goes on to verify the token itself, against the
    provider's key set rather than Keycloak's.
    """
    monkeypatch.setattr(get_settings(), "edr_enabled", True)

    assert await _resolve(edr_token, AGREEMENT) is None
    assert keycloak == []


@pytest.mark.asyncio
async def test_a_keycloak_token_in_dataspace_mode_loses_its_identity(
    monkeypatch, keycloak
):
    """The direction the gate can move authority in: down, never up.

    A caller holding a genuine Keycloak token who also asserts the agreement
    header arrives anonymous — and dataspace mode then demands an EDR token that
    verifies, which its Keycloak token will not. It has spent its identity to
    reach the floor every unauthenticated caller already stands on.
    """
    monkeypatch.setattr(get_settings(), "edr_enabled", True)

    assert await _resolve("a-keycloak-access-token", AGREEMENT) is None
    assert keycloak == []


# ---------------------------------------------------------------------------
# …and stays shut everywhere else
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_without_the_header_the_same_token_is_still_refused(
    monkeypatch, keycloak, edr_token
):
    """No header, no dataspace mode — the ordinary path, unchanged."""
    monkeypatch.setattr(get_settings(), "edr_enabled", True)

    with pytest.raises(HTTPException) as refused:
        await _resolve(edr_token, None)
    assert refused.value.status_code == 401
    assert keycloak == [edr_token]


@pytest.mark.asyncio
async def test_with_edr_disabled_the_header_is_inert(monkeypatch, keycloak, edr_token):
    """An instance that is not in the dataspace is not made anonymous by a header.

    This is the row that stops the gate being a Keycloak bypass anyone can ask
    for: switching it on is a deployment decision, not a request header.
    """
    monkeypatch.setattr(get_settings(), "edr_enabled", False)

    with pytest.raises(HTTPException) as refused:
        await _resolve(edr_token, AGREEMENT)
    assert refused.value.status_code == 401
    assert keycloak == [edr_token]


@pytest.mark.asyncio
async def test_an_empty_agreement_header_does_not_open_the_gate(
    monkeypatch, keycloak, edr_token
):
    monkeypatch.setattr(get_settings(), "edr_enabled", True)

    with pytest.raises(HTTPException) as refused:
        await _resolve(edr_token, "")
    assert refused.value.status_code == 401
    assert keycloak == [edr_token]


@pytest.mark.asyncio
async def test_no_credentials_is_still_none_and_asks_nobody(monkeypatch, keycloak):
    """The pre-existing anonymous path, unchanged by the gate.

    Also the measurement behind the safety argument: `None` was already
    reachable by every caller, with or without the header, so the gate adds no
    state the service did not already have to be safe against.
    """
    monkeypatch.setattr(get_settings(), "edr_enabled", True)

    assert await _resolve(None, AGREEMENT) is None
    assert await _resolve(None, None) is None
    assert keycloak == []
