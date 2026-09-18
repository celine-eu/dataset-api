"""Two participants, one dataset-api: each request reaches its own control plane.

The integration half of `tests/security/test_edr_connector_resolution.py`. That
one holds the resolution; this one holds the wiring from the HTTP request to it —
`routes/query.py` carrying the provider out of the verified token, and
`executor.execute_query` carrying it back into the disclosure.

**This does not replace the deployment topology.** Two instances remain how two
participants are deployed, because the warehouse is still one engine per process
(`db/engine.py::get_datasets_engine`). What one instance now buys is a test or a
validation pass that exercises two participants without standing up two data
planes — which is exactly the case these tests are.

**Nothing here is stubbed on the authentication path, and that is recent.**
`POST /query` depends on `get_optional_user`, which runs *before* the route body
and used to validate any `Authorization: Bearer …` against Keycloak — which can
never hold the `kid` of a key EDC's vault minted, so the dataspace path was
unreachable at *any* instance. These tests carried a
`dependency_overrides[get_optional_user]` to work around it. The dependency now
returns `None` for a request in dataspace mode, so the override is gone and the
real one runs: every request below reaches the route through the same code a
deployed instance uses.
"""
from __future__ import annotations

from typing import Any

import jwt
import pytest
from fastapi import HTTPException
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text

from celine.dataset.core.config import get_settings
from celine.dataset.db.engine import get_datasets_session, get_session
from celine.dataset.db.models.dataset_entry import DatasetEntry
from celine.dataset.dps.tokens import TokenIssuer
from celine.dataset.main import create_app
from celine.dataset.security import auth as auth_mod
from celine.dataset.security import edr as edr_mod
from celine.dataset.security.edr import clear_jwks_cache
from tests.security.test_edr_connector_resolution import httpx_shim

PROVIDER_A = "did:web:provider-a.example.org"
PROVIDER_B = "did:web:provider-b.example.org"
CONSUMER = "did:web:consumer.example.org"

CONNECTOR_A = "http://connector-a.example.org:30001"
CONNECTOR_B = "http://connector-b.example.org:30001"

TABLE_A = "dataset_api.two_participants_a"
TABLE_B = "dataset_api.two_participants_b"


@pytest.fixture()
async def dataspace_client(test_session):
    """`tests/conftest.py`'s client — the two database sessions overridden, and
    nothing else.

    `get_optional_user` is deliberately *not* overridden. It is the dependency
    this whole path used to die in, so overriding it would prove the wiring
    against a service that does not exist.
    """

    async def override_get_session():
        try:
            yield test_session
        finally:
            if test_session.in_transaction():
                await test_session.rollback()

    app = create_app(use_lifespan=False)
    app.dependency_overrides[get_session] = override_get_session
    app.dependency_overrides[get_datasets_session] = override_get_session

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as c:
        yield c

    app.dependency_overrides.clear()


@pytest.fixture()
async def catalogue_of_two_participants(test_session):
    """One catalogue, two owners' datasets — the shape the map exists for."""
    for table in (TABLE_A, TABLE_B):
        await test_session.execute(text(f"CREATE TABLE IF NOT EXISTS {table} (id INTEGER)"))
        await test_session.execute(text(f"INSERT INTO {table} VALUES (1)"))
    for dataset_id, table in (("ds_from_a", TABLE_A), ("ds_from_b", TABLE_B)):
        test_session.add(
            DatasetEntry(
                dataset_id=dataset_id,
                title=dataset_id,
                backend_type="postgres",
                backend_config={"table": table},
                expose=True,
                dataspace_expose=True,
                access_level="open",
            )
        )
    await test_session.commit()
    yield
    for table in (TABLE_A, TABLE_B):
        await test_session.execute(text(f"DROP TABLE IF EXISTS {table}"))
    await test_session.commit()


class _Response:
    def __init__(self, payload: Any) -> None:
        self._payload = payload
        self.status_code = 200

    def json(self) -> Any:
        return self._payload

    def raise_for_status(self) -> None:
        return None


class _TwoConnectors:
    """Both connectors, answering, and recording who was asked what."""

    def __init__(self, keys: dict[str, Any]) -> None:
        self.keys = keys
        self.calls: list[tuple[str, str]] = []

    def factory(self, *args: Any, **kwargs: Any) -> "_TwoConnectors":
        return self

    async def __aenter__(self) -> "_TwoConnectors":
        return self

    async def __aexit__(self, *exc: Any) -> None:
        return None

    def _base(self, url: str) -> str:
        return url.split("/internal/")[0]

    async def get(self, url: str, **kwargs: Any) -> _Response:
        self.calls.append(("GET", url))
        assert url.endswith("/internal/edr-jwks"), url
        return _Response(self.keys[self._base(url)])

    async def post(self, url: str, json: dict | None = None, **kwargs: Any) -> _Response:
        self.calls.append(("POST", url))
        if url.endswith("/internal/dataplane/authorize"):
            return _Response(
                {
                    "decision": "allow",
                    "datasets": [
                        {"dataset_id": ds, "row_filter": None}
                        for ds in (json or {}).get("dataset_ids", [])
                    ],
                }
            )
        return _Response({})

    def urls(self, method: str, suffix: str) -> list[str]:
        return [u for m, u in self.calls if m == method and u.endswith(suffix)]


@pytest.fixture()
def dataspace(monkeypatch):
    """One instance configured as the data plane of two participants."""
    issuers = {
        PROVIDER_A: TokenIssuer(ephemeral=True),
        PROVIDER_B: TokenIssuer(ephemeral=True),
    }
    connectors = {PROVIDER_A: CONNECTOR_A, PROVIDER_B: CONNECTOR_B}
    keys = {
        connectors[did]: {
            "keys": [
                jwt.algorithms.ECAlgorithm.to_jwk(
                    issuer._public_key,  # noqa: SLF001 — the test owns the key
                    as_dict=True,
                )
            ]
        }
        for did, issuer in issuers.items()
    }

    settings = get_settings()
    monkeypatch.setattr(settings, "edr_enabled", True)
    monkeypatch.setattr(settings, "connector_internal_url", CONNECTOR_A)
    monkeypatch.setattr(settings, "connector_internal_urls", dict(connectors))

    http = _TwoConnectors(keys)
    monkeypatch.setattr(edr_mod, "httpx", httpx_shim(http.factory))
    clear_jwks_cache()

    def token(provider: str) -> str:
        issued, _ = issuers[provider].issue(issuer=provider, audience=CONSUMER)
        return issued

    http.token = token  # type: ignore[attr-defined]
    yield http
    clear_jwks_cache()


async def _query(dataspace_client, dataspace, *, provider: str, dataset: str):
    return await dataspace_client.post(
        "/query",
        json={"sql": f"SELECT * FROM {dataset}", "limit": 10},
        headers={
            "Authorization": f"Bearer {dataspace.token(provider)}",
            "Edc-Contract-Agreement-Id": "agr-1",
            "Edc-Transfer-Process-Id": "tr-1",
        },
    )


@pytest.mark.asyncio
async def test_each_participants_request_reaches_its_own_connector(
    dataspace_client, catalogue_of_two_participants, dataspace
) -> None:
    """The whole point, end to end through the route.

    Two requests, two providers, one process. Each one's keys, decision and
    disclosure come from and go to that provider's connector — nothing is
    resolved once at startup.
    """
    first = await _query(dataspace_client, dataspace, provider=PROVIDER_A, dataset="ds_from_a")
    assert first.status_code == 200, first.text

    second = await _query(dataspace_client, dataspace, provider=PROVIDER_B, dataset="ds_from_b")
    assert second.status_code == 200, second.text

    assert dataspace.urls("GET", "/internal/edr-jwks") == [
        f"{CONNECTOR_A}/internal/edr-jwks",
        f"{CONNECTOR_B}/internal/edr-jwks",
    ]
    assert dataspace.urls("POST", "/internal/dataplane/authorize") == [
        f"{CONNECTOR_A}/internal/dataplane/authorize",
        f"{CONNECTOR_B}/internal/dataplane/authorize",
    ]
    assert dataspace.urls("POST", "/internal/audit/query") == [
        f"{CONNECTOR_A}/internal/audit/query",
        f"{CONNECTOR_B}/internal/audit/query",
    ]


@pytest.mark.asyncio
async def test_the_disclosure_follows_the_decision_not_the_default(
    dataspace_client, catalogue_of_two_participants, dataspace
) -> None:
    """B alone, so nothing can be attributed to A being the configured default."""
    response = await _query(dataspace_client, dataspace, provider=PROVIDER_B, dataset="ds_from_b")
    assert response.status_code == 200, response.text
    assert all(url.startswith(CONNECTOR_B) for _, url in dataspace.calls), dataspace.calls


@pytest.mark.asyncio
async def test_a_query_for_another_participants_dataset_asks_the_issuers_connector(
    dataspace_client, catalogue_of_two_participants, dataspace
) -> None:
    """Fail-closed by construction rather than by a check here.

    The token issued by B names B's dataset ids to B's connector even when the
    SQL reaches for A's table. B holds no agreement covering A's dataset, so B
    denies — which is correct, and is why the catalogue's owner is not consulted
    to pick the connector. Asking A instead, on the strength of the catalogue,
    would be letting an import choose who authorises.
    """
    response = await _query(dataspace_client, dataspace, provider=PROVIDER_B, dataset="ds_from_a")
    assert response.status_code == 200, response.text  # this stub connector allows
    assert dataspace.urls("POST", "/internal/dataplane/authorize") == [
        f"{CONNECTOR_B}/internal/dataplane/authorize"
    ]


@pytest.mark.asyncio
async def test_the_same_token_without_the_agreement_header_still_meets_keycloak(
    dataspace_client, catalogue_of_two_participants, dataspace, monkeypatch
) -> None:
    """The gate is a gate, over HTTP and not only in the unit test.

    Drop `Edc-Contract-Agreement-Id` and the identical bearer token is validated
    as an ordinary access token again — so the fix cannot be used to shed
    Keycloak by adding one header to a request that is not a dataspace request.
    Keycloak is stood in for by a refusal, which is what it really answers for a
    token EDC's vault signed, and it keeps the test off the network.
    """

    asked: list[str] = []

    async def refuse(token: str):
        asked.append(token)
        raise HTTPException(status_code=401, detail="Token validation failed")

    monkeypatch.setattr(auth_mod, "_decode_and_validate_token", refuse)

    token = dataspace.token(PROVIDER_A)
    response = await dataspace_client.post(
        "/query",
        json={"sql": "SELECT * FROM ds_from_a", "limit": 10},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 401, response.text
    assert asked == [token]
    assert dataspace.calls == []


@pytest.mark.asyncio
async def test_a_provider_the_instance_does_not_serve_is_refused(
    dataspace_client, catalogue_of_two_participants, dataspace, monkeypatch
) -> None:
    """A stranger's token is refused before any connector is asked — and the
    ordinary user-auth path is not a fallback for it."""
    stranger = TokenIssuer(ephemeral=True)
    token, _ = stranger.issue(issuer="did:web:stranger.example.org", audience=CONSUMER)
    response = await dataspace_client.post(
        "/query",
        json={"sql": "SELECT * FROM ds_from_a", "limit": 10},
        headers={
            "Authorization": f"Bearer {token}",
            "Edc-Contract-Agreement-Id": "agr-1",
        },
    )
    assert response.status_code == 401
    # The *reason* is asserted, not only the status. Keycloak's refusal of the
    # same request was also a 401, so a status-only assertion passed while the
    # request never reached this gate at all.
    assert (
        response.json()["detail"]
        == "This data plane does not serve the provider that issued the token"
    ), response.text
    assert dataspace.calls == []
