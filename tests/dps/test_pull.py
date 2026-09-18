"""DPS-09 — a pull with a DPS token runs the governed query (integration).

Needs PostgreSQL, like the rest of the query tests. ds is replaced at the one
seam the executor calls (`authorize_dataplane`, `audit_query`), exactly as
`tests/api/test_edr_dataspace_gate.py` does. Everything in between is real: the
token, the flow state in the catalogue database, the exposure gate, the row
filter and the SQL.
"""
from __future__ import annotations

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text

from celine.dataset.api.dataset_query import executor as executor_mod
from celine.dataset.core.config import get_settings
from celine.dataset.db.engine import get_datasets_session, get_session
from celine.dataset.db.models.dataset_entry import DatasetEntry
from celine.dataset.dps.api import get_dataplane
from celine.dataset.dps.messages import EDR_AUTHORIZATION
from celine.dataset.security.edr import DataPlaneDecision

from .conftest import CONSUMER, bearer, edc_start_message

CP = bearer("cp-a")


def _table() -> str:
    return f"{get_settings().catalogue_schema}.dps_pull_readings"


@pytest.fixture()
async def readings(test_session):
    table = _table()
    await test_session.execute(text(f"CREATE TABLE IF NOT EXISTS {table} (owner TEXT, value INTEGER)"))
    await test_session.execute(
        text(f"INSERT INTO {table} VALUES ('member-a', 1), ('member-b', 2), ('member-c', 3)")
    )
    for dataset_id, offered in (("dps_readings", True), ("dps_withheld", False)):
        test_session.add(
            DatasetEntry(
                dataset_id=dataset_id,
                title=dataset_id,
                backend_type="postgres",
                backend_config={"table": table},
                expose=True,
                dataspace_expose=offered,
                access_level="restricted",
            )
        )
    await test_session.commit()
    yield
    await test_session.execute(text(f"DROP TABLE IF EXISTS {table}"))
    await test_session.commit()


@pytest.fixture()
async def http(dps_app, test_session, sql_dataplane):
    async def _session():
        yield test_session

    dps_app.dependency_overrides[get_dataplane] = lambda: sql_dataplane
    dps_app.dependency_overrides[get_session] = _session
    dps_app.dependency_overrides[get_datasets_session] = _session
    async with AsyncClient(transport=ASGITransport(app=dps_app), base_url="http://test") as client:
        yield client


@pytest.fixture()
def ds(monkeypatch):
    """ds's side of the seam: records every question, answers as told."""
    state = {"calls": [], "audits": [], "decision": None}

    async def _authorize(*, context, dataset_ids):
        state["calls"].append((context, dataset_ids))
        return state["decision"] or DataPlaneDecision(
            allowed=True,
            datasets=[{"dataset_id": d, "decision": "allow", "row_filter": None} for d in dataset_ids],
        )

    async def _audit(**kwargs):
        state["audits"].append(kwargs)

    monkeypatch.setattr(executor_mod, "authorize_dataplane", _authorize)
    monkeypatch.setattr(executor_mod, "audit_query", _audit)
    return state


async def _start(http, flow_id: str) -> str:
    r = await http.post("/dps/v1/dataflows/start", json=edc_start_message(flow_id), headers=CP)
    assert r.status_code == 200, r.text
    props = {p["name"]: p["value"] for p in r.json()["dataAddress"]["endpointProperties"]}
    return props[EDR_AUTHORIZATION]


async def _pull(http, token: str | None, dataset: str = "dps_readings", **headers):
    h = {k.replace("_", "-"): v for k, v in headers.items()}
    if token is not None:
        h["Authorization"] = token
    return await http.post(
        "/dps/public/query",
        json={"sql": f"SELECT owner, value FROM {dataset} ORDER BY value", "limit": 10},
        headers=h,
    )


async def test_a_started_flow_serves_rows_under_its_own_agreement(http, readings, ds) -> None:
    token = await _start(http, "tp-pull-1")

    # The raw token, as ds's consumers send it today (no `Bearer`).
    r = await _pull(http, token, Edc_Purpose="research, billing", Edc_Transfer_Process_Id="consumer-tp")
    assert r.status_code == 200, r.text
    assert [row["value"] for row in r.json()["items"]] == [1, 2, 3]

    context, dataset_ids = ds["calls"][0]
    assert context.agreement_id == "agreement-1"  # from the flow
    assert context.consumer_id == CONSUMER  # from the token's aud
    assert context.transfer_id == "consumer-tp"
    assert context.purpose == ["research", "billing"]
    assert dataset_ids == ["dps_readings"]
    assert ds["audits"][0]["agreement_id"] == "agreement-1"
    assert ds["audits"][0]["consumer_id"] == CONSUMER


async def test_the_ds_row_filter_narrows_the_pull(http, readings, ds) -> None:
    token = await _start(http, "tp-pull-2")
    ds["decision"] = DataPlaneDecision(
        allowed=True,
        datasets=[{
            "dataset_id": "dps_readings",
            "decision": "allow",
            "row_filter": {"handler": "direct_user_match", "args": {"column": "owner"},
                           "principals": ["member-a", "member-c"]},
        }],
        cache_ttl=0,
    )
    r = await _pull(http, f"Bearer {token}")
    assert r.status_code == 200, r.text
    assert [row["owner"] for row in r.json()["items"]] == ["member-a", "member-c"]
    assert ds["audits"][0]["authorized_subject_ids"] == ["member-a", "member-c"]


async def test_a_ds_denial_is_a_403(http, readings, ds) -> None:
    token = await _start(http, "tp-pull-3")
    ds["decision"] = DataPlaneDecision(allowed=False, reason="consent_missing")
    r = await _pull(http, token)
    assert r.status_code == 403
    assert "consent_missing" in r.json()["detail"]


async def test_a_dataset_not_offered_is_refused_before_ds_is_asked(http, readings, ds) -> None:
    token = await _start(http, "tp-pull-4")
    r = await _pull(http, token, dataset="dps_withheld")
    assert r.status_code == 403
    assert ds["calls"] == []


async def test_an_agreement_header_cannot_name_another_agreement(http, readings, ds) -> None:
    token = await _start(http, "tp-pull-5")
    r = await _pull(http, token, Edc_Contract_Agreement_Id="someone-elses")
    assert r.status_code == 403
    assert ds["calls"] == []
    # The matching header is accepted.
    r = await _pull(http, token, Edc_Contract_Agreement_Id="agreement-1")
    assert r.status_code == 200


async def test_suspend_stops_the_pull_and_resume_needs_the_new_token(http, readings, ds) -> None:
    token = await _start(http, "tp-pull-6")
    assert (await http.post("/dps/v1/dataflows/tp-pull-6/suspend", json={}, headers=CP)).status_code == 200
    assert (await _pull(http, token)).status_code == 401

    r = await http.post("/dps/v1/dataflows/tp-pull-6/resume", json={}, headers=CP)
    props = {p["name"]: p["value"] for p in r.json()["dataAddress"]["endpointProperties"]}
    assert (await _pull(http, token)).status_code == 401
    assert (await _pull(http, props[EDR_AUTHORIZATION])).status_code == 200


@pytest.mark.parametrize("signal", ["terminate", "completed"])
async def test_a_finished_flow_serves_nothing(http, readings, ds, signal) -> None:
    token = await _start(http, f"tp-pull-{signal}")
    r = await http.post(f"/dps/v1/dataflows/tp-pull-{signal}/{signal}", json={}, headers=CP)
    assert r.status_code == 200
    assert (await _pull(http, token)).status_code == 401
    assert ds["calls"] == []


async def test_a_flow_not_started_refuses_a_live_token(http, readings, ds, sql_dataplane) -> None:
    """Defence in depth: a live token on a flow that is not STARTED is a 403."""
    from celine.dataset.dps.flows import FlowState

    token = await _start(http, "tp-pull-7")
    async with sql_dataplane.store.locked("tp-pull-7") as flow:
        flow.state = FlowState.SUSPENDED  # the token is left bound
    r = await _pull(http, token)
    assert r.status_code == 403
    assert ds["calls"] == []


@pytest.mark.parametrize(
    "token",
    [None, "", "Bearer ", "not-a-jwt"],
)
async def test_no_valid_token_no_rows(http, readings, ds, token) -> None:
    r = await _pull(http, token)
    assert r.status_code == 401
    assert ds["calls"] == []


async def test_a_token_from_another_data_plane_is_refused(http, readings, ds) -> None:
    from celine.dataset.dps.tokens import TokenIssuer

    await _start(http, "tp-pull-8")
    foreign, _ = TokenIssuer(ephemeral=True).issue(issuer="did:web:provider.example.org", audience=CONSUMER)
    assert (await _pull(http, foreign)).status_code == 401
    assert ds["calls"] == []
