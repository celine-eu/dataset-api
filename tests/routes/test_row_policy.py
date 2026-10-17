"""The row policy's two layers, and what an undeclared one does (GS-10, GS-11, GS-12).

Each dataset below is `internal` unless it says otherwise, holds rows `1, 2`, and
declares some part of its row policy. The callers are an organization's viewer, a
service holding the scope and the platform administrator. Real query path, shipped
Rego; the only seam is the identity.
"""
from __future__ import annotations

import json
import logging
from types import SimpleNamespace

import pytest
from sqlalchemy import text

from celine.dataset.api.dataset_query.row_filters import get_row_filter_registry
from celine.dataset.core.config import get_settings
from celine.dataset.db.models.dataset_entry import DatasetEntry
from celine.dataset.security.auth import _normalize_user, get_optional_user

AUDIT = "celine.audit"

PERSON_ALL = {"handler": "allow_all", "binds": "person"}
PERSON_NONE = {"handler": "deny_all", "binds": "person"}
ORG_ALL = {"handler": "allow_all", "binds": "organization"}
ORG_NONE = {"handler": "deny_all", "binds": "organization"}
WIDE = {"handler": "member_wide", "binds": "organization"}

VIEWER = {
    "sub": "viewer-a",
    "azp": "oauth2_proxy",
    "preferred_username": "viewer-a",
    "realm_access": {"roles": ["default-roles-celine"]},
    "organization": {"example-rec-a": {"type": ["rec"], "groups": ["/viewers"]}},
}
SERVICE = {
    "sub": "svc-digital-twin",
    "azp": "svc-digital-twin",
    "preferred_username": "service-account-svc-digital-twin",
    "scope": "dataset.query",
}
PLATFORM_ADMIN = {
    "sub": "platform-admin",
    "azp": "oauth2_proxy",
    "preferred_username": "platform-admin",
    "realm_access": {"roles": ["platform-admin"]},
}


def _as(client, claims: dict | None) -> None:
    if claims is None:
        client._transport.app.dependency_overrides.pop(get_optional_user, None)
        return
    user = _normalize_user(
        SimpleNamespace(
            claims=claims,
            sub=claims["sub"],
            preferred_username=claims.get("preferred_username"),
            email=None,
            iss="https://auth.example.org/realms/celine",
        ),
        token=None,
    )
    client._transport.app.dependency_overrides[get_optional_user] = lambda: user


async def _dataset(session, name: str, row_filters, *, access_level="internal") -> str:
    table = f"dataset_api.rp_{name}"
    await session.execute(text(f"CREATE TABLE {table} (id INTEGER)"))
    await session.execute(text(f"INSERT INTO {table} VALUES (1), (2)"))
    governance = {} if row_filters is None else {"rowFilters": row_filters}
    session.add(
        DatasetEntry(
            dataset_id=f"datasets.{table}",
            title=name,
            backend_type="postgres",
            backend_config={"table": table},
            expose=True,
            access_level=access_level,
            lineage={"facets": {"governance": governance}},
        )
    )
    await session.commit()
    return f"datasets.{table}"


async def _query(client, dataset: str):
    return await client.post("/query", json={"sql": f"SELECT id FROM {dataset}"})


@pytest.fixture(autouse=True)
def _fresh_plans():
    get_row_filter_registry().cache._store.clear()
    yield
    get_row_filter_registry().cache._store.clear()


@pytest.fixture
def report_only(monkeypatch):
    monkeypatch.setattr(get_settings(), "row_filters_default", "allow")


# ---------------------------------------------------------------------------
# GS-10 — both layers stated
# ---------------------------------------------------------------------------


# @verifies GS-10
def test_the_default_is_deny():
    from celine.dataset.core.config import Settings

    assert Settings().row_filters_default == "deny"


# @verifies GS-10
@pytest.mark.parametrize("claims", [VIEWER, SERVICE], ids=["viewer", "service"])
async def test_both_layers_stated_serve_the_rows(client, test_session, claims):
    dataset = await _dataset(test_session, "both", [WIDE, PERSON_ALL])
    _as(client, claims)
    resp = await _query(client, dataset)
    assert resp.status_code == 200, resp.text
    assert sorted(r["id"] for r in resp.json()["items"]) == [1, 2]


# @verifies GS-10
@pytest.mark.parametrize(
    "row_filters, missing",
    [
        ([WIDE], "person"),
        ([PERSON_ALL], "organization"),
        (None, "person and organization"),
    ],
    ids=["no person layer", "no organization layer", "nothing"],
)
@pytest.mark.parametrize("claims", [VIEWER, SERVICE], ids=["viewer", "service"])
async def test_an_undeclared_layer_is_refused_under_deny(
    client, test_session, caplog, row_filters, missing, claims
):
    caplog.set_level(logging.INFO, logger=AUDIT)
    dataset = await _dataset(test_session, "undeclared", row_filters)
    _as(client, claims)
    resp = await _query(client, dataset)
    if claims is VIEWER and row_filters is None:
        # An organization's reader never gets this far: GS-07 closes an
        # `internal` dataset declaring nothing to organizations first.
        assert resp.status_code == 403
        return
    assert resp.status_code == 403, resp.text
    assert f"declares no {missing} row policy" in resp.json()["detail"]
    [record] = [json.loads(r.getMessage()) for r in caplog.records if r.name == AUDIT]
    assert (record["event"], record["reason"]) == ("denied", "row_policy_undeclared")


# @verifies GS-10
async def test_report_only_serves_and_says_why(client, test_session, caplog, report_only):
    caplog.set_level(logging.INFO)
    dataset = await _dataset(test_session, "report", [WIDE])
    _as(client, VIEWER)
    resp = await _query(client, dataset)
    assert resp.status_code == 200, resp.text
    assert sorted(r["id"] for r in resp.json()["items"]) == [1, 2]
    warning = [
        r for r in caplog.records if r.levelno == logging.WARNING and "row_policy_undeclared" in r.getMessage()
    ]
    assert warning and dataset in warning[0].getMessage()
    [record] = [json.loads(r.getMessage()) for r in caplog.records if r.name == AUDIT]
    assert (record["event"], record["outcome"], record["reason"]) == (
        "access",
        "allowed",
        "row_policy_undeclared",
    )


# @verifies GS-10
@pytest.mark.parametrize("row_filters", [[WIDE, PERSON_NONE], [ORG_NONE, PERSON_ALL]])
@pytest.mark.parametrize("claims", [VIEWER, SERVICE], ids=["viewer", "service"])
async def test_deny_all_in_either_layer_refuses(client, test_session, row_filters, claims):
    dataset = await _dataset(test_session, "denied", row_filters)
    _as(client, claims)
    resp = await _query(client, dataset)
    assert resp.status_code == 403, resp.text
    assert "admits no row" in resp.json()["detail"]


# @verifies GS-10
@pytest.mark.parametrize(
    "row_filters", [None, [WIDE], [ORG_NONE, PERSON_ALL]], ids=["nothing", "one layer", "deny_all"]
)
async def test_the_platform_admin_bypass_comes_before_the_policy(
    client, test_session, row_filters
):
    dataset = await _dataset(test_session, "admin", row_filters)
    _as(client, PLATFORM_ADMIN)
    resp = await _query(client, dataset)
    assert resp.status_code == 200, resp.text
    assert sorted(r["id"] for r in resp.json()["items"]) == [1, 2]


# @verifies GS-10
@pytest.mark.parametrize("row_filters", [None, [PERSON_ALL]], ids=["nothing", "one layer"])
async def test_an_open_dataset_admits_its_undeclared_layers(client, test_session, row_filters):
    dataset = await _dataset(test_session, "open", row_filters, access_level="open")
    _as(client, VIEWER)
    resp = await _query(client, dataset)
    assert resp.status_code == 200, resp.text


# @verifies GS-10
async def test_an_open_dataset_still_honours_deny_all(client, test_session):
    dataset = await _dataset(test_session, "open_denied", [ORG_NONE], access_level="open")
    _as(client, VIEWER)
    assert (await _query(client, dataset)).status_code == 403


# ---------------------------------------------------------------------------
# GS-11 — unreadable is refused, never read as nothing
# ---------------------------------------------------------------------------


# @verifies GS-11
async def test_an_unknown_handler_is_refused_not_a_server_error(client, test_session):
    dataset = await _dataset(
        test_session, "unknown", [{"handler": "no_such", "binds": "organization"}, PERSON_ALL]
    )
    _as(client, SERVICE)
    resp = await _query(client, dataset)
    assert resp.status_code == 403
    assert "implements no such handler" in resp.json()["detail"]


# @verifies GS-11
@pytest.mark.parametrize(
    "row_filters",
    ["oops", ["x"], [{"args": {}}], [{"handler": "member_wide", "binds": "organization", "args": "x"}]],
    ids=["string", "string item", "no handler", "args string"],
)
async def test_a_stored_unreadable_declaration_is_refused(client, test_session, row_filters):
    """Written around the import (an older one, raw SQL): refused, not served whole."""
    dataset = await _dataset(test_session, "unreadable", row_filters)
    _as(client, SERVICE)
    resp = await _query(client, dataset)
    assert resp.status_code == 403, resp.text
    assert "cannot be read" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# GS-12 — open is for the registered user base
# ---------------------------------------------------------------------------


# @verifies GS-12
@pytest.mark.parametrize("row_filters", [None, [ORG_ALL, PERSON_ALL]], ids=["nothing", "both layers"])
async def test_an_open_dataset_refuses_anonymous(client, test_session, row_filters):
    dataset = await _dataset(test_session, "anon", row_filters, access_level="open")
    _as(client, None)
    resp = await _query(client, dataset)
    assert resp.status_code == 401
