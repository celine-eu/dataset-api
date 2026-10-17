"""`POST /admin/catalogue` refuses a row policy it cannot read (GS-10, GS-11).

A declaration that is skipped reads as a layer nobody declared. Before the deny
default that served every row; under it, a typo would refuse a dataset for a reason
nobody can see. Either way the import is the place to say so, naming the dataset.
"""
from __future__ import annotations

import pytest
from sqlalchemy import text

from celine.dataset.security.auth import get_current_user
from celine.dataset.security.models import AuthenticatedUser


def _admin(client) -> None:
    client._transport.app.dependency_overrides[get_current_user] = lambda: (
        AuthenticatedUser(sub="svc", scopes=["dataset.admin"])
    )


def _entry(row_filters) -> dict:
    return {
        "dataset_id": "gs11",
        "title": "G",
        "backend_type": "postgres",
        "backend_config": {"table": "dataset_api.gs11"},
        "lineage": {"facets": {"governance": {"rowFilters": row_filters}}},
    }


async def _import(client, entry: dict):
    _admin(client)
    return await client.post("/admin/catalogue", json={"datasets": [entry]})


@pytest.fixture
async def table(test_session):
    await test_session.execute(text("CREATE TABLE dataset_api.gs11 (id INTEGER, u TEXT)"))
    await test_session.commit()


# @verifies GS-10
@pytest.mark.parametrize("binds", ["person", "organization"])
@pytest.mark.parametrize("handler", ["allow_all", "deny_all"])
async def test_a_layer_verdict_stating_its_layer_imports(client, table, handler, binds):
    resp = await _import(client, _entry([{"handler": handler, "binds": binds}]))
    assert resp.status_code == 200, resp.text


# @verifies GS-10
@pytest.mark.parametrize("handler", ["allow_all", "deny_all"])
async def test_a_layer_verdict_without_binds_is_refused(client, table, handler):
    resp = await _import(client, _entry([{"handler": handler}]))
    assert resp.status_code == 422
    assert "must state binds" in str(resp.json()["detail"])


# @verifies GS-11
@pytest.mark.parametrize(
    "row_filters, expected",
    [
        ("oops", "not a list"),
        (["x"], "not a mapping"),
        ([{"args": {}}], "names no handler"),
        ([{"handler": "direct_user_match", "args": "u"}], "args is not a mapping"),
    ],
    ids=["string", "string item", "no handler", "args string"],
)
async def test_an_unreadable_row_policy_is_refused(client, table, row_filters, expected):
    resp = await _import(client, _entry(row_filters))
    assert resp.status_code == 422
    detail = str(resp.json()["detail"])
    assert "gs11" in detail and expected in detail
