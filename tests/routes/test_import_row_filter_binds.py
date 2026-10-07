"""`POST /admin/catalogue` refuses a row filter whose `binds` is wrong (GS-08).

`binds` says whether a filter narrows people's rows or an organization's, and ds gates
consent on it (celine-utils REQ-0010). A built-in handler knows which it is, so a
governance file that disagrees is refused at import, naming the dataset and the
filter — never discovered as a dataset that is gated, or not, by mistake.
"""
from __future__ import annotations

import pytest
from sqlalchemy import text

from celine.dataset.api.dataset_query.row_filters.handlers import HANDLER_BINDS
from celine.dataset.security.auth import get_current_user
from celine.dataset.security.models import AuthenticatedUser


def _admin(client) -> None:
    client._transport.app.dependency_overrides[get_current_user] = lambda: (
        AuthenticatedUser(sub="svc", scopes=["dataset.admin"])
    )


def _entry(*row_filters: dict) -> dict:
    return {
        "dataset_id": "gs08",
        "title": "G",
        "backend_type": "postgres",
        "backend_config": {"table": "dataset_api.gs08"},
        "lineage": {"facets": {"governance": {"rowFilters": list(row_filters)}}},
    }


async def _import(client, entry: dict):
    _admin(client)
    return await client.post("/admin/catalogue", json={"datasets": [entry]})


@pytest.fixture
async def table(test_session):
    # Every column a filter below names: a missing one is refused by GS-09.
    await test_session.execute(
        text("CREATE TABLE dataset_api.gs08 (id INTEGER, device_id TEXT, community_id TEXT, u TEXT, d TEXT)")
    )
    await test_session.commit()


def test_every_built_in_handler_declares_what_it_binds():
    assert HANDLER_BINDS == {
        "direct_user_match": "person",
        "http_in_list": "person",
        "subject_key_match": "person",
        "table_pointer": "person",
        "rec_registry": "person",
        "organization_match": "organization",
        "member_wide": "organization",
    }


# @verifies GS-08
@pytest.mark.parametrize(
    "row_filter",
    [
        {"handler": "rec_registry", "args": {"column": "device_id"}},
        {"handler": "rec_registry", "binds": "person", "args": {"column": "device_id"}},
        {"handler": "organization_match", "binds": "organization", "args": {"column": "community_id"}},
        {"handler": "member_wide", "binds": "organization"},
        # Another package's handler: only a readable value is required.
        {"handler": "plugin_handler", "binds": "organization", "args": {}},
    ],
    ids=["person by default", "person stated", "organization_match", "member_wide", "plugin"],
)
async def test_a_binds_that_agrees_with_its_handler_imports(client, table, row_filter):
    resp = await _import(client, _entry(row_filter))
    assert resp.status_code == 200, resp.text


# @verifies GS-08
@pytest.mark.parametrize(
    "row_filter, expected",
    [
        # The flag forgotten on an organization filter: read as person.
        ({"handler": "organization_match", "args": {"column": "community_id"}}, "organization"),
        ({"handler": "member_wide"}, "organization"),
        # A person filter marked as an organization's: would un-gate personal data.
        ({"handler": "rec_registry", "binds": "organization", "args": {"column": "d"}}, "person"),
    ],
    ids=["organization_match unflagged", "member_wide unflagged", "rec_registry as organization"],
)
async def test_a_binds_that_contradicts_its_handler_is_refused(client, table, row_filter, expected):
    resp = await _import(client, _entry({"handler": "direct_user_match", "args": {"column": "u"}}, row_filter))
    assert resp.status_code == 422
    detail = str(resp.json()["detail"])
    assert "gs08" in detail and "rowFilters[1]" in detail and f"binds: {expected}" in detail


# @verifies GS-08
@pytest.mark.parametrize("value", ["persn", "community", ""])
async def test_an_unreadable_binds_is_refused(client, table, value):
    resp = await _import(client, _entry({"handler": "plugin_handler", "binds": value}))
    assert resp.status_code == 422
    assert "gs08" in str(resp.json()["detail"])
