"""`POST /admin/catalogue` refuses a row filter naming a column its table lacks (GS-09).

A filter on a column that does not exist answers 400 at query time, and only to the
callers it narrows: services and the platform administrator emit no predicate, so a
governance file naming a renamed or missing column would break every organization
reader or person while every service path stays green. The import reads the table
already; it refuses such a filter there, naming the dataset, the filter and the column,
and a refused import changes nothing in the catalogue.
"""
from __future__ import annotations

import pytest
from sqlalchemy import select, text

from celine.dataset.db.models.dataset_entry import DatasetEntry
from celine.dataset.security.auth import get_current_user
from celine.dataset.security.models import AuthenticatedUser


def _admin(client) -> None:
    client._transport.app.dependency_overrides[get_current_user] = lambda: (
        AuthenticatedUser(sub="svc", scopes=["dataset.admin"])
    )


def _entry(dataset_id: str, table: str, *row_filters: dict, title: str = "G") -> dict:
    return {
        "dataset_id": dataset_id,
        "title": title,
        "backend_type": "postgres",
        "backend_config": {"table": f"dataset_api.{table}"},
        "lineage": {"facets": {"governance": {"rowFilters": list(row_filters)}}},
    }


def _org(column: str) -> dict:
    return {"handler": "organization_match", "binds": "organization", "args": {"column": column}}


def _registry(column: str) -> dict:
    return {"handler": "rec_registry", "args": {"column": column}}


async def _import(client, *entries: dict):
    _admin(client)
    return await client.post("/admin/catalogue", json={"datasets": list(entries)})


async def _catalogue(session) -> dict[str, str]:
    session.expire_all()
    rows = (await session.execute(select(DatasetEntry))).scalars().all()
    return {row.dataset_id: row.title for row in rows}


@pytest.fixture
async def tables(test_session):
    # `migrated` carries the new column name, `unmigrated` the old one.
    await test_session.execute(
        text("CREATE TABLE dataset_api.migrated (device_id TEXT, community_id TEXT, v INTEGER)")
    )
    await test_session.execute(
        text("CREATE TABLE dataset_api.unmigrated (device_id TEXT, rec_id TEXT, v INTEGER)")
    )
    await test_session.commit()


# @verifies GS-09
@pytest.mark.parametrize(
    "table, row_filter",
    [
        ("migrated", _org("community_id")),
        ("unmigrated", _org("rec_id")),
        ("migrated", _registry("device_id")),
    ],
    ids=["organization_match", "organization_match old table", "rec_registry"],
)
async def test_a_filter_on_a_column_the_table_has_imports(client, tables, table, row_filter):
    resp = await _import(client, _entry("gs09", table, row_filter))
    assert resp.status_code == 200, resp.text


# @verifies GS-09
@pytest.mark.parametrize(
    "table, row_filter, column",
    [
        # Governance still naming the old column after the table was migrated.
        ("migrated", _org("rec_id"), "rec_id"),
        # Governance naming the new column before the table was migrated.
        ("unmigrated", _org("community_id"), "community_id"),
        ("migrated", _registry("meter_id"), "meter_id"),
    ],
    ids=["renamed column", "column not yet added", "rec_registry missing column"],
)
async def test_a_filter_on_a_column_the_table_lacks_is_refused(
    client, tables, table, row_filter, column
):
    resp = await _import(client, _entry("gs09", table, _registry("device_id"), row_filter))
    assert resp.status_code == 422, resp.text
    detail = str(resp.json()["detail"])
    assert "gs09" in detail and "rowFilters[1]" in detail and f"'{column}'" in detail
    assert f"dataset_api.{table}" in detail


# @verifies GS-09
@pytest.mark.parametrize(
    "row_filter",
    [
        {"handler": "member_wide", "binds": "organization"},
        {"handler": "organization_match", "binds": "organization", "args": {"org_type": "dso"}},
    ],
    ids=["member_wide", "organization_match by type"],
)
async def test_a_filter_that_names_no_column_is_not_refused(client, tables, row_filter):
    resp = await _import(client, _entry("gs09", "migrated", row_filter))
    assert resp.status_code == 200, resp.text


# @verifies GS-09
async def test_every_refusal_of_an_import_is_named(client, tables):
    resp = await _import(
        client,
        _entry("first", "migrated", _org("rec_id")),
        _entry("second", "unmigrated", _org("community_id")),
    )
    assert resp.status_code == 422
    detail = resp.json()["detail"]
    assert len(detail) == 2
    assert detail[0].startswith("first:") and detail[1].startswith("second:")


# @verifies GS-09
async def test_a_refused_import_changes_nothing(client, tables, test_session):
    # `kept` is catalogued, `stale` names a table that is gone: an accepted import
    # would retitle the first and remove the second.
    resp = await _import(client, _entry("kept", "migrated", _org("community_id"), title="before"))
    assert resp.status_code == 200, resp.text
    test_session.add(
        DatasetEntry(
            dataset_id="stale",
            title="stale",
            backend_type="postgres",
            backend_config={"table": "dataset_api.gone"},
        )
    )
    await test_session.commit()

    resp = await _import(
        client,
        _entry("kept", "migrated", _org("community_id"), title="after"),
        _entry("new", "migrated", _registry("device_id")),
        _entry("refused", "migrated", _org("rec_id")),
    )
    assert resp.status_code == 422, resp.text
    assert await _catalogue(test_session) == {"kept": "before", "stale": "stale"}

    # The same import without the refused entry does all three.
    resp = await _import(
        client,
        _entry("kept", "migrated", _org("community_id"), title="after"),
        _entry("new", "migrated", _registry("device_id")),
    )
    assert resp.status_code == 200, resp.text
    assert await _catalogue(test_session) == {"kept": "after", "new": "G"}
