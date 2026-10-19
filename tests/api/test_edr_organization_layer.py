"""GS-13 — through an agreement, the consumer's organization reads its own rows only.

Integration: real PostgreSQL, real catalogue, real parser and filters. ds is
replaced at the one seam the executor calls (`authorize_dataplane`,
`audit_query`), as in `test_edr_subject_key_filter.py`.

A grid table holds two operators' rows, `example-dso-a` and `example-dso-b`, keyed
by `dso_id`. Before this, a dataset whose filters all bound an organization was
served whole through any agreement — every operator's rows.
"""
from __future__ import annotations

import pytest
from fastapi import HTTPException
from sqlalchemy import text

from celine.dataset.api.dataset_query import executor as executor_mod
from celine.dataset.api.dataset_query.executor import execute_query
from celine.dataset.api.dataset_query.row_filters import registry as registry_mod
from celine.dataset.core.config import get_settings
from celine.dataset.db.models.dataset_entry import DatasetEntry
from celine.dataset.security.audit import Refused
from celine.dataset.security.edr import DataPlaneDecision, EDRRequestContext

ORG_MATCH = {
    "handler": "organization_match",
    "binds": "organization",
    "args": {"column": "dso_id"},
}
PERSON_ALL = {"handler": "allow_all", "binds": "person"}
ORG_ALL = {"handler": "allow_all", "binds": "organization"}
UNSET = object()


def _table() -> str:
    return f"{get_settings().catalogue_schema}.edr_grid_lines"


@pytest.fixture()
def fresh_registry():
    registry_mod._registry = None
    yield
    registry_mod._registry = None


@pytest.fixture()
def catalogue(test_session):
    """Seeds the grid table and an entry declaring the given row filters."""

    async def _seed(row_filters, *, access_level="internal"):
        table = _table()
        await test_session.execute(text(f"CREATE TABLE {table} (id INTEGER, dso_id TEXT)"))
        await test_session.execute(
            text(
                f"INSERT INTO {table} VALUES "
                "(1, 'example-dso-a'), (2, 'example-dso-a'), (3, 'example-dso-b')"
            )
        )
        governance = {} if row_filters is None else {"rowFilters": row_filters}
        test_session.add(
            DatasetEntry(
                dataset_id="grid_lines",
                title="grid_lines",
                backend_type="postgres",
                backend_config={"table": table},
                expose=True,
                dataspace_expose=True,
                access_level=access_level,
                lineage={"facets": {"governance": governance}},
            )
        )
        await test_session.commit()

    return _seed


@pytest.fixture(autouse=True)
async def _drop(test_session):
    yield
    await test_session.rollback()
    await test_session.execute(text(f"DROP TABLE IF EXISTS {_table()}"))
    await test_session.commit()


@pytest.fixture()
def ds(monkeypatch):
    """ds's verdict: `organizations` as told, `row_filter` as told, listed or not."""
    state: dict = {"organizations": UNSET, "row_filter": None, "listed": True}

    async def _authorize(*, context, dataset_ids):
        entries = []
        if state["listed"]:
            for d in dataset_ids:
                entry = {"dataset_id": d, "decision": "allow", "row_filter": state["row_filter"]}
                if state["organizations"] is not UNSET:
                    entry["organizations"] = state["organizations"]
                entries.append(entry)
        return DataPlaneDecision(allowed=True, datasets=entries, cache_ttl=60)

    async def _audit(**kwargs):
        pass

    monkeypatch.setattr(executor_mod, "authorize_dataplane", _authorize)
    monkeypatch.setattr(executor_mod, "audit_query", _audit)
    return state


def _edr(agreement: str = "agr-1") -> EDRRequestContext:
    return EDRRequestContext(
        agreement_id=agreement,
        consumer_id="did:web:consumer.example.org",
        transfer_id="tr-1",
        purpose=["research"],
    )


async def _ids(session, agreement: str = "agr-1") -> list[int]:
    result = await execute_query(
        catalogue_db=session,
        datasets_db=session,
        raw_sql="SELECT id FROM grid_lines ORDER BY id",
        limit=10,
        offset=0,
        user=None,
        edr_context=_edr(agreement),
    )
    return [row["id"] for row in result.items]


# @verifies GS-13
async def test_the_consumer_organization_reads_its_own_rows(
    test_session, catalogue, ds, fresh_registry
):
    await catalogue([ORG_MATCH, PERSON_ALL])
    ds["organizations"] = ["example-dso-a"]
    assert await _ids(test_session) == [1, 2]


# @verifies GS-13
async def test_two_organizations_never_share_a_plan(
    test_session, catalogue, ds, fresh_registry
):
    await catalogue([ORG_MATCH, PERSON_ALL])
    ds["organizations"] = ["example-dso-a"]
    assert await _ids(test_session, "agr-a") == [1, 2]
    ds["organizations"] = ["example-dso-b"]
    assert await _ids(test_session, "agr-b") == [3]


# @verifies GS-13
@pytest.mark.parametrize("organizations", [UNSET, None, []], ids=["absent", "null", "empty"])
async def test_no_established_organization_reads_nothing(
    test_session, catalogue, ds, fresh_registry, organizations
):
    """An older ds, or none established: the organization layer narrows to nothing."""
    await catalogue([ORG_MATCH, PERSON_ALL])
    ds["organizations"] = organizations
    assert await _ids(test_session) == []


# @verifies GS-13
async def test_ds_s_organization_filter_naming_nobody_is_not_the_person_layer(
    test_session, catalogue, ds, fresh_registry
):
    """ds X-17 sends the organization filter with nobody in it; it is not consent."""
    await catalogue([ORG_MATCH, PERSON_ALL])
    ds["row_filter"] = {
        "handler": "organization_match",
        "args": {"column": "dso_id"},
        "principals": [],
        "keys": [],
    }
    ds["organizations"] = ["example-dso-b"]
    assert await _ids(test_session) == [3]


# @verifies GS-13
async def test_an_unlisted_dataset_follows_its_local_declaration(
    test_session, catalogue, ds, fresh_registry
):
    """Not in the verdict → the local declaration, with no organization."""
    await catalogue([ORG_MATCH, PERSON_ALL])
    ds["listed"] = False
    assert await _ids(test_session) == []


# @verifies GS-13
async def test_allow_all_on_both_layers_serves_the_agreement_every_row(
    test_session, catalogue, ds, fresh_registry
):
    await catalogue([ORG_ALL, PERSON_ALL])
    assert await _ids(test_session) == [1, 2, 3]


# @verifies GS-13
async def test_an_undeclared_layer_follows_the_default_on_the_dataspace_path(
    test_session, catalogue, ds, fresh_registry
):
    await catalogue([ORG_MATCH])
    ds["organizations"] = ["example-dso-a"]
    with pytest.raises(Refused) as exc:
        await _ids(test_session)
    assert "declares no person row policy" in exc.value.detail


# @verifies GS-13
async def test_deny_all_is_visible_on_the_dataspace_path(
    test_session, catalogue, ds, fresh_registry
):
    await catalogue([{"handler": "deny_all", "binds": "organization"}, PERSON_ALL])
    with pytest.raises(Refused) as exc:
        await _ids(test_session)
    assert "admits no row" in exc.value.detail


# @verifies GS-13
async def test_a_local_person_filter_without_ds_s_subjects_is_refused(
    test_session, catalogue, ds, fresh_registry
):
    """The ends disagree: this end narrows per person, ds named nobody."""
    await catalogue([ORG_ALL, {"handler": "rec_registry", "args": {"column": "id"}}])
    with pytest.raises(Refused) as exc:
        await _ids(test_session)
    assert "carries none" in exc.value.detail


# @verifies GS-13
async def test_member_wide_is_refused_through_an_agreement(
    test_session, catalogue, ds, fresh_registry
):
    await catalogue([{"handler": "member_wide", "binds": "organization"}, PERSON_ALL])
    ds["organizations"] = ["example-dso-a"]
    with pytest.raises(Refused) as exc:
        await _ids(test_session)
    assert exc.value.reason == "row_filter_unenforceable"


# @verifies GS-13
async def test_unreadable_organizations_are_a_bad_gateway(
    test_session, catalogue, ds, fresh_registry
):
    await catalogue([ORG_MATCH, PERSON_ALL])
    ds["organizations"] = "example-dso-a"
    with pytest.raises(HTTPException) as exc:
        await _ids(test_session)
    assert exc.value.status_code == 502
