"""RF-04, RF-06, RF-08 — a dataspace pull narrowed to the consent's supply points.

Integration, like the rest of the query tests: real PostgreSQL, real catalogue,
real parser, real row filter, real SQL. ds is replaced at the one seam the
executor calls (`authorize_dataplane`, `audit_query`), exactly as
`test_edr_dataspace_gate.py` does.

The shape under test is the one the platform actually runs: a data holder serves
rows keyed by supply point, and the people those rows are about consented at
another organisation entirely — which is why the allow-list arrives as typed
data keys and not as anything this holder could have resolved itself.

Every POD is obviously fake (`EX000E…`) and belongs to no deployment.
"""
from __future__ import annotations

import logging

import pytest
from fastapi import HTTPException
from sqlalchemy import text

from celine.dataset.api.dataset_query import executor as executor_mod
from celine.dataset.api.dataset_query.executor import execute_query
from celine.dataset.api.dataset_query.row_filters import registry as registry_mod
from celine.dataset.core.config import get_settings
from celine.dataset.db.models.dataset_entry import DatasetEntry
from celine.dataset.security.edr import DataPlaneDecision, EDRRequestContext

MINE = "EX000E00000001"
ALSO_MINE = "EX000E00000002"
NOBODYS = "EX000E00000009"


def _table() -> str:
    return f"{get_settings().catalogue_schema}.grid_meter_readings"


@pytest.fixture()
def fresh_registry():
    """The row-filter registry is a process-wide singleton with a TTL cache."""
    registry_mod._registry = None
    yield
    registry_mod._registry = None


@pytest.fixture()
async def readings(test_session):
    table = _table()
    await test_session.execute(
        text(f"CREATE TABLE IF NOT EXISTS {table} (pod TEXT, kwh INTEGER)")
    )
    await test_session.execute(
        text(
            f"INSERT INTO {table} VALUES "
            f"('{MINE}', 1), ('{ALSO_MINE}', 2), ('{NOBODYS}', 3)"
        )
    )
    test_session.add(
        DatasetEntry(
            dataset_id="grid_readings",
            title="grid_readings",
            backend_type="postgres",
            backend_config={"table": table},
            expose=True,
            dataspace_expose=True,
            access_level="restricted",
        )
    )
    await test_session.commit()
    yield
    await test_session.execute(text(f"DROP TABLE IF EXISTS {table}"))
    await test_session.commit()


@pytest.fixture()
def ds(monkeypatch):
    """ds's side of the seam: answers as told, records what it was told about."""
    state: dict = {"row_filter": None, "audits": []}

    async def _authorize(*, context, dataset_ids):
        return DataPlaneDecision(
            allowed=True,
            datasets=[
                {
                    "dataset_id": d,
                    "decision": "allow",
                    "row_filter": state["row_filter"],
                }
                for d in dataset_ids
            ],
            cache_ttl=60,
        )

    async def _audit(**kwargs):
        state["audits"].append(kwargs)

    monkeypatch.setattr(executor_mod, "authorize_dataplane", _authorize)
    monkeypatch.setattr(executor_mod, "audit_query", _audit)
    return state


def _edr() -> EDRRequestContext:
    return EDRRequestContext(
        agreement_id="agr-1",
        consumer_id="did:web:consumer",
        transfer_id="tr-1",
        purpose=["research"],
    )


async def _pull(session):
    return await execute_query(
        catalogue_db=session,
        datasets_db=session,
        raw_sql="SELECT pod, kwh FROM grid_readings ORDER BY kwh",
        limit=10,
        offset=0,
        user=None,
        edr_context=_edr(),
    )


def _keys_filter(*keys: str) -> dict:
    return {
        "handler": "subject_key_match",
        "args": {"column": "pod", "key_type": "pod"},
        # The collecting organisation named its members by key. It could name
        # none of them by a username this holder would recognise, which is the
        # whole reason the keys travel with the consent.
        "principals": [],
        "keys": list(keys),
    }


# ---------------------------------------------------------------------------
# RF-04 — the pull is narrowed to the keys the consent carried
# ---------------------------------------------------------------------------


async def test_only_the_consented_supply_points_are_served(
    test_session, readings, ds, fresh_registry
):
    ds["row_filter"] = _keys_filter(f"pod:{MINE}", f"pod:{ALSO_MINE}")

    result = await _pull(test_session)

    assert [row["pod"] for row in result.items] == [MINE, ALSO_MINE]


async def test_a_supply_point_nobody_consented_for_never_leaves(
    test_session, readings, ds, fresh_registry
):
    ds["row_filter"] = _keys_filter(f"pod:{MINE}")

    result = await _pull(test_session)

    assert [row["pod"] for row in result.items] == [MINE]
    assert result.total == 1


async def test_a_key_of_another_type_narrows_to_nothing(
    test_session, readings, ds, fresh_registry
):
    """No key of the column's type is "no rows", never "no filter"."""
    ds["row_filter"] = _keys_filter(f"meter_id:{MINE}")

    result = await _pull(test_session)

    assert result.items == []


async def test_an_allow_with_no_filter_still_serves_everything(
    test_session, readings, ds, fresh_registry
):
    """The half that must not regress: a dataset with no data subject."""
    ds["row_filter"] = None

    result = await _pull(test_session)

    assert len(result.items) == 3


# ---------------------------------------------------------------------------
# RF-06 — a handler this data plane cannot apply refuses the request
# ---------------------------------------------------------------------------


async def test_an_unimplemented_handler_serves_no_rows(
    test_session, readings, ds, fresh_registry
):
    """An *allow* carrying a filter says "these rows", not "this dataset"."""
    ds["row_filter"] = {
        "handler": "supply_point_registry",
        "args": {"column": "pod"},
        "keys": [f"pod:{MINE}"],
    }

    with pytest.raises(HTTPException) as exc:
        await _pull(test_session)

    assert exc.value.status_code == 403
    assert "supply_point_registry" in exc.value.detail
    assert ds["audits"] == []  # nothing was disclosed, so nothing is recorded


async def test_a_handler_that_cannot_be_delegated_refuses_too(
    test_session, readings, ds, fresh_registry
):
    ds["row_filter"] = {
        "handler": "table_pointer",
        "args": {
            "column": "pod",
            "pointer_table": "pointers",
            "pointer_key_column": "pod",
        },
        "keys": [f"pod:{MINE}"],
    }

    with pytest.raises(HTTPException) as exc:
        await _pull(test_session)

    assert exc.value.status_code == 403


async def test_a_filter_this_data_plane_cannot_read_serves_no_rows(
    test_session, readings, ds, fresh_registry
):
    """RF-02, end to end: an unknown narrowing is a refusal, not a field to skip."""
    ds["row_filter"] = {
        "handler": "subject_key_match",
        "args": {"column": "pod", "key_type": "pod"},
        "keys": [f"pod:{MINE}"],
        "except_keys": [f"pod:{ALSO_MINE}"],
    }

    with pytest.raises(HTTPException) as exc:
        await _pull(test_session)

    assert exc.value.status_code == 502


# ---------------------------------------------------------------------------
# RF-08 — the keys reach the predicate and nothing else
# ---------------------------------------------------------------------------


async def test_the_audit_disclosure_carries_no_key(
    test_session, readings, ds, fresh_registry
):
    """ds records who received which rows. Keys are not how it names them."""
    ds["row_filter"] = _keys_filter(f"pod:{MINE}")

    await _pull(test_session)

    assert len(ds["audits"]) == 1
    recorded = repr(ds["audits"][0])
    assert "EX000E" not in recorded
    assert ds["audits"][0]["authorized_subject_ids"] == []
    assert ds["audits"][0]["row_count"] == 1


async def test_the_filtered_sql_is_not_logged(
    test_session, readings, ds, fresh_registry, caplog
):
    """The predicate is a literal list of the consenting subjects."""
    ds["row_filter"] = _keys_filter(f"pod:{MINE}")

    with caplog.at_level(logging.DEBUG, logger="celine.dataset"):
        await _pull(test_session)

    assert MINE not in caplog.text
    assert "withheld" in caplog.text
