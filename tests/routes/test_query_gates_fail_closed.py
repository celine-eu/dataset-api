"""Every query gate fails closed.

- A dataset that states no `backend_config.table` is queried through the table its
  id names, and is access-checked and row-filtered like any other. It used to be
  left unmapped *and* unchecked: its logical name reached SQL with no decision.
- A backend with no SQL table (s3, fs, …) is not queryable.
- An entry that states no `access_level` is `internal`, not `open`.
- `secret`, and a level nobody can read, answer as an unexposed dataset does.
- A two-part reference matches a catalogue id literally: `_` is not a wildcard,
  and two matches are refused rather than picked from.
"""
from __future__ import annotations

import pytest
from sqlalchemy import text

from celine.dataset.db.models.dataset_entry import DatasetEntry


async def _table(test_session, table: str) -> None:
    await test_session.execute(text(f"CREATE TABLE {table} (id INTEGER, user_id TEXT)"))
    await test_session.execute(text(f"INSERT INTO {table} VALUES (1, 'a'), (2, 'b')"))
    await test_session.commit()


async def _entry(test_session, dataset_id: str, **kw) -> None:
    fields = dict(
        title="DS",
        backend_type="postgres",
        backend_config={},
        expose=True,
        access_level="open",
    )
    fields.update(kw)
    test_session.add(DatasetEntry(dataset_id=dataset_id, **fields))
    await test_session.commit()


async def _query(client, sql: str):
    return await client.post("/query", json={"sql": sql})


# ---------------------------------------------------------------------------
# A table is needed; the one the id names is used, safely
# ---------------------------------------------------------------------------


async def test_an_entry_without_a_table_is_queried_through_the_table_its_id_names(
    client, test_session
):
    await _table(test_session, "dataset_api.no_table_open")
    await _entry(test_session, "datasets.dataset_api.no_table_open")
    resp = await _query(client, "SELECT id FROM datasets.dataset_api.no_table_open")
    assert resp.status_code == 200, resp.text
    assert resp.json()["total"] == 2


async def test_an_entry_without_a_table_is_still_access_checked(client, test_session):
    """The regression: this dataset used to skip `enforce_dataset_access`."""
    await _table(test_session, "dataset_api.no_table_internal")
    await _entry(
        test_session, "datasets.dataset_api.no_table_internal", access_level="internal"
    )
    resp = await _query(client, "SELECT id FROM datasets.dataset_api.no_table_internal")
    assert resp.status_code == 401


async def test_an_entry_without_a_table_is_still_row_filtered(client, test_session):
    await _table(test_session, "dataset_api.no_table_filtered")
    await _entry(
        test_session,
        "datasets.dataset_api.no_table_filtered",
        lineage={
            "facets": {
                "governance": {
                    "rowFilters": [
                        {"handler": "direct_user_match", "args": {"column": "user_id"}}
                    ]
                }
            }
        },
    )
    resp = await _query(client, "SELECT id FROM datasets.dataset_api.no_table_filtered")
    assert resp.status_code == 401


async def test_a_backend_without_a_sql_table_is_not_queryable(client, test_session):
    await _entry(
        test_session,
        "datasets.files.report",
        backend_type="s3",
        backend_config={"path": "s3://bucket/key"},
    )
    resp = await _query(client, "SELECT * FROM datasets.files.report")
    assert resp.status_code == 400
    assert "not queryable" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# Access levels
# ---------------------------------------------------------------------------


async def test_an_entry_stating_no_level_is_internal(client, test_session):
    """It used to be `open`: served with no authentication and no policy."""
    await _table(test_session, "dataset_api.null_level")
    await _entry(
        test_session,
        "datasets.dataset_api.null_level",
        backend_config={"table": "dataset_api.null_level"},
    )
    # The ORM column defaults to "internal" on insert; a NULL is what a row
    # written around it holds (older rows, raw SQL), so it is written directly.
    await test_session.execute(
        text(
            "UPDATE dataset_api.datasets_entries SET access_level = NULL "
            "WHERE dataset_id = 'datasets.dataset_api.null_level'"
        )
    )
    await test_session.commit()
    resp = await _query(client, "SELECT id FROM datasets.dataset_api.null_level")
    assert resp.status_code == 401


@pytest.mark.parametrize("level", ["secret", "external"])
async def test_secret_and_unreadable_levels_answer_as_unexposed(
    client, test_session, level
):
    await _table(test_session, "dataset_api.hidden_level")
    await _entry(
        test_session,
        "datasets.dataset_api.hidden_level",
        backend_config={"table": "dataset_api.hidden_level"},
        access_level=level,
    )
    resp = await _query(client, "SELECT id FROM datasets.dataset_api.hidden_level")
    assert resp.status_code == 403
    assert resp.json()["detail"] == "Dataset not available"


async def test_an_unexposed_dataset_answers_the_same(client, test_session):
    await _table(test_session, "dataset_api.unexposed")
    await _entry(
        test_session,
        "datasets.dataset_api.unexposed",
        backend_config={"table": "dataset_api.unexposed"},
        expose=False,
    )
    resp = await _query(client, "SELECT id FROM datasets.dataset_api.unexposed")
    assert resp.status_code == 403
    assert resp.json()["detail"] == "Dataset not available"


# ---------------------------------------------------------------------------
# Two-part references resolve literally
# ---------------------------------------------------------------------------


async def test_an_underscore_in_a_reference_is_not_a_wildcard(client, test_session):
    await _table(test_session, "dataset_api.wild")
    await _entry(
        test_session, "datasets.abXcd.wild", backend_config={"table": "dataset_api.wild"}
    )
    resp = await _query(client, "SELECT id FROM ab_cd.wild")
    assert resp.status_code == 400
    assert "unknown datasets" in resp.json()["detail"]


async def test_the_literal_suffix_still_resolves(client, test_session):
    await _table(test_session, "dataset_api.suffix_ok")
    await _entry(
        test_session,
        "datasets.ds_dev_gold.suffix_ok",
        backend_config={"table": "dataset_api.suffix_ok"},
    )
    resp = await _query(client, "SELECT id FROM ds_dev_gold.suffix_ok")
    assert resp.status_code == 200, resp.text


async def test_an_ambiguous_suffix_is_refused(client, test_session):
    await _table(test_session, "dataset_api.amb")
    for prefix in ("datasets", "singer"):
        await _entry(
            test_session,
            f"{prefix}.gold.amb",
            backend_config={"table": "dataset_api.amb"},
        )
    resp = await _query(client, "SELECT id FROM gold.amb")
    assert resp.status_code == 400
    assert "ambiguous" in resp.json()["detail"]
