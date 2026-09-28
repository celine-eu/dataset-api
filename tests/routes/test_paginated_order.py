"""QE-04 — the page is cut in the statement's order, on PostgreSQL.

The engine wraps the statement as `SELECT * FROM (<statement>) AS q … LIMIT
OFFSET` and carries its top-level ORDER BY onto that outer query, naming `q`'s
columns. These run statements through `POST /query` and check the rows of
each page, and the executed page query itself. They also run the statements
whose order cannot be named on `q` (a column not selected; a star over a join
whose tables share a column name): those must keep working, with no outer
ORDER BY added. Synthetic rows only.
"""

from __future__ import annotations

import pytest
import sqlglot
from sqlalchemy import text

from celine.dataset.db.models.dataset_entry import DatasetEntry

READINGS = "ds_readings"
READINGS_TABLE = "dataset_api.qe04_readings"
DEVICES = "ds_devices"
DEVICES_TABLE = "dataset_api.qe04_devices"

# Inserted out of every order the tests ask for.
ROWS = [
    ("ex-00003", 30, "b"),
    ("ex-00001", 10, "c"),
    ("ex-00005", 50, "a"),
    ("ex-00002", 20, "e"),
    ("ex-00004", 40, "d"),
]


@pytest.fixture
async def readings(test_session):
    for dataset_id, table in ((READINGS, READINGS_TABLE), (DEVICES, DEVICES_TABLE)):
        test_session.add(
            DatasetEntry(
                dataset_id=dataset_id,
                title=f"Synthetic {dataset_id}",
                backend_type="postgres",
                backend_config={"table": table},
                expose=True,
                access_level="open",
            )
        )
    await test_session.commit()
    await test_session.execute(
        text(f"CREATE TABLE {READINGS_TABLE} (device_id TEXT, value INT, label TEXT)")
    )
    await test_session.execute(
        text(f"CREATE TABLE {DEVICES_TABLE} (device_id TEXT, label TEXT)")
    )
    for device_id, value, label in ROWS:
        await test_session.execute(
            text(f"INSERT INTO {READINGS_TABLE} VALUES (:d, :v, :l)"),
            {"d": device_id, "v": value, "l": label},
        )
        await test_session.execute(
            text(f"INSERT INTO {DEVICES_TABLE} VALUES (:d, :l)"),
            {"d": device_id, "l": label.upper()},
        )
    await test_session.commit()
    try:
        yield
    finally:
        await test_session.execute(text(f"DROP TABLE IF EXISTS {READINGS_TABLE}"))
        await test_session.execute(text(f"DROP TABLE IF EXISTS {DEVICES_TABLE}"))
        await test_session.commit()


@pytest.fixture
def executed(monkeypatch):
    """The page queries the executor runs, as sent."""
    from celine.dataset.api.dataset_query import executor

    seen: list[str] = []
    original = executor.execute_rows_with_timeout

    async def _record(db, sql, params=None):
        seen.append(sql)
        return await original(db, sql, params)

    monkeypatch.setattr(executor, "execute_rows_with_timeout", _record)
    return seen


def _outer_order(page_sql: str) -> str | None:
    outer = sqlglot.parse_one(
        page_sql.replace(":limit", "1").replace(":offset", "0"), read="postgres"
    )
    order = outer.args.get("order")
    return order.sql(dialect="postgres") if order is not None else None


async def _pages(client, sql: str, limit: int) -> list[list[dict]]:
    pages = []
    for offset in range(0, len(ROWS), limit):
        resp = await client.post(
            "/query", json={"sql": sql, "limit": limit, "offset": offset}
        )
        assert resp.status_code == 200, resp.text
        pages.append(resp.json()["items"])
    return pages


# ---------------------------------------------------------------------------
# Carried onto the outer query
# ---------------------------------------------------------------------------


async def test_pages_follow_a_descending_order(client, readings, executed):
    sql = f"SELECT device_id FROM {READINGS} ORDER BY device_id DESC"
    pages = await _pages(client, sql, 2)
    assert [[r["device_id"] for r in p] for p in pages] == [
        ["ex-00005", "ex-00004"],
        ["ex-00003", "ex-00002"],
        ["ex-00001"],
    ]
    assert all(_outer_order(s) == "ORDER BY q.device_id DESC NULLS LAST" for s in executed)


async def test_an_alias_orders_the_page(client, readings, executed):
    sql = f"SELECT device_id, value AS v FROM {READINGS} ORDER BY v"
    pages = await _pages(client, sql, 3)
    assert [r["v"] for p in pages for r in p] == [10, 20, 30, 40, 50]
    assert "q.v" in _outer_order(executed[0])


async def test_a_position_orders_the_page(client, readings, executed):
    sql = f"SELECT label, device_id FROM {READINGS} ORDER BY 1 DESC"
    resp = await client.post("/query", json={"sql": sql, "limit": 2})
    assert resp.status_code == 200, resp.text
    assert [r["label"] for r in resp.json()["items"]] == ["e", "d"]
    assert "q.label DESC" in _outer_order(executed[0])


async def test_a_star_orders_the_page(client, readings, executed):
    sql = f"SELECT * FROM {READINGS} ORDER BY label"
    resp = await client.post("/query", json={"sql": sql, "limit": 2, "offset": 1})
    assert resp.status_code == 200, resp.text
    assert [r["label"] for r in resp.json()["items"]] == ["b", "c"]
    assert "q.label" in _outer_order(executed[0])


async def test_an_aggregate_orders_the_page(client, readings, executed):
    sql = (
        f"SELECT label, SUM(value) AS total FROM {READINGS} "
        "GROUP BY label ORDER BY SUM(value) DESC"
    )
    resp = await client.post("/query", json={"sql": sql, "limit": 2})
    assert resp.status_code == 200, resp.text
    assert [r["total"] for r in resp.json()["items"]] == [50, 40]
    assert "q.total DESC" in _outer_order(executed[0])


# ---------------------------------------------------------------------------
# Not nameable on q: runs as before, with no outer ORDER BY
# ---------------------------------------------------------------------------


async def test_ordering_by_a_column_not_selected_still_runs(client, readings, executed):
    sql = f"SELECT device_id FROM {READINGS} ORDER BY value DESC"
    resp = await client.post("/query", json={"sql": sql, "limit": 5})
    assert resp.status_code == 200, resp.text
    assert [r["device_id"] for r in resp.json()["items"]] == [
        "ex-00005", "ex-00004", "ex-00003", "ex-00002", "ex-00001",
    ]
    assert _outer_order(executed[0]) is None


async def test_a_star_over_a_join_sharing_column_names_still_runs(
    client, readings, executed
):
    """`q` has two `device_id` and two `label` columns: `q.device_id` would be
    ambiguous, so the outer query must not name it."""
    sql = (
        f"SELECT * FROM {READINGS} r JOIN {DEVICES} d ON r.device_id = d.device_id "
        "ORDER BY r.device_id"
    )
    resp = await client.post("/query", json={"sql": sql, "limit": 5})
    assert resp.status_code == 200, resp.text
    assert len(resp.json()["items"]) == 5
    assert _outer_order(executed[0]) is None


async def test_one_column_selected_from_each_side_of_a_join_orders_the_page(
    client, readings, executed
):
    sql = (
        f"SELECT r.device_id, d.label FROM {READINGS} r "
        f"JOIN {DEVICES} d ON r.device_id = d.device_id ORDER BY d.label DESC"
    )
    resp = await client.post("/query", json={"sql": sql, "limit": 2})
    assert resp.status_code == 200, resp.text
    assert [r["label"] for r in resp.json()["items"]] == ["E", "D"]
    assert "q.label DESC" in _outer_order(executed[0])
