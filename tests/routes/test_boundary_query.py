"""QE-01 — a boundary-style query runs through the query engine on PostGIS.

The parser suites prove `ST_AsGeoJSON` and `ST_Simplify` are admitted; this
proves the admitted statement executes: a point-in-shape lookup on a table of
synthetic polygons, answered with the shape as GeoJSON, simplified in the
statement. The geometries are made up (squares off the Gulf of Guinea, one
detailed ring), and the ids only have the reference-boundary shape.
"""

import json
import logging
import math

import pytest
from sqlalchemy import text

from celine.dataset.db.models.dataset_entry import DatasetEntry

DATASET_ID = "ds_boundaries"
TABLE = "dataset_api.boundaries_test"


def _detailed_ring_wkt(cx: float, cy: float, r: float, n: int) -> str:
    """A closed ring of `n` vertices: the detail a published boundary carries."""
    pts = [
        (cx + r * math.cos(2 * math.pi * i / n), cy + r * math.sin(2 * math.pi * i / n))
        for i in range(n)
    ]
    pts.append(pts[0])
    return "POLYGON((" + ", ".join(f"{x} {y}" for x, y in pts) + "))"


@pytest.fixture
async def boundaries(test_session):
    test_session.add(
        DatasetEntry(
            dataset_id=DATASET_ID,
            title="Synthetic boundaries",
            backend_type="postgres",
            backend_config={"table": TABLE},
            expose=True,
            access_level="open",
        )
    )
    await test_session.commit()
    await test_session.execute(
        text(f"CREATE TABLE {TABLE} (cod_ac TEXT, geometry geometry(Polygon, 4326))")
    )
    # Two squares sharing the edge x = 1, and one detailed ring further east.
    await test_session.execute(
        text(
            f"""
            INSERT INTO {TABLE} (cod_ac, geometry) VALUES
              ('AC000E00001', ST_GeomFromText('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))', 4326)),
              ('AC000E00002', ST_GeomFromText('POLYGON((1 0, 2 0, 2 1, 1 1, 1 0))', 4326)),
              ('AC000E00003', ST_GeomFromText(:ring, 4326))
            """
        ),
        {"ring": _detailed_ring_wkt(5.0, 0.5, 0.4, 720)},
    )
    await test_session.commit()
    try:
        yield
    finally:
        await test_session.execute(text(f"DROP TABLE IF EXISTS {TABLE}"))
        await test_session.commit()


async def test_point_in_shape_returns_the_boundary_id(client, boundaries):
    resp = await client.post(
        "/query",
        json={
            "sql": f"SELECT cod_ac FROM {DATASET_ID} "
            "WHERE ST_Contains(geometry, ST_SetSRID(ST_Point(0.5, 0.5), 4326)) "
            "ORDER BY cod_ac"
        },
    )
    assert resp.status_code == 200, resp.text
    assert [r["cod_ac"] for r in resp.json()["items"]] == ["AC000E00001"]


async def test_point_outside_every_shape_returns_nothing(client, boundaries):
    resp = await client.post(
        "/query",
        json={
            "sql": f"SELECT cod_ac FROM {DATASET_ID} "
            "WHERE ST_Contains(geometry, ST_SetSRID(ST_Point(3.0, 0.5), 4326))"
        },
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["items"] == []


async def test_shape_is_answered_as_simplified_geojson(client, boundaries):
    resp = await client.post(
        "/query",
        json={
            "sql": f"SELECT cod_ac, "
            "ST_AsGeoJSON(geometry) AS full_shape, "
            "ST_AsGeoJSON(ST_Simplify(geometry, 0.01)) AS shape "
            f"FROM {DATASET_ID} WHERE cod_ac IN ('AC000E00001', 'AC000E00003') "
            "ORDER BY cod_ac"
        },
    )
    assert resp.status_code == 200, resp.text
    items = resp.json()["items"]
    assert [r["cod_ac"] for r in items] == ["AC000E00001", "AC000E00003"]

    for row in items:
        shape = json.loads(row["shape"])
        assert shape["type"] == "Polygon"

    square, ring = items
    # A square has nothing to simplify.
    assert json.loads(square["shape"]) == json.loads(square["full_shape"])
    # The detailed ring leaves the database far smaller than it is stored.
    full = json.loads(ring["full_shape"])["coordinates"][0]
    simplified = json.loads(ring["shape"])["coordinates"][0]
    assert len(full) == 721
    assert 4 <= len(simplified) < len(full) // 4


# ---------------------------------------------------------------------------
# A point on a shared edge: the Digital Twin's `boundary_at_point` statement
# ---------------------------------------------------------------------------
#
# The DT asks "which boundary covers this point" as `ST_Intersects(shape,
# point) … ORDER BY cod_ac` with the request's `limit` of 1 (a LIMIT in the
# statement is refused, see below). `ST_Intersects` of a polygon and a point is
# true on the polygon's edge, so a point on the edge two squares share is
# covered by both, and the lowest id must be the answer. The squares share the
# edge x = 1; the point sits on it.

EDGE_SQL = (
    f"SELECT cod_ac FROM {DATASET_ID} "
    "WHERE ST_Intersects(geometry, ST_SetSRID(ST_Point(1.0, 0.05), 4326)) "
    "ORDER BY cod_ac"
)


async def test_point_on_a_shared_edge_is_covered_by_both_shapes(client, boundaries):
    resp = await client.post("/query", json={"sql": EDGE_SQL})
    assert resp.status_code == 200, resp.text
    assert [r["cod_ac"] for r in resp.json()["items"]] == ["AC000E00001", "AC000E00002"]


async def test_point_on_a_shared_edge_with_limit_one_answers_the_lowest_id(
    client, boundaries
):
    resp = await client.post("/query", json={"sql": EDGE_SQL, "limit": 1})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert [r["cod_ac"] for r in body["items"]] == ["AC000E00001"]
    assert body["total"] == 2


async def test_limit_one_follows_the_statements_order_not_insertion_order(
    client, boundaries
):
    """Descending, the same lookup answers the other square: the ORDER BY decides."""
    resp = await client.post(
        "/query", json={"sql": EDGE_SQL + " DESC", "limit": 1}
    )
    assert resp.status_code == 200, resp.text
    assert [r["cod_ac"] for r in resp.json()["items"]] == ["AC000E00002"]


async def test_the_edge_lookup_with_contains_misses_the_edge(client, boundaries):
    """Why the DT uses `ST_Intersects`: `ST_Contains` excludes the boundary."""
    resp = await client.post(
        "/query", json={"sql": EDGE_SQL.replace("ST_Intersects", "ST_Contains")}
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["items"] == []


async def test_a_limit_in_the_statement_is_refused(client, boundaries):
    """The row cap is the request's `limit`, applied server-side, never the SQL's."""
    resp = await client.post("/query", json={"sql": EDGE_SQL + " LIMIT 1"})
    assert resp.status_code == 400
    assert "LIMIT" in resp.text


async def test_negative_coordinates_execute_on_postgis(client, boundaries):
    """QE-02 end to end: a point with negative coordinates runs; it is in no shape."""
    resp = await client.post(
        "/query",
        json={
            "sql": f"SELECT cod_ac FROM {DATASET_ID} "
            "WHERE ST_Intersects(geometry, ST_SetSRID(ST_Point(-0.05, -0.05), 4326))"
        },
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["items"] == []


async def test_negative_coordinates_are_values_not_decoration(client, boundaries):
    """A point just west of x = 0 is within 0.1 of the first square only."""
    resp = await client.post(
        "/query",
        json={
            "sql": f"SELECT cod_ac FROM {DATASET_ID} "
            "WHERE ST_Distance(geometry, ST_SetSRID(ST_Point(-0.05, 0.05), 4326)) < 0.1 "
            "ORDER BY cod_ac"
        },
    )
    assert resp.status_code == 200, resp.text
    assert [r["cod_ac"] for r in resp.json()["items"]] == ["AC000E00001"]


# ---------------------------------------------------------------------------
# QE-03 — the query log carries the statement's shape, never its literals
# ---------------------------------------------------------------------------
#
# A boundary lookup's point is a supply address's coordinates. Each executed,
# failed and refused statement below carries two distinctive coordinates; none
# of them may reach a log at any level, while the log still shows the statement
# happened.

LON, LAT = "0.0371", "0.0829"
POINT = f"ST_SetSRID(ST_Point({LON}, {LAT}), 4326)"


def _assert_no_coordinates(caplog):
    assert LON not in caplog.text
    assert LAT not in caplog.text
    # sqlglot may re-render a number; the digits are what must not appear.
    assert "0371" not in caplog.text and "0829" not in caplog.text


async def test_an_executed_lookup_logs_no_coordinates(client, boundaries, caplog):
    with caplog.at_level(logging.DEBUG, logger="celine.dataset"):
        resp = await client.post(
            "/query",
            json={
                "sql": f"SELECT cod_ac FROM {DATASET_ID} "
                f"WHERE ST_Intersects(geometry, {POINT}) ORDER BY cod_ac",
                "limit": 1,
            },
        )
    assert resp.status_code == 200, resp.text
    assert [r["cod_ac"] for r in resp.json()["items"]] == ["AC000E00001"]
    _assert_no_coordinates(caplog)
    # The shape is still there for whoever debugs it.
    assert "Parsing raw SQL" in caplog.text
    assert "ST_POINT(?, ?)" in caplog.text.upper()
    assert "Complete SQL (after table mapping)" in caplog.text


async def test_a_failed_lookup_logs_no_coordinates(client, boundaries, caplog):
    """Fails in PostgreSQL (mixed SRIDs): the driver error path logs the shape."""
    with caplog.at_level(logging.DEBUG, logger="celine.dataset"):
        resp = await client.post(
            "/query",
            json={
                "sql": f"SELECT cod_ac FROM {DATASET_ID} WHERE ST_Intersects("
                f"geometry, ST_SetSRID(ST_Point({LON}, {LAT}), 3857))"
            },
        )
    assert resp.status_code == 400, resp.text
    assert "Query failed sql=" in caplog.text
    _assert_no_coordinates(caplog)


async def test_a_statement_that_does_not_parse_logs_no_coordinates(
    client, boundaries, caplog
):
    with caplog.at_level(logging.DEBUG, logger="celine.dataset"):
        resp = await client.post(
            "/query",
            json={
                "sql": f"SELECT cod_ac FROM {DATASET_ID} "
                f"WHERE ST_Intersects(geometry, ST_Point({LON}, {LAT})"
            },
        )
    assert resp.status_code == 400
    assert "Invalid SQL syntax" in caplog.text
    _assert_no_coordinates(caplog)


async def test_a_refused_tautology_logs_no_coordinates(client, boundaries, caplog):
    with caplog.at_level(logging.DEBUG, logger="celine.dataset"):
        resp = await client.post(
            "/query",
            json={
                "sql": f"SELECT cod_ac FROM {DATASET_ID} "
                f"WHERE ST_Intersects(geometry, {POINT}) OR {LON} = {LON}"
            },
        )
    assert resp.status_code == 400
    assert "Tautological predicate" in caplog.text
    _assert_no_coordinates(caplog)


def _statement_error(sql, params):
    """A SQLAlchemy error that is not a DBAPIError: its text repeats the statement."""
    from sqlalchemy.exc import StatementError

    return StatementError("could not bind", sql, params, ValueError("bind"))


@pytest.mark.parametrize(
    ("target", "message"),
    [
        ("execute_scalar_with_timeout", "Count query failed"),
        ("execute_rows_with_timeout", "Query execution failed"),
    ],
)
async def test_a_non_driver_error_logs_no_coordinates(
    client, boundaries, caplog, monkeypatch, target, message
):
    """QE-03: a non-DBAPI error escapes `_execute_sql_with_timeout`'s handler;
    the executor's catch-all logs its type and the statement's shape, never the
    error text or a traceback, both of which carry the statement's literals."""
    from celine.dataset.api.dataset_query import executor

    async def _fail(db, sql, params=None):
        raise _statement_error(sql, params)

    monkeypatch.setattr(executor, target, _fail)
    with caplog.at_level(logging.DEBUG, logger="celine.dataset"):
        resp = await client.post(
            "/query",
            json={
                "sql": f"SELECT cod_ac FROM {DATASET_ID} "
                f"WHERE ST_Intersects(geometry, {POINT})"
            },
        )
    assert resp.status_code == 500, resp.text
    assert message in caplog.text
    assert "StatementError" in caplog.text
    _assert_no_coordinates(caplog)
    assert not any(r.exc_info for r in caplog.records if message in r.getMessage())
