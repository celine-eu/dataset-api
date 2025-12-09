# tests/test_dataset_query.py
import pytest
from sqlalchemy import text

from dataset.db.models import DatasetEntry


@pytest.mark.asyncio
async def test_query_open_dataset_simple(client, test_session):
    table = "dataset_api.test_table"

    try:
        ds = DatasetEntry(
            dataset_id="ds_open",
            title="OpenDS",
            backend_type="postgres",
            backend_config={"table": table},
            expose=True,
            access_level="open",
        )
        test_session.add(ds)
        await test_session.commit()

        await test_session.execute(
            text(
                f"""
            CREATE TABLE {table} (
                id INTEGER,
                temperature INTEGER,
                city TEXT
            )
        """
            )
        )

        await test_session.execute(
            text(
                f"""
            INSERT INTO {table} (id, temperature, city) VALUES
              (1, 25, 'Milan'),
              (2, 10, 'London'),
              (3, 30, 'Milan')
        """
            )
        )
        await test_session.commit()

        resp = await client.get("/dataset/ds_open/query")
        assert resp.status_code == 200
        assert len(resp.json()["items"]) == 3

    finally:
        await test_session.execute(text(f"DROP TABLE IF EXISTS {table}"))
        await test_session.commit()


@pytest.mark.asyncio
async def test_query_sql_filter(client, test_session):
    table = "dataset_api.filter_table"

    try:
        ds = DatasetEntry(
            dataset_id="ds_filter",
            title="FilterDS",
            backend_type="postgres",
            backend_config={"table": table},
            expose=True,
            access_level="open",
        )
        test_session.add(ds)
        await test_session.commit()

        await test_session.execute(
            text(
                f"""
            CREATE TABLE {table} (
                id INTEGER,
                temperature INTEGER,
                city TEXT
            )
        """
            )
        )

        await test_session.execute(
            text(
                f"""
            INSERT INTO {table} (id, temperature, city) VALUES
              (1, 20, 'Rome'),
              (2, 25, 'Milan'),
              (3, 30, 'Rome')
        """
            )
        )
        await test_session.commit()

        resp = await client.get(
            "/dataset/ds_filter/query",
            params={"filter": "temperature > 22 AND city = 'Milan'"},
        )

        assert resp.status_code == 200
        items = resp.json()["items"]
        assert len(items) == 1
        assert items[0]["id"] == 2

    finally:
        await test_session.execute(text(f"DROP TABLE IF EXISTS {table}"))
        await test_session.commit()


@pytest.mark.asyncio
async def test_query_pagination(client, test_session):
    table = "dataset_api.page_table"

    try:
        ds = DatasetEntry(
            dataset_id="ds_page",
            title="PageDS",
            backend_type="postgres",
            backend_config={"table": table},
            expose=True,
            access_level="open",
        )
        test_session.add(ds)
        await test_session.commit()

        await test_session.execute(text(f"CREATE TABLE {table} (id INTEGER)"))
        await test_session.execute(
            text(f"INSERT INTO {table} (id) VALUES (1), (2), (3), (4), (5)")
        )
        await test_session.commit()

        resp = await client.get(
            "/dataset/ds_page/query", params={"limit": 2, "offset": 2}
        )
        assert resp.status_code == 200

        items = resp.json()["items"]
        assert len(items) == 2
        assert [i["id"] for i in items] == [3, 4]

    finally:
        await test_session.execute(text(f"DROP TABLE IF EXISTS {table}"))
        await test_session.commit()


@pytest.mark.asyncio
async def test_query_nonexistent_dataset(client):
    resp = await client.get("/dataset/missing/query")
    assert resp.status_code in (400, 404)


@pytest.mark.asyncio
async def test_query_unsupported_backend(client, test_session):
    ds = DatasetEntry(
        dataset_id="ds_s3",
        title="S3DS",
        backend_type="s3",
        backend_config={"path": "s3://bucket/key"},
        expose=True,
    )
    test_session.add(ds)
    await test_session.commit()

    resp = await client.get("/dataset/ds_s3/query")
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_query_sql_injection_blocked(client, test_session):
    table = "dataset_api.inj_table"

    try:
        ds = DatasetEntry(
            dataset_id="ds_inj",
            title="InjDS",
            backend_type="postgres",
            backend_config={"table": table},
            expose=True,
        )
        test_session.add(ds)
        await test_session.commit()

        await test_session.execute(
            text(
                f"""
            CREATE TABLE {table} (
                id INTEGER,
                value TEXT
            )
        """
            )
        )
        await test_session.execute(
            text(
                f"""
            INSERT INTO {table} (id, value)
            VALUES (1, 'safe'), (2, 'safe')
        """
            )
        )
        await test_session.commit()

        resp = await client.get(
            "/dataset/ds_inj/query",
            params={"filter": "value = 'safe'; DROP TABLE inj_table"},
        )

        assert resp.status_code == 400

    finally:
        await test_session.execute(text(f"DROP TABLE IF EXISTS {table}"))
        await test_session.commit()


@pytest.mark.asyncio
async def test_query_geospatial_filter(client, test_session):
    table = "dataset_api.geo_table"

    try:
        ds = DatasetEntry(
            dataset_id="ds_geo",
            title="GeoDS",
            backend_type="postgres",
            backend_config={"table": table},
            expose=True,
        )
        test_session.add(ds)
        await test_session.commit()

        await test_session.execute(
            text(
                f"""
            CREATE TABLE {table} (
                id INTEGER,
                geom geometry(Point, 4326)
            )
        """
            )
        )

        await test_session.execute(
            text(
                f"""
            INSERT INTO {table} (id, geom) VALUES
              (1, ST_Point(9.0, 45.0)),
              (2, ST_Point(20.0, 10.0))
        """
            )
        )
        await test_session.commit()

        polygon = '{"type": "Polygon", "coordinates": [[[8,44],[10,44],[10,46],[8,46],[8,44]]]}'
        sql_filter = (
            f"ST_Intersects(geom, ST_SetSRID(ST_GeomFromGeoJSON('{polygon}'), 4326))"
        )

        resp = await client.get("/dataset/ds_geo/query", params={"filter": sql_filter})

        assert resp.status_code == 200
        items = resp.json()["items"]
        assert len(items) == 1
        assert items[0]["id"] == 1

    finally:
        await test_session.execute(text(f"DROP TABLE IF EXISTS {table}"))
        await test_session.commit()


@pytest.mark.asyncio
async def test_query_temporal_filter(client, test_session):
    table = "dataset_api.temp_table"

    try:
        ds = DatasetEntry(
            dataset_id="ds_temp",
            title="TempDS",
            backend_type="postgres",
            backend_config={"table": table},
            expose=True,
        )
        test_session.add(ds)
        await test_session.commit()

        await test_session.execute(
            text(
                f"""
            CREATE TABLE {table} (
                id INTEGER,
                ts TIMESTAMP
            )
        """
            )
        )
        await test_session.execute(
            text(
                f"""
            INSERT INTO {table} (id, ts) VALUES
              (1, '2025-01-01'),
              (2, '2025-02-01'),
              (3, '2024-12-01')
        """
            )
        )
        await test_session.commit()

        resp = await client.get(
            "/dataset/ds_temp/query",
            params={"filter": "ts >= '2025-01-01T00:00:00Z'"},
        )
        assert resp.status_code == 200

        ids = {i["id"] for i in resp.json()["items"]}
        assert ids == {1, 2}

    finally:
        await test_session.execute(text(f"DROP TABLE IF EXISTS {table}"))
        await test_session.commit()


@pytest.mark.asyncio
async def test_query_complex_filter(client, test_session):
    table = "dataset_api.complex_table"

    try:
        ds = DatasetEntry(
            dataset_id="ds_complex",
            title="ComplexDS",
            backend_type="postgres",
            backend_config={"table": table},
            expose=True,
        )
        test_session.add(ds)
        await test_session.commit()

        await test_session.execute(
            text(
                f"""
            CREATE TABLE {table} (
                id INTEGER,
                a INTEGER,
                b TEXT
            )
        """
            )
        )
        await test_session.execute(
            text(
                f"""
            INSERT INTO {table} (id, a, b) VALUES
              (1, 10, 'z'),
              (2, 20, 'y'),
              (3, 30, 'x')
        """
            )
        )
        await test_session.commit()

        filter_sql = "(a >= 20 AND b = 'y') OR (a >= 30 AND b = 'x')"

        resp = await client.get(
            "/dataset/ds_complex/query",
            params={"filter": filter_sql},
        )
        assert resp.status_code == 200

        ids = {i["id"] for i in resp.json()["items"]}
        assert ids == {2, 3}

    finally:
        await test_session.execute(text(f"DROP TABLE IF EXISTS {table}"))
        await test_session.commit()
