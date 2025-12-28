# tests/test_query_api.py
import pytest


@pytest.mark.asyncio
async def test_query_missing_dataset(client):
    resp = await client.post("/query", json={"sql": "select * from unknown"})
    assert resp.status_code in (400, 404)


@pytest.mark.asyncio
async def test_query_missing_sql(client):
    resp = await client.post("/query", json={})
    assert resp.status_code in (400, 404)


@pytest.mark.asyncio
async def test_query_empty_sql(client):
    resp = await client.post("/query", json={"sql": " "})
    assert resp.status_code in (400, 404)
