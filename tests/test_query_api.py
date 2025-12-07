import pytest


@pytest.mark.asyncio
async def test_query_missing_dataset(client):
    resp = await client.get("/dataset/unknown/query")
    assert resp.status_code in (400, 404)
