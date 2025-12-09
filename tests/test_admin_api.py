# tests/test_admin_api.py
import pytest


@pytest.mark.asyncio
async def test_admin_catalogue_import(client):
    payload = {
        "datasets": [
            {
                "dataset_id": "ds1",
                "title": "DS1",
                "backend_type": "postgres",
                "backend_config": {"table": "t"},
            }
        ]
    }

    resp = await client.post("/admin/catalogue", json=payload)
    assert resp.status_code == 200
    data = resp.json()
    assert data["created"] == 1