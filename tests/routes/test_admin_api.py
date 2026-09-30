# tests/test_admin_api.py
import pytest
from sqlalchemy import text

from celine.dataset.security.auth import get_current_user
from celine.dataset.security.models import AuthenticatedUser


@pytest.mark.asyncio
async def test_admin_catalogue_import(client, test_session):
    # The import route validates that the physical postgres table exists before
    # creating a catalogue entry, so the table has to be there for the import to
    # take effect. It lives in the catalogue schema, which the fixture drops.
    table = "dataset_api.t"
    await test_session.execute(text(f"CREATE TABLE {table} (id INTEGER)"))
    await test_session.commit()

    payload = {
        "datasets": [
            {
                "dataset_id": "ds1",
                "title": "DS1",
                "backend_type": "postgres",
                "backend_config": {"table": table},
            }
        ]
    }

    client._transport.app.dependency_overrides[get_current_user] = lambda: (
        AuthenticatedUser(sub="svc", scopes=["dataset.admin"])
    )
    resp = await client.post("/admin/catalogue", json=payload)
    assert resp.status_code == 200
    data = resp.json()
    assert data["created"] == 1
