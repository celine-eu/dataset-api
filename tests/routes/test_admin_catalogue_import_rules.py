"""What `POST /admin/catalogue` stores, and what its cleanup keeps.

- An entry stating no `access_level` is stored as `internal`.
- An `access_level` nobody can read is refused at import, not at the first query.
- The table an entry is checked against is the one it would be queried through:
  `backend_config.table`, or for postgres the table its id names. The import skips an
  entry whose table does not exist, and the cleanup keeps one whose table does.
"""
from __future__ import annotations

from sqlalchemy import text

from celine.dataset.security.auth import get_current_user
from celine.dataset.security.models import AuthenticatedUser


def _admin(client) -> None:
    client._transport.app.dependency_overrides[get_current_user] = lambda: (
        AuthenticatedUser(sub="svc", scopes=["dataset.admin"])
    )


async def _import(client, *datasets: dict):
    _admin(client)
    return await client.post("/admin/catalogue", json={"datasets": list(datasets)})


async def _rows(test_session) -> dict[str, str | None]:
    res = await test_session.execute(
        text("SELECT dataset_id, access_level FROM dataset_api.datasets_entries")
    )
    return dict(res.all())


async def test_an_entry_stating_no_level_is_stored_internal(client, test_session):
    await test_session.execute(text("CREATE TABLE dataset_api.lvl (id INTEGER)"))
    await test_session.commit()
    resp = await _import(
        client,
        {
            "dataset_id": "lvl",
            "title": "L",
            "backend_type": "postgres",
            "backend_config": {"table": "dataset_api.lvl"},
        },
    )
    assert resp.status_code == 200, resp.text
    assert (await _rows(test_session)) == {"lvl": "internal"}


async def test_an_unreadable_level_is_refused_at_import(client, test_session):
    resp = await _import(
        client,
        {
            "dataset_id": "bad",
            "title": "B",
            "backend_type": "postgres",
            "backend_config": {"table": "dataset_api.bad"},
            "access_level": "external",
        },
    )
    assert resp.status_code == 422
    assert (await _rows(test_session)) == {}


async def test_an_entry_without_a_table_is_checked_against_the_table_its_id_names(
    client, test_session
):
    await test_session.execute(text("CREATE TABLE dataset_api.derived (id INTEGER)"))
    await test_session.commit()
    resp = await _import(
        client,
        # table exists under the derived name: imported and kept by the cleanup
        {"dataset_id": "datasets.dataset_api.derived", "title": "D", "backend_type": "postgres"},
        # no such table: skipped
        {"dataset_id": "datasets.dataset_api.missing", "title": "M", "backend_type": "postgres"},
        # not queryable: catalogue-only, not checked, kept
        {
            "dataset_id": "datasets.files.report",
            "title": "R",
            "backend_type": "s3",
            "backend_config": {"path": "s3://bucket/key"},
        },
    )
    assert resp.status_code == 200, resp.text
    assert set(await _rows(test_session)) == {
        "datasets.dataset_api.derived",
        "datasets.files.report",
    }
