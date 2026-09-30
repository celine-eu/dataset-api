"""`POST /admin/catalogue` admits only the `dataset.admin` scope or the `admins` group.

The import writes `expose` and `access_level`, the fields every other gate reads, so
an import open to anyone is a way around all of them.
"""
from __future__ import annotations

import pytest
from sqlalchemy import text

from celine.dataset.security.auth import get_current_user
from celine.dataset.security.models import AuthenticatedUser

TABLE = "dataset_api.t_admin_auth"


def _payload() -> dict:
    return {
        "datasets": [
            {
                "dataset_id": "ds_admin_auth",
                "title": "DS",
                "backend_type": "postgres",
                "backend_config": {"table": TABLE},
            }
        ]
    }


def _as(client, user: AuthenticatedUser) -> None:
    client._transport.app.dependency_overrides[get_current_user] = lambda: user


@pytest.fixture
async def table(test_session):
    await test_session.execute(text(f"CREATE TABLE {TABLE} (id INTEGER)"))
    await test_session.commit()


async def _count(test_session) -> int:
    res = await test_session.execute(
        text("SELECT count(*) FROM dataset_api.datasets_entries")
    )
    return res.scalar_one()


async def test_no_token_is_refused_and_writes_nothing(client, test_session, table):
    resp = await client.post("/admin/catalogue", json=_payload())
    assert resp.status_code == 401
    assert await _count(test_session) == 0


async def test_an_invalid_token_is_refused(client, test_session, table):
    resp = await client.post(
        "/admin/catalogue",
        json=_payload(),
        headers={"Authorization": "Bearer not-a-jwt"},
    )
    assert resp.status_code == 401
    assert await _count(test_session) == 0


async def test_a_user_without_scope_or_group_is_forbidden(client, test_session, table):
    _as(client, AuthenticatedUser(sub="u", groups=["viewers"], scopes=["dataset.query"]))
    resp = await client.post("/admin/catalogue", json=_payload())
    assert resp.status_code == 403
    assert await _count(test_session) == 0


@pytest.mark.parametrize(
    "user",
    [
        AuthenticatedUser(sub="service-account-svc-dataset-api", scopes=["dataset.admin"]),
        AuthenticatedUser(sub="alice", groups=["admins"]),
    ],
    ids=["dataset.admin scope", "admins group"],
)
async def test_an_admin_imports(client, test_session, table, user):
    _as(client, user)
    resp = await client.post("/admin/catalogue", json=_payload())
    assert resp.status_code == 200, resp.text
    assert resp.json()["created"] == 1
