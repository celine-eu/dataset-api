"""An organization's `admins` is not the platform's `admins` (NIS2 R1).

A token carries groups at realm level and inside every organization. The SDK's
`extract_groups` merged the two, and this service read the merged list, so a
community operator holding `admins` inside its own organization was a platform
administrator here: every row filter skipped, `restricted` datasets readable,
the catalogue writable — every community's meter data, from one community's
operator account.

These run the real query path and the shipped Rego. The only seam is the
identity: the caller is built by the service's own `_normalize_user` from claims
shaped as Keycloak issues them, rather than from a signed token.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from sqlalchemy import text

from celine.dataset.db.models.dataset_entry import DatasetEntry
from celine.dataset.security.auth import (
    _normalize_user,
    get_optional_user,
    require_catalogue_admin,
)
from celine.dataset.security.groups import authorization_groups, is_realm_admin

TABLE = "dataset_api.r1_member_readings"
DATASET = "datasets.dataset_api.r1_member_readings"
RESTRICTED_TABLE = "dataset_api.r1_restricted"
RESTRICTED = "datasets.dataset_api.r1_restricted"


def _claims(sub: str, *, realm: list[str] | None = None, orgs: dict | None = None) -> dict:
    claims: dict = {"sub": sub, "preferred_username": sub, "email": f"{sub}@rec.example.org"}
    if realm is not None:
        claims["groups"] = realm
    if orgs is not None:
        claims["organization"] = {
            alias: {"type": "rec", "groups": groups} for alias, groups in orgs.items()
        }
    return claims


def _user(claims: dict):
    jwt_user = SimpleNamespace(
        claims=claims,
        sub=claims["sub"],
        preferred_username=claims.get("preferred_username"),
        email=claims.get("email"),
        iss="https://auth.example.org/realms/celine",
    )
    return _normalize_user(jwt_user, token=None)


#: The community operator: `admins` and `viewers` inside its own organization only.
OPERATOR_A = _claims("operator-a", orgs={"example-rec": ["/admins", "/viewers"]})
#: An operator with nothing but its organization's `admins`.
ADMIN_ONLY_A = _claims("admin-only-a", orgs={"example-rec": ["/admins"]})
#: The platform administrator: realm-level `admins`.
PLATFORM_ADMIN = _claims("platform-admin", realm=["/admins"])


async def _seed(test_session) -> None:
    await test_session.execute(
        text(f"CREATE TABLE {TABLE} (id INTEGER, user_id TEXT, rec TEXT)")
    )
    await test_session.execute(
        text(
            f"INSERT INTO {TABLE} VALUES "
            "(1, 'operator-a', 'example-rec'), (2, 'member-a', 'example-rec'), "
            "(3, 'member-b', 'other-rec')"
        )
    )
    await test_session.execute(text(f"CREATE TABLE {RESTRICTED_TABLE} (id INTEGER)"))
    await test_session.execute(text(f"INSERT INTO {RESTRICTED_TABLE} VALUES (1)"))
    test_session.add(
        DatasetEntry(
            dataset_id=DATASET,
            title="Member readings",
            backend_type="postgres",
            backend_config={"table": TABLE},
            expose=True,
            access_level="internal",
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
    )
    test_session.add(
        DatasetEntry(
            dataset_id=RESTRICTED,
            title="Restricted",
            backend_type="postgres",
            backend_config={"table": RESTRICTED_TABLE},
            expose=True,
            access_level="restricted",
        )
    )
    await test_session.commit()


def _as(client, claims: dict) -> None:
    user = _user(claims)
    client._transport.app.dependency_overrides[get_optional_user] = lambda: user


async def _ids(client, sql: str) -> tuple[int, list]:
    resp = await client.post("/query", json={"sql": sql})
    if resp.status_code != 200:
        return resp.status_code, []
    return 200, sorted(row["id"] for row in resp.json()["items"])


# ---------------------------------------------------------------------------
# The query path
# ---------------------------------------------------------------------------


async def test_an_organization_admin_is_row_filtered_like_any_member(client, test_session):
    await _seed(test_session)
    _as(client, OPERATOR_A)
    status, ids = await _ids(client, f"SELECT id FROM {DATASET}")
    # Its own row only — not its community's other member, not the other community.
    assert (status, ids) == (200, [1])


async def test_a_realm_admin_still_reads_every_row(client, test_session):
    await _seed(test_session)
    _as(client, PLATFORM_ADMIN)
    assert await _ids(client, f"SELECT id FROM {DATASET}") == (200, [1, 2, 3])


async def test_an_organization_admin_cannot_read_a_restricted_dataset(client, test_session):
    await _seed(test_session)
    _as(client, OPERATOR_A)
    status, _ = await _ids(client, f"SELECT id FROM {RESTRICTED}")
    assert status == 403


async def test_a_realm_admin_reads_a_restricted_dataset(client, test_session):
    await _seed(test_session)
    _as(client, PLATFORM_ADMIN)
    assert await _ids(client, f"SELECT id FROM {RESTRICTED}") == (200, [1])


async def test_an_organization_admin_alone_grants_no_internal_read(client, test_session):
    """`admins` inside an organization is not even a viewer here."""
    await _seed(test_session)
    _as(client, ADMIN_ONLY_A)
    status, _ = await _ids(client, f"SELECT id FROM {DATASET}")
    assert status == 403


# ---------------------------------------------------------------------------
# The catalogue
# ---------------------------------------------------------------------------


async def test_an_organization_admin_cannot_write_the_catalogue():
    with pytest.raises(HTTPException) as err:
        await require_catalogue_admin(_user(OPERATOR_A))
    assert err.value.status_code == 403


async def test_a_realm_admin_can_write_the_catalogue():
    user = _user(PLATFORM_ADMIN)
    assert await require_catalogue_admin(user) is user


# ---------------------------------------------------------------------------
# The groups read
# ---------------------------------------------------------------------------


def test_organization_admins_is_dropped_and_its_other_groups_kept():
    claims = _claims(
        "x", realm=["/viewers"], orgs={"example-rec": ["/admins", "/managers"]}
    )
    assert authorization_groups(claims) == ["viewers", "managers"]


def test_realm_admins_is_kept():
    assert authorization_groups(PLATFORM_ADMIN) == ["admins"]
    assert is_realm_admin(PLATFORM_ADMIN)


@pytest.mark.parametrize(
    "orgs",
    [{"example-rec": ["/admins"]}, {"example-rec": ["admins"], "other-rec": ["/admins"]}],
)
def test_no_organization_makes_a_realm_admin(orgs):
    claims = _claims("x", orgs=orgs)
    assert not is_realm_admin(claims)
    assert "admins" not in authorization_groups(claims)
