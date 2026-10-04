"""A platform administrator is the realm role `platform-admin`, and nothing else is.

Two levels, never mixed:

- the realm role `platform-admin` (`realm_access.roles`) skips every row filter,
  reads `restricted` datasets and writes the catalogue;
- an organization's own groups count only as organization groups: `managers`
  and `viewers` read `internal` datasets, row-filtered; its `admins` grants
  nothing here.

A realm group (`groups: ["/admins"]`) or one of the retired realm roles
(`admin`, `manager`, `viewer`) still present in a token grants nothing.

These run the real query path and the shipped Rego. The only seam is the
identity: the caller is built by the service's own `_normalize_user` from claims
shaped as Keycloak issues them, rather than from a signed token. The same
callers with real tokens are in `tests/integration/test_platform_admin_real_tokens.py`.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from sqlalchemy import text

from celine.dataset.db.models.dataset_entry import DatasetEntry
from celine.dataset.security import governance as gov
from celine.dataset.security.auth import (
    _normalize_user,
    get_optional_user,
    require_catalogue_admin,
)
from celine.dataset.security.groups import (
    is_platform_admin,
    organization_groups_held,
    platform_roles,
)
from celine.sdk.policies import (
    Action,
    PolicyEngine,
    PolicyInput,
    Resource,
    ResourceType,
)

TABLE = "dataset_api.r1_member_readings"
DATASET = "datasets.dataset_api.r1_member_readings"
RESTRICTED_TABLE = "dataset_api.r1_restricted"
RESTRICTED = "datasets.dataset_api.r1_restricted"
PLAIN_TABLE = "dataset_api.r1_plain"
PLAIN = "datasets.dataset_api.r1_plain"

#: What every person's token carries in `realm_access.roles` without any grant.
DEFAULT_ROLES = ["default-roles-celine", "offline_access", "uma_authorization"]


def _claims(
    sub: str,
    *,
    roles: list[str] | None = None,
    orgs: dict | None = None,
    **extra,
) -> dict:
    claims: dict = {
        "sub": sub,
        "azp": "oauth2_proxy",
        "preferred_username": sub,
        "email": f"{sub}@rec.example.org",
    }
    if roles is not None:
        claims["realm_access"] = {"roles": roles}
    if orgs is not None:
        claims["organization"] = {
            alias: {"type": ["rec"], "groups": groups} for alias, groups in orgs.items()
        }
    claims.update(extra)
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


#: The platform administrator: the realm role, and no organization at all.
PLATFORM_ADMIN = _claims("platform-admin-1", roles=["platform-admin"])
#: A dev-team administrator as the local realm issues it: the role, plus
#: `admins` inside two organizations.
PLATFORM_ADMIN_IN_ORGS = _claims(
    "platform-admin-2",
    roles=["platform-admin"],
    orgs={"example_rec": ["/admins"], "example_dso": ["/admins"]},
)
#: The community operator: `admins` and `viewers` inside its own organization only.
OPERATOR_A = _claims(
    "operator-a", roles=DEFAULT_ROLES, orgs={"example_rec": ["/admins", "/viewers"]}
)
#: An operator with nothing but its organization's `admins`.
ORG_ADMIN = _claims("org-admin", roles=DEFAULT_ROLES, orgs={"example_rec": ["/admins"]})
#: A member: `viewers` inside its organization.
ORG_VIEWER = _claims("member-a", roles=DEFAULT_ROLES, orgs={"example_rec": ["/viewers"]})
#: The old platform administrator, as the retired mappers wrote it: realm group
#: `/admins` in both forms, the realm role `admin` mapped from it, and an
#: organization's `admins`.
LEGACY_REALM_ADMIN = _claims(
    "legacy-admin",
    roles=["admin"],
    orgs={"example_rec": ["/admins"]},
    groups=["/admins", "admins"],
)
#: The old realm `managers` and `viewers`, with no organization: they used to
#: read every `internal` dataset.
LEGACY_REALM_MANAGER = _claims("legacy-manager", roles=["manager"], groups=["/managers"])
LEGACY_REALM_VIEWER = _claims("legacy-viewer", roles=["viewer"], groups=["/viewers", "viewers"])


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
    await test_session.execute(text(f"CREATE TABLE {PLAIN_TABLE} (id INTEGER)"))
    await test_session.execute(text(f"INSERT INTO {PLAIN_TABLE} VALUES (7)"))
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
    test_session.add(
        DatasetEntry(
            dataset_id=PLAIN,
            title="Internal, no row filter",
            backend_type="postgres",
            backend_config={"table": PLAIN_TABLE},
            expose=True,
            access_level="internal",
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


@pytest.mark.parametrize(
    "claims", [PLATFORM_ADMIN, PLATFORM_ADMIN_IN_ORGS], ids=["role only", "role and org admins"]
)
async def test_a_platform_admin_reads_every_row(client, test_session, claims):
    await _seed(test_session)
    _as(client, claims)
    assert await _ids(client, f"SELECT id FROM {DATASET}") == (200, [1, 2, 3])


@pytest.mark.parametrize(
    "claims", [PLATFORM_ADMIN, PLATFORM_ADMIN_IN_ORGS], ids=["role only", "role and org admins"]
)
async def test_a_platform_admin_reads_a_restricted_dataset(client, test_session, claims):
    await _seed(test_session)
    _as(client, claims)
    assert await _ids(client, f"SELECT id FROM {RESTRICTED}") == (200, [1])


async def test_an_organization_admin_is_row_filtered_like_any_member(client, test_session):
    await _seed(test_session)
    _as(client, OPERATOR_A)
    status, ids = await _ids(client, f"SELECT id FROM {DATASET}")
    # Its own row only — not its community's other member, not the other community.
    assert (status, ids) == (200, [1])


async def test_an_organization_viewer_reads_its_own_rows(client, test_session):
    await _seed(test_session)
    _as(client, ORG_VIEWER)
    assert await _ids(client, f"SELECT id FROM {DATASET}") == (200, [2])


@pytest.mark.parametrize(
    "claims",
    [OPERATOR_A, ORG_ADMIN, ORG_VIEWER, LEGACY_REALM_ADMIN, LEGACY_REALM_MANAGER],
    ids=["org admins+viewers", "org admins", "org viewers", "legacy realm /admins", "legacy realm /managers"],
)
async def test_only_the_platform_admin_reads_a_restricted_dataset(client, test_session, claims):
    await _seed(test_session)
    _as(client, claims)
    status, _ = await _ids(client, f"SELECT id FROM {RESTRICTED}")
    assert status == 403


async def test_an_organization_admin_alone_grants_no_internal_read(client, test_session):
    """`admins` inside an organization is not even a viewer here."""
    await _seed(test_session)
    _as(client, ORG_ADMIN)
    status, _ = await _ids(client, f"SELECT id FROM {PLAIN}")
    assert status == 403


@pytest.mark.parametrize(
    "claims",
    [LEGACY_REALM_ADMIN, LEGACY_REALM_MANAGER, LEGACY_REALM_VIEWER],
    ids=["realm /admins", "realm /managers", "realm /viewers"],
)
async def test_a_realm_group_grants_no_internal_read(client, test_session, claims):
    await _seed(test_session)
    _as(client, claims)
    for dataset in (PLAIN, DATASET):
        status, _ = await _ids(client, f"SELECT id FROM {dataset}")
        assert status == 403, dataset


# ---------------------------------------------------------------------------
# The catalogue
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "claims",
    [OPERATOR_A, ORG_ADMIN, LEGACY_REALM_ADMIN],
    ids=["org admins+viewers", "org admins", "legacy realm /admins"],
)
async def test_only_the_platform_admin_writes_the_catalogue(claims):
    with pytest.raises(HTTPException) as err:
        await require_catalogue_admin(_user(claims))
    assert err.value.status_code == 403


@pytest.mark.parametrize("claims", [PLATFORM_ADMIN, PLATFORM_ADMIN_IN_ORGS])
async def test_a_platform_admin_can_write_the_catalogue(claims):
    user = _user(claims)
    assert await require_catalogue_admin(user) is user


# ---------------------------------------------------------------------------
# The two levels, read apart
# ---------------------------------------------------------------------------


def test_the_platform_level_is_the_realm_roles_only():
    user = _user(PLATFORM_ADMIN_IN_ORGS)
    assert user.roles == ["platform-admin"]
    assert user.groups == ["admins"]
    assert is_platform_admin(PLATFORM_ADMIN_IN_ORGS)


def test_a_realm_group_is_never_read():
    user = _user(LEGACY_REALM_ADMIN)
    # The organization's `admins`, once — not the realm's `/admins` or `admins`.
    assert user.groups == ["admins"]
    assert user.roles == ["admin"]
    assert not is_platform_admin(LEGACY_REALM_ADMIN)
    assert organization_groups_held(LEGACY_REALM_VIEWER) == []


def test_organization_groups_are_held_per_organization_and_deduplicated():
    claims = _claims(
        "x", orgs={"example_rec": ["/viewers", "/admins"], "example_dso": ["/viewers"]}
    )
    assert organization_groups_held(claims) == ["viewers", "admins"]
    assert platform_roles(claims) == []


@pytest.mark.parametrize(
    "claims",
    [
        # An organization group named like the role.
        _claims("x", orgs={"example_rec": ["/platform-admin"]}),
        # The role name in the wrong claim.
        _claims("x", groups=["/platform-admin", "platform-admin"]),
        {**_claims("x", roles=DEFAULT_ROLES), "roles": ["platform-admin"]},
        _claims(
            "x",
            roles=DEFAULT_ROLES,
            resource_access={"svc-dataset-api": {"roles": ["platform-admin"]}},
        ),
        # A retired realm role.
        _claims("x", roles=["admin", "admins"]),
    ],
    ids=["org group", "realm group", "top-level roles", "client role", "retired realm role"],
)
def test_nothing_but_the_realm_role_makes_a_platform_admin(claims):
    assert not is_platform_admin(claims)
    assert "platform-admin" not in _user(claims).roles


def test_the_policy_input_carries_roles_and_organization_groups_apart():
    subject = gov._build_subject_from_user(_user(LEGACY_REALM_ADMIN))
    engine = PolicyEngine(policies_dir="policies")
    data = engine.build_input_dict(
        PolicyInput(
            subject=subject,
            resource=Resource(type=ResourceType.DATASET, id="d", attributes={}),
            action=Action(name="read", context={}),
        )
    )
    assert data["subject"]["type"] == "user"
    assert data["subject"]["roles"] == ["admin"]
    assert data["subject"]["groups"] == ["admins"]


def test_the_policy_input_of_a_platform_admin_carries_the_role():
    subject = gov._build_subject_from_user(_user(PLATFORM_ADMIN))
    data = PolicyEngine(policies_dir="policies").build_input_dict(
        PolicyInput(
            subject=subject,
            resource=Resource(type=ResourceType.DATASET, id="d", attributes={}),
            action=Action(name="read", context={}),
        )
    )
    assert data["subject"]["roles"] == ["platform-admin"]
    assert data["subject"]["groups"] == []
