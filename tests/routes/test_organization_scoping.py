"""An organization's group reaches only its own organization's rows (GS-06, GS-07).

Two communities, `example-rec-a` and `example-rec-b`, a grid operator
`example-dso`, and every kind of caller, against four kinds of `internal`
dataset:

- per community: `organization_match` on the column holding the community's
  organization alias;
- per organization type: `organization_match` with `org_type: dso` only;
- member-wide: `member_wide`, declared on purpose;
- unscoped: no row filter at all.

These run the real query path and the shipped Rego; the only seam is the
identity, built by the service's own `_normalize_user` from claims shaped as
Keycloak issues them.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest
from sqlalchemy import text

from celine.dataset.api.dataset_query.row_filters import get_row_filter_registry
from celine.dataset.api.dataset_query.row_filters.handlers import (
    MemberWideHandler,
    OrganizationMatchHandler,
)
from celine.dataset.db.models.dataset_entry import DatasetEntry
from celine.dataset.security.auth import _normalize_user, get_optional_user
from celine.dataset.security.groups import reading_organizations

COMMUNITY_TABLE = "dataset_api.gs06_community"
COMMUNITY = "datasets.dataset_api.gs06_community"
DSO_TABLE = "dataset_api.gs06_grid"
DSO = "datasets.dataset_api.gs06_grid"
WIDE_TABLE = "dataset_api.gs06_weather"
WIDE = "datasets.dataset_api.gs06_weather"
PLAIN_TABLE = "dataset_api.gs06_plain"
PLAIN = "datasets.dataset_api.gs06_plain"

DEFAULT_ROLES = ["default-roles-celine", "offline_access", "uma_authorization"]


def _claims(sub: str, *, roles=None, orgs: dict | None = None, **extra) -> dict:
    """`orgs` maps an alias to `(type, [groups])`."""
    claims: dict = {
        "sub": sub,
        "azp": "oauth2_proxy",
        "preferred_username": sub,
        "email": f"{sub}@rec.example.org",
        "realm_access": {"roles": roles if roles is not None else DEFAULT_ROLES},
    }
    if orgs:
        claims["organization"] = {
            alias: {"type": [kind], "groups": groups}
            for alias, (kind, groups) in orgs.items()
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


VIEWER_A = _claims("viewer-a", orgs={"example-rec-a": ("rec", ["/viewers"])})
MANAGER_A = _claims("manager-a", orgs={"example-rec-a": ("rec", ["/managers"])})
ADMINS_ONLY_A = _claims("admins-a", orgs={"example-rec-a": ("rec", ["/admins"])})
EDITOR_A = _claims("editor-a", orgs={"example-rec-a": ("rec", ["/editors"])})
VIEWER_B = _claims("viewer-b", orgs={"example-rec-b": ("rec", ["/viewers"])})
#: An operator of both communities.
OPERATOR_AB = _claims(
    "operator-ab",
    orgs={
        "example-rec-a": ("rec", ["/admins"]),
        "example-rec-b": ("rec", ["/managers"]),
    },
)
#: Released from A, but the token still says A until it is reissued.
STALE_MEMBER = _claims(
    "moved",
    orgs={
        "example-rec-a": ("rec", ["/viewers"]),
        "example-rec-b": ("rec", ["/viewers"]),
    },
)
DSO_VIEWER = _claims("dso-viewer", orgs={"example-dso": ("dso", ["/viewers"])})
PLATFORM_ADMIN = _claims("platform-admin", roles=["platform-admin"])
SERVICE = {
    "sub": "svc-digital-twin",
    "azp": "svc-digital-twin",
    "preferred_username": "service-account-svc-digital-twin",
    "scope": "dataset.query",
}


async def _seed(test_session) -> None:
    await test_session.execute(text(f"CREATE TABLE {COMMUNITY_TABLE} (id INTEGER, rec_id TEXT)"))
    await test_session.execute(
        text(
            f"INSERT INTO {COMMUNITY_TABLE} VALUES "
            "(1, 'example-rec-a'), (2, 'example-rec-a'), (3, 'example-rec-b'), (4, NULL)"
        )
    )
    for table in (DSO_TABLE, WIDE_TABLE, PLAIN_TABLE):
        await test_session.execute(text(f"CREATE TABLE {table} (id INTEGER)"))
        await test_session.execute(text(f"INSERT INTO {table} VALUES (1), (2)"))

    def entry(dataset_id: str, table: str, row_filters: list | None) -> DatasetEntry:
        governance = {"rowFilters": row_filters} if row_filters is not None else {}
        return DatasetEntry(
            dataset_id=dataset_id,
            title=dataset_id,
            backend_type="postgres",
            backend_config={"table": table},
            expose=True,
            access_level="internal",
            lineage={"facets": {"governance": governance}},
        )

    test_session.add(
        entry(
            COMMUNITY,
            COMMUNITY_TABLE,
            [{"handler": "organization_match", "binds": "organization", "args": {"column": "rec_id"}}],
        )
    )
    test_session.add(
        entry(DSO, DSO_TABLE, [{"handler": "organization_match", "binds": "organization", "args": {"org_type": "dso"}}])
    )
    test_session.add(entry(WIDE, WIDE_TABLE, [{"handler": "member_wide", "binds": "organization"}]))
    test_session.add(entry(PLAIN, PLAIN_TABLE, None))
    await test_session.commit()


def _as(client, claims: dict) -> None:
    user = _user(claims)
    client._transport.app.dependency_overrides[get_optional_user] = lambda: user


async def _ids(client, dataset: str) -> tuple[int, list]:
    resp = await client.post("/query", json={"sql": f"SELECT id FROM {dataset}"})
    if resp.status_code != 200:
        return resp.status_code, []
    return 200, sorted(row["id"] for row in resp.json()["items"])


@pytest.fixture(autouse=True)
def _fresh_plans():
    # Plans are cached per process; a test must not read another's.
    get_row_filter_registry().cache._store.clear()
    yield
    get_row_filter_registry().cache._store.clear()


# ---------------------------------------------------------------------------
# GS-06 — an organization's reader gets its own organization's rows only
# ---------------------------------------------------------------------------


# @verifies GS-06
@pytest.mark.parametrize(
    "claims, expected",
    [
        (VIEWER_A, [1, 2]),
        (MANAGER_A, [1, 2]),
        (ADMINS_ONLY_A, [1, 2]),
        (VIEWER_B, [3]),
        (OPERATOR_AB, [1, 2, 3]),
        (STALE_MEMBER, [1, 2, 3]),
        (PLATFORM_ADMIN, [1, 2, 3, 4]),
        (SERVICE, [1, 2, 3, 4]),
    ],
    ids=[
        "A viewers",
        "A managers",
        "A admins only",
        "B viewers",
        "operator of A and B",
        "stale token in A and B",
        "platform-admin",
        "service by scope",
    ],
)
async def test_a_community_dataset_serves_the_callers_communities_only(
    client, test_session, claims, expected
):
    await _seed(test_session)
    _as(client, claims)
    assert await _ids(client, COMMUNITY) == (200, expected)


# @verifies GS-06
@pytest.mark.parametrize(
    "claims", [EDITOR_A, DSO_VIEWER], ids=["A editors", "a DSO's viewers"]
)
async def test_no_reading_group_in_a_community_reads_none_of_it(client, test_session, claims):
    await _seed(test_session)
    _as(client, claims)
    status, ids = await _ids(client, COMMUNITY)
    if claims is EDITOR_A:
        # `editors` is not a reading group: the policy refuses.
        assert status == 403
    else:
        # A reader elsewhere is admitted, and its organization holds no row here.
        assert (status, ids) == (200, [])


# @verifies GS-06
async def test_the_same_person_with_a_new_token_gets_the_new_organizations(
    client, test_session
):
    """A plan is cached per organization set, not per `sub`."""
    await _seed(test_session)
    _as(client, STALE_MEMBER)
    assert await _ids(client, COMMUNITY) == (200, [1, 2, 3])
    reissued = _claims("moved", orgs={"example-rec-b": ("rec", ["/viewers"])})
    _as(client, reissued)
    assert await _ids(client, COMMUNITY) == (200, [3])


# @verifies GS-06
@pytest.mark.parametrize(
    "claims, expected",
    [
        (DSO_VIEWER, (200, [1, 2])),
        (VIEWER_A, (200, [])),
        (PLATFORM_ADMIN, (200, [1, 2])),
        (SERVICE, (200, [1, 2])),
    ],
    ids=["DSO viewers", "REC viewers", "platform-admin", "service"],
)
async def test_an_org_type_dataset_serves_that_type_of_organization_only(
    client, test_session, claims, expected
):
    await _seed(test_session)
    _as(client, claims)
    assert await _ids(client, DSO) == expected


# ---------------------------------------------------------------------------
# GS-07 — an internal dataset with no row filter is closed to organizations
# ---------------------------------------------------------------------------


# @verifies GS-07
@pytest.mark.parametrize(
    "claims", [VIEWER_A, ADMINS_ONLY_A, OPERATOR_AB, DSO_VIEWER],
    ids=["A viewers", "A admins only", "operator of A and B", "DSO viewers"],
)
async def test_an_unscoped_internal_dataset_refuses_every_organization(
    client, test_session, claims
):
    await _seed(test_session)
    _as(client, claims)
    resp = await client.post("/query", json={"sql": f"SELECT id FROM {PLAIN}"})
    assert resp.status_code == 403
    assert "without a row filter" in resp.json()["detail"]


# @verifies GS-07
@pytest.mark.parametrize("claims", [PLATFORM_ADMIN, SERVICE], ids=["platform-admin", "service"])
async def test_an_unscoped_internal_dataset_stays_readable_by_platform_and_services(
    client, test_session, claims
):
    await _seed(test_session)
    _as(client, claims)
    assert await _ids(client, PLAIN) == (200, [1, 2])


# @verifies GS-07
@pytest.mark.parametrize(
    "claims", [VIEWER_A, VIEWER_B, DSO_VIEWER], ids=["A viewers", "B viewers", "DSO viewers"]
)
async def test_a_member_wide_dataset_is_read_whole_by_any_organization(
    client, test_session, claims
):
    await _seed(test_session)
    _as(client, claims)
    assert await _ids(client, WIDE) == (200, [1, 2])


# ---------------------------------------------------------------------------
# The handlers
# ---------------------------------------------------------------------------


def test_reading_organizations_counts_reading_groups_per_organization():
    assert reading_organizations(OPERATOR_AB) == ["example-rec-a", "example-rec-b"]
    assert reading_organizations(EDITOR_A) == []
    assert reading_organizations(DSO_VIEWER, org_type="dso") == ["example-dso"]
    assert reading_organizations(VIEWER_A, org_type="dso") == []
    # A type nested under `attributes`, as a differently configured mapper writes it.
    nested = _claims("n")
    nested["organization"] = {
        "example-dso": {"attributes": {"type": ["dso"]}, "groups": ["/viewers"]}
    }
    assert reading_organizations(nested, org_type="dso") == ["example-dso"]
    # A realm group is never read.
    assert reading_organizations(_claims("r", groups=["/viewers"])) == []


@pytest.mark.parametrize(
    "handler, args",
    [(OrganizationMatchHandler(), {"column": "rec_id"}), (MemberWideHandler(), {})],
    ids=["organization_match", "member_wide"],
)
@pytest.mark.parametrize(
    "delegation",
    [{"principals": []}, {"principals": ["alice"]}, {"keys": ["pod:EX1"]}],
    ids=["nobody", "principals", "keys"],
)
async def test_a_delegated_request_is_refused(handler, args, delegation):
    # The executor turns NotImplementedError into "serve no rows".
    with pytest.raises(NotImplementedError):
        await handler.resolve(table="t", user=_user(VIEWER_A), args=args, **delegation)


@pytest.mark.parametrize(
    "args", [{}, {"column": ""}, {"org_type": ""}, {"column": 3}],
    ids=["neither", "empty column", "empty type", "column not a string"],
)
async def test_organization_match_refuses_args_that_name_no_scope(args):
    with pytest.raises(ValueError):
        await OrganizationMatchHandler().resolve(
            table="t", user=_user(VIEWER_A), args=args
        )
