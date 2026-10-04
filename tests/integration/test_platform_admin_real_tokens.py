"""The two levels with real tokens from a local Keycloak.

`tests/routes/test_platform_admin_role.py` proves the rule on claims built by
hand. This proves it on tokens the realm actually issues, validated by the
service's own JWT path (signature, issuer, audience) with no identity override:

- an organization's `admins` member is not a platform administrator;
- the holder of the realm role `platform-admin` is;
- a token still carrying the legacy realm group `/admins` grants nothing.

Skipped unless `DATASET_API_KEYCLOAK_URL` names the realm, e.g.
`http://keycloak.celine.localhost/realms/celine`. Local stack only, never a
shared host. The realm must be converged to the two-level model (role
`platform-admin`, no realm groups) and hold the dev users `admin`, `org-admin`
and `org-viewer` (password = username). User tokens come from the
`oauth2_proxy` client (secret `DATASET_API_KEYCLOAK_CLIENT_SECRET`, default the
dev default). The legacy case needs a token in `DATASET_API_LEGACY_TOKEN`, minted
by a fixture that briefly puts the old realm group and mappers back; without
it, that case is skipped.
"""
from __future__ import annotations

import base64
import json
import os

import httpx
import pytest
from sqlalchemy import text

from celine.dataset.db.models.dataset_entry import DatasetEntry

KEYCLOAK = os.environ.get("DATASET_API_KEYCLOAK_URL", "").rstrip("/")
CLIENT_ID = "oauth2_proxy"
CLIENT_SECRET = os.environ.get("DATASET_API_KEYCLOAK_CLIENT_SECRET", "oauth2_proxy")
LEGACY_TOKEN = os.environ.get("DATASET_API_LEGACY_TOKEN")

pytestmark = pytest.mark.skipif(
    not KEYCLOAK, reason="DATASET_API_KEYCLOAK_URL not set (needs a local Keycloak)"
)

TABLE = "dataset_api.rt_member_readings"
DATASET = "datasets.dataset_api.rt_member_readings"
PLAIN_TABLE = "dataset_api.rt_plain"
PLAIN = "datasets.dataset_api.rt_plain"
RESTRICTED_TABLE = "dataset_api.rt_restricted"
RESTRICTED = "datasets.dataset_api.rt_restricted"


def _mint(username: str) -> str:
    resp = httpx.post(
        f"{KEYCLOAK}/protocol/openid-connect/token",
        data={
            "grant_type": "password",
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "username": username,
            "password": username,
            "scope": "openid email profile organization:*",
        },
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def _claims(token: str) -> dict:
    payload = token.split(".")[1]
    return json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))


@pytest.fixture(scope="module")
def tokens() -> dict[str, str]:
    return {name: _mint(name) for name in ("admin", "org-admin", "org-viewer")}


@pytest.fixture
async def seeded(test_session, tokens) -> dict[str, str]:
    """Three rows: the platform admin's, the viewer's, and a stranger's."""
    subs = {name: _claims(tok)["sub"] for name, tok in tokens.items()}
    await test_session.execute(text(f"CREATE TABLE {TABLE} (id INTEGER, user_id TEXT)"))
    await test_session.execute(
        text(f"INSERT INTO {TABLE} VALUES (1, :a), (2, :v), (3, 'someone-else')"),
        {"a": subs["admin"], "v": subs["org-viewer"]},
    )
    await test_session.execute(text(f"CREATE TABLE {PLAIN_TABLE} (id INTEGER)"))
    await test_session.execute(text(f"INSERT INTO {PLAIN_TABLE} VALUES (7)"))
    await test_session.execute(text(f"CREATE TABLE {RESTRICTED_TABLE} (id INTEGER)"))
    await test_session.execute(text(f"INSERT INTO {RESTRICTED_TABLE} VALUES (1)"))
    test_session.add_all(
        [
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
                                {
                                    "handler": "direct_user_match",
                                    "args": {"column": "user_id"},
                                }
                            ]
                        }
                    }
                },
            ),
            DatasetEntry(
                dataset_id=PLAIN,
                title="Internal, no row filter",
                backend_type="postgres",
                backend_config={"table": PLAIN_TABLE},
                expose=True,
                access_level="internal",
            ),
            DatasetEntry(
                dataset_id=RESTRICTED,
                title="Restricted",
                backend_type="postgres",
                backend_config={"table": RESTRICTED_TABLE},
                expose=True,
                access_level="restricted",
            ),
        ]
    )
    await test_session.commit()
    return subs


async def _query(client, token: str, dataset: str) -> tuple[int, list]:
    resp = await client.post(
        "/query",
        json={"sql": f"SELECT id FROM {dataset}"},
        headers={"Authorization": f"Bearer {token}"},
    )
    if resp.status_code != 200:
        return resp.status_code, []
    return 200, sorted(row["id"] for row in resp.json()["items"])


async def _import(client, token: str) -> int:
    resp = await client.post(
        "/admin/catalogue",
        json={
            "datasets": [
                {
                    "dataset_id": "rt_catalogue_write",
                    "title": "DS",
                    "backend_type": "postgres",
                    "backend_config": {"table": PLAIN_TABLE},
                }
            ]
        },
        headers={"Authorization": f"Bearer {token}"},
    )
    return resp.status_code


def test_the_realm_issues_the_two_levels_apart(tokens):
    """The premise: what the realm puts in each token."""
    admin = _claims(tokens["admin"])
    org_admin = _claims(tokens["org-admin"])
    assert "platform-admin" in admin["realm_access"]["roles"]
    assert "platform-admin" not in org_admin.get("realm_access", {}).get("roles", [])
    assert org_admin["organization"]["example_rec"]["groups"] == ["/admins"]
    for claims in (admin, org_admin):
        assert "groups" not in claims
        # A user token holds no dataset scope: a grant below can only come
        # from the role.
        assert not any(s.startswith("dataset.") for s in claims["scope"].split())


async def test_the_platform_admin_holder_is_a_platform_admin(client, seeded, tokens):
    tok = tokens["admin"]
    assert await _query(client, tok, DATASET) == (200, [1, 2, 3])
    assert await _query(client, tok, RESTRICTED) == (200, [1])
    assert await _import(client, tok) == 200


async def test_an_organization_admins_member_is_not_a_platform_admin(client, seeded, tokens):
    tok = tokens["org-admin"]
    assert (await _query(client, tok, RESTRICTED))[0] == 403
    # Its organization's `admins` is not even a viewer here.
    assert (await _query(client, tok, PLAIN))[0] == 403
    assert await _import(client, tok) == 403


async def test_an_organization_viewer_reads_its_own_rows_only(client, seeded, tokens):
    tok = tokens["org-viewer"]
    assert await _query(client, tok, DATASET) == (200, [2])
    assert await _query(client, tok, PLAIN) == (200, [7])
    assert (await _query(client, tok, RESTRICTED))[0] == 403
    assert await _import(client, tok) == 403


@pytest.mark.skipif(not LEGACY_TOKEN, reason="DATASET_API_LEGACY_TOKEN not set")
async def test_a_legacy_realm_admins_group_grants_nothing(client, seeded):
    claims = _claims(LEGACY_TOKEN)
    assert "/admins" in claims.get("groups", []) or "admins" in claims.get("groups", [])
    assert "platform-admin" not in claims.get("realm_access", {}).get("roles", [])
    assert (await _query(client, LEGACY_TOKEN, RESTRICTED))[0] == 403
    assert (await _query(client, LEGACY_TOKEN, PLAIN))[0] == 403
    assert (await _query(client, LEGACY_TOKEN, DATASET))[0] == 403
    assert await _import(client, LEGACY_TOKEN) == 403
