"""The access audit: who read which dataset, and who was refused (GS-01 – GS-03).

Every record is one JSON line on the `celine.audit` logger. These run the real
query path; the only seam is the identity, built by the service's own
`_normalize_user` from claims shaped as Keycloak issues them.
"""
from __future__ import annotations

import json
import logging
from types import SimpleNamespace

import pytest
from sqlalchemy import text

from celine.dataset.db.models.dataset_entry import DatasetEntry
from celine.dataset.routes import query as query_route
from celine.dataset.security import auth as auth_mod
from celine.dataset.security.auth import (
    _normalize_user,
    get_current_user,
    get_optional_user,
)

AUDIT = "celine.audit"

PLAIN_TABLE = "dataset_api.audit_plain"
PLAIN = "datasets.dataset_api.audit_plain"
OTHER_TABLE = "dataset_api.audit_other"
OTHER = "datasets.dataset_api.audit_other"
WITHHELD_TABLE = "dataset_api.audit_withheld"
WITHHELD = "datasets.dataset_api.audit_withheld"

#: What a person's token says about them beyond `sub`: none of it may reach a record.
EMAIL = "example.person@rec.example.org"
USERNAME = "example.person"
NAME = "Example Person"
PERSONAL = (EMAIL, USERNAME, NAME, "rec.example.org")


def _claims(sub: str, *, groups: list[str], **extra) -> dict:
    claims = {
        "sub": sub,
        "azp": "oauth2_proxy",
        "preferred_username": USERNAME,
        "email": EMAIL,
        "name": NAME,
        "realm_access": {"roles": ["default-roles-celine"]},
        "organization": {"example_rec": {"type": ["rec"], "groups": groups}},
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


#: Reads `internal` datasets.
VIEWER = _claims("4b1e0c7a-0000-4000-8000-000000000001", groups=["/viewers"])
#: An organization's `admins` grants no read here.
ORG_ADMIN = _claims("4b1e0c7a-0000-4000-8000-000000000002", groups=["/admins"])


def _as(client, claims: dict) -> None:
    user = _user(claims)
    client._transport.app.dependency_overrides[get_optional_user] = lambda: user
    client._transport.app.dependency_overrides[get_current_user] = lambda: user


def _records(caplog) -> list[dict]:
    return [json.loads(r.getMessage()) for r in caplog.records if r.name == AUDIT]


def _only(caplog) -> dict:
    records = _records(caplog)
    assert len(records) == 1, records
    return records[0]


def _no_personal_data(caplog) -> None:
    for r in caplog.records:
        if r.name == AUDIT:
            line = r.getMessage()
            for value in PERSONAL:
                assert value not in line, (value, line)


@pytest.fixture(autouse=True)
def _capture(caplog):
    caplog.set_level(logging.INFO, logger=AUDIT)


@pytest.fixture
async def seeded(test_session):
    for table in (PLAIN_TABLE, OTHER_TABLE, WITHHELD_TABLE):
        await test_session.execute(text(f"CREATE TABLE {table} (id INTEGER)"))
        await test_session.execute(text(f"INSERT INTO {table} VALUES (1)"))
    for dataset_id, table, offered in (
        (PLAIN, PLAIN_TABLE, True),
        (OTHER, OTHER_TABLE, True),
        (WITHHELD, WITHHELD_TABLE, False),
    ):
        test_session.add(
            DatasetEntry(
                dataset_id=dataset_id,
                title=dataset_id,
                backend_type="postgres",
                backend_config={"table": table},
                expose=True,
                dataspace_expose=offered,
                access_level="internal",
            )
        )
    await test_session.commit()


async def _query(client, sql: str, **headers):
    return await client.post("/query", json={"sql": sql}, headers=headers)


# ---------------------------------------------------------------------------
# GS-01 — a read names the caller and the datasets
# ---------------------------------------------------------------------------


# @verifies GS-01
async def test_a_query_records_who_read_which_dataset(client, seeded, caplog):
    _as(client, VIEWER)
    resp = await _query(
        client,
        f"SELECT id FROM {PLAIN}",
        **{"X-Request-ID": "req-1", "traceparent": "00-" + "a" * 32 + "-" + "b" * 16 + "-01"},
    )
    assert resp.status_code == 200, resp.text

    record = _only(caplog)
    assert record["event"] == "access"
    assert record["outcome"] == "allowed"
    assert record["service"] == "dataset-api"
    assert record["action"] == "dataset.query"
    assert record["sub"] == VIEWER["sub"]
    assert record["client_id"] == "oauth2_proxy"
    assert record["service_account"] is False
    assert record["method"] == "POST"
    assert record["route"] == "/query"
    assert record["resource"] == PLAIN
    assert record["request_id"] == "req-1"
    assert record["trace_id"] == "a" * 32
    _no_personal_data(caplog)


# @verifies GS-01
async def test_a_join_is_one_record_naming_every_dataset(client, seeded, caplog):
    _as(client, VIEWER)
    resp = await _query(client, f"SELECT p.id FROM {PLAIN} p JOIN {OTHER} o ON p.id = o.id")
    assert resp.status_code == 200, resp.text
    assert _only(caplog)["resource"] == ",".join(sorted([PLAIN, OTHER]))


# @verifies GS-01
async def test_an_anonymous_read_is_recorded_without_a_caller(client, test_session, caplog):
    await test_session.execute(text("CREATE TABLE dataset_api.audit_open (id INTEGER)"))
    test_session.add(
        DatasetEntry(
            dataset_id="datasets.dataset_api.audit_open",
            title="open",
            backend_type="postgres",
            backend_config={"table": "dataset_api.audit_open"},
            expose=True,
            access_level="open",
        )
    )
    await test_session.commit()

    resp = await _query(client, "SELECT id FROM datasets.dataset_api.audit_open")
    assert resp.status_code == 200, resp.text
    record = _only(caplog)
    assert (record["event"], record["sub"], record["resource"]) == (
        "access",
        None,
        "datasets.dataset_api.audit_open",
    )


# @verifies GS-01
async def test_the_audit_stays_on_when_the_application_logs_only_warnings(
    client, seeded, caplog
):
    _as(client, VIEWER)
    app_logger = logging.getLogger("celine")
    before = app_logger.level
    app_logger.setLevel(logging.WARNING)
    try:
        resp = await _query(client, f"SELECT id FROM {PLAIN}")
    finally:
        app_logger.setLevel(before)
    assert resp.status_code == 200
    assert _only(caplog)["event"] == "access"


# @verifies GS-01
async def test_a_statement_that_does_not_parse_is_an_error_not_a_refusal(
    client, seeded, caplog
):
    _as(client, VIEWER)
    resp = await _query(client, "SELECT FROM WHERE")
    assert resp.status_code == 400
    record = _only(caplog)
    assert (record["event"], record["outcome"], record["reason"]) == (
        "access",
        "error",
        "http 400",
    )


# ---------------------------------------------------------------------------
# GS-02 — a refusal names the caller and a reason code
# ---------------------------------------------------------------------------


# @verifies GS-02
async def test_a_policy_denial_names_the_caller(client, seeded, caplog):
    _as(client, ORG_ADMIN)
    resp = await _query(client, f"SELECT id FROM {PLAIN}")
    assert resp.status_code == 403

    record = _only(caplog)
    assert record["event"] == "denied"
    assert record["outcome"] == "denied"
    assert record["reason"] == "policy"
    assert record["sub"] == ORG_ADMIN["sub"]
    assert record["resource"] == PLAIN
    _no_personal_data(caplog)
    warning = next(r for r in caplog.records if r.name == AUDIT)
    assert warning.levelno == logging.WARNING


# @verifies GS-02
async def test_a_dataset_requiring_a_login_is_refused_and_recorded(client, seeded, caplog):
    resp = await _query(client, f"SELECT id FROM {PLAIN}")
    assert resp.status_code == 401
    record = _only(caplog)
    assert (record["event"], record["reason"], record["sub"]) == (
        "denied",
        "auth_required",
        None,
    )


# @verifies GS-02
async def test_a_reference_outside_the_catalogue_is_a_recorded_refusal(
    client, seeded, caplog
):
    _as(client, VIEWER)
    resp = await _query(client, f"SELECT * FROM pg_catalog.pg_authid, {PLAIN}")
    assert resp.status_code == 400

    record = _only(caplog)
    assert (record["event"], record["reason"], record["sub"]) == (
        "denied",
        "unknown_dataset",
        VIEWER["sub"],
    )
    # What the caller named is not a platform id, so it is not the resource.
    assert record["resource"] is None


# @verifies GS-02
@pytest.mark.parametrize(
    "sql",
    [
        # QE-05: a CTE's own body cannot see the CTE, so there the name is a table.
        f"WITH audit_log AS (SELECT * FROM audit_log) SELECT * FROM audit_log, {PLAIN}",
        # An earlier CTE cannot see a later one.
        f"WITH b AS (SELECT * FROM a), a AS (SELECT * FROM {PLAIN}) SELECT * FROM b",
    ],
    ids=["own body", "earlier CTE"],
)
async def test_a_cte_name_out_of_scope_is_a_recorded_refusal(client, seeded, caplog, sql):
    _as(client, VIEWER)
    resp = await _query(client, sql)
    assert resp.status_code == 400

    record = _only(caplog)
    assert (record["event"], record["reason"], record["sub"]) == (
        "denied",
        "unknown_dataset",
        VIEWER["sub"],
    )


# @verifies GS-02
async def test_scopes_that_cannot_be_resolved_are_a_recorded_refusal(
    client, seeded, caplog, monkeypatch
):
    from celine.dataset.api.dataset_query import parser

    def _unresolvable(_ast):
        raise RuntimeError("scope")

    monkeypatch.setattr(parser, "traverse_scope", _unresolvable)
    _as(client, VIEWER)
    resp = await _query(client, f"WITH m AS (SELECT * FROM {PLAIN}) SELECT * FROM m")
    assert resp.status_code == 400
    assert _only(caplog)["reason"] == "scope_unresolved"


# @verifies GS-02
@pytest.mark.parametrize(
    "sql",
    [
        f"SELECT pg_read_file('/etc/hostname') FROM {PLAIN}",
        f"SELECT id FROM {PLAIN}; SELECT 1",
        f"SELECT id FROM {PLAIN} WHERE id = 2 OR 1 = 1",
    ],
    ids=["function", "stacked", "tautology in OR"],
)
async def test_a_statement_the_guard_refuses_is_a_recorded_refusal(
    client, seeded, caplog, sql
):
    _as(client, VIEWER)
    resp = await _query(client, sql)
    assert resp.status_code == 400

    record = _only(caplog)
    assert (record["event"], record["reason"], record["sub"]) == (
        "denied",
        "sql_refused",
        VIEWER["sub"],
    )
    # The reason is a code: nothing of the statement reaches the record.
    assert "etc" not in json.dumps(record)


# @verifies GS-02
async def test_a_token_that_does_not_verify_is_recorded_without_a_caller(
    client, seeded, caplog
):
    resp = await _query(client, f"SELECT id FROM {PLAIN}", Authorization="Bearer not-a-jwt")
    assert resp.status_code == 401

    record = _only(caplog)
    assert record["event"] == "denied"
    assert record["action"] == "auth.token"
    assert record["reason"] == "invalid_token"
    assert record["sub"] is None
    assert record["route"] == "/query"


# @verifies GS-02
async def test_a_dataspace_refusal_names_the_consumer(client, seeded, caplog, monkeypatch):
    consumer = "did:web:consumer.example.org"

    async def _verified(_authorization):
        return SimpleNamespace(consumer_id=consumer, provider_id="did:web:provider.example.org")

    monkeypatch.setattr(query_route, "dataspace_mode", lambda header: bool(header))
    monkeypatch.setattr(auth_mod, "dataspace_mode", lambda header: bool(header))
    monkeypatch.setattr(query_route, "verify_edr_token", _verified)

    resp = await _query(
        client,
        f"SELECT id FROM {WITHHELD}",
        Authorization="Bearer edr",
        **{"Edc-Contract-Agreement-Id": "agreement-1"},
    )
    assert resp.status_code == 403

    record = _only(caplog)
    assert record["event"] == "denied"
    assert record["reason"] == "not_offered"
    assert record["sub"] == consumer
    assert record["client_id"] == consumer
    assert record["service_account"] is True
    assert record["resource"] == WITHHELD


# ---------------------------------------------------------------------------
# GS-03 — the catalogue import
# ---------------------------------------------------------------------------


def _import_payload() -> dict:
    return {
        "datasets": [
            {
                "dataset_id": "ds_audit_import",
                "title": "DS",
                "backend_type": "postgres",
                "backend_config": {"table": PLAIN_TABLE},
            }
        ]
    }


# @verifies GS-03
async def test_a_refused_import_names_the_caller(client, seeded, caplog):
    _as(client, ORG_ADMIN)
    resp = await client.post("/admin/catalogue", json=_import_payload())
    assert resp.status_code == 403

    record = _only(caplog)
    assert (record["event"], record["action"], record["reason"], record["sub"]) == (
        "denied",
        "catalogue.import",
        "not_catalogue_admin",
        ORG_ADMIN["sub"],
    )
    assert record["route"] == "/admin/catalogue"
    _no_personal_data(caplog)


# @verifies GS-03
async def test_an_import_is_recorded(client, seeded, caplog):
    service = {
        "sub": "5d0c1f00-0000-4000-8000-00000000000a",
        "azp": "svc-dataset-api",
        "preferred_username": "service-account-svc-dataset-api",
        "scope": "dataset.admin",
    }
    _as(client, service)
    resp = await client.post("/admin/catalogue", json=_import_payload())
    assert resp.status_code == 200, resp.text

    record = _only(caplog)
    assert (record["event"], record["action"], record["outcome"]) == (
        "access",
        "catalogue.import",
        "allowed",
    )
    assert (record["client_id"], record["service_account"]) == ("svc-dataset-api", True)
