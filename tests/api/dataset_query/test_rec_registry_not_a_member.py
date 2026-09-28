"""RF-12 — the registry's "no member" answer is no rows; nothing else is.

On the self-service path the `rec_registry` handler asks the registry for the
caller's own assets (`GET /user/assets`, the caller's token). For a person who
is no member of any community the registry answers `403` with the code
`not_a_member` (a registry older than the error codes: the detail
"You are not a member of any community" alone) — the only 403 that route gives,
since its middleware answers a missing or invalid token with 401. That answer is
a fact about the caller, not a failure: they own no asset, so no row is theirs,
and the plan is the same `deny` RF-11 gives a member without a meter.

Only that answer. A 403 carrying anything else (a gateway in front of the
registry, another registry code), a 401, a 404, a 5xx, a body that is not the
registry's, an unreachable registry: each stays an error (RF-05's distinction),
never read as "owns nothing".

The handler asks through the SDK's public `RecRegistryUserClient.get_my_assets`
and reads the answer off the SDK's own `RecRegistryApiError` (its `status_code`,
and the registry's `code` and `detail`). The wire-level cases drive that real
SDK client over an `httpx.MockTransport`, so they also pin the SDK's side of it:
every status but a readable 200 arrives as that error, never `UnexpectedStatus`.
"""
from __future__ import annotations

import json

import httpx
import pytest
from celine.sdk.openapi.rec_registry.errors import UnexpectedStatus
from celine.sdk.rec_registry import RecRegistryApiError, RecRegistryUserClient
from sqlalchemy import text

from celine.dataset.api.dataset_query.parser import parse_sql_query
from celine.dataset.api.dataset_query.row_filters.apply import apply_row_filter_plans
from celine.dataset.api.dataset_query.row_filters.handlers import RecRegistryHandler
from celine.dataset.api.dataset_query.row_filters.handlers import (
    rec_registry as rec_registry_module,
)
from celine.dataset.security.models import AuthenticatedUser

TABLE = "dataset_api.rf12_readings"
ARGS = {"column": "device_id", "url": "http://registry.invalid"}
DETAIL = "You are not a member of any community"
# A registry that predates the error codes: the detail alone.
NOT_A_MEMBER = {"detail": DETAIL}
# The registry's answer since the codes (rec-registry REQ-0047/REQ-0073).
NOT_A_MEMBER_CODED = {"detail": DETAIL, "code": "not_a_member"}


def _person() -> AuthenticatedUser:
    return AuthenticatedUser(
        sub="person-a",
        claims={"preferred_username": "person-a"},
        token="person-a-token",
    )


async def _resolve():
    return await RecRegistryHandler().resolve(
        table=TABLE, user=_person(), args=dict(ARGS), principals=None
    )


def _rendered(plan) -> str:
    parsed = parse_sql_query("SELECT device_id FROM readings")
    mapped = parsed.to_ast(tables_map={"readings": TABLE})
    return apply_row_filter_plans(mapped, [plan]).sql(dialect="postgres")


@pytest.fixture
def registry_replies(monkeypatch):
    """Answer the handler's registry call with `status` and `body`, on the wire."""
    seen: list[httpx.Request] = []

    def _install(status: int, body: bytes | dict | None):
        content = json.dumps(body).encode() if isinstance(body, dict) else (body or b"")

        def _reply(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            return httpx.Response(status, content=content)

        original = rec_registry_module.RecRegistryUserClient._get_client

        def _get_client(self, token):
            client = original(self, token)
            client._httpx_args["transport"] = httpx.MockTransport(_reply)
            return client

        monkeypatch.setattr(
            rec_registry_module.RecRegistryUserClient, "_get_client", _get_client
        )
        return seen

    return _install


# ---------------------------------------------------------------------------
# The answer the clause names
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "body",
    [
        NOT_A_MEMBER_CODED,
        # The code decides: a reworded detail is still the same answer.
        {"detail": "No membership", "code": "not_a_member"},
        # The fallback for a registry older than the codes.
        NOT_A_MEMBER,
    ],
    ids=["coded", "coded-reworded", "detail-only"],
)
async def test_no_member_answer_is_a_deny(registry_replies, body):
    seen = registry_replies(403, body)

    plan = await _resolve()

    assert [r.url.path for r in seen] == ["/user/assets"]
    assert seen[0].headers["authorization"] == "Bearer person-a-token"
    assert plan.kind == "deny"
    assert plan.predicate_template is None


async def test_no_member_deny_executes_to_no_rows(registry_replies, test_session):
    registry_replies(403, NOT_A_MEMBER_CODED)
    plan = await _resolve()
    try:
        await test_session.execute(text(f"CREATE TABLE {TABLE} (device_id TEXT)"))
        await test_session.execute(
            text(f"INSERT INTO {TABLE} (device_id) VALUES ('ex-00001')")
        )
        rows = (await test_session.execute(text(_rendered(plan)))).all()
        assert rows == []
    finally:
        await test_session.rollback()


async def test_no_member_deny_is_logged_without_the_caller(registry_replies, caplog):
    registry_replies(403, NOT_A_MEMBER_CODED)
    with caplog.at_level("DEBUG"):
        await _resolve()
    assert "no registry member" in caplog.text
    assert "person-a" not in caplog.text


# ---------------------------------------------------------------------------
# Every other failure stays an error
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "status, body",
    [
        (403, {"detail": "Access denied"}),  # a 403 from something else
        (403, b"<html>Forbidden</html>"),  # a gateway's page, not JSON
        (403, b""),  # no body at all
        (403, ["You are not a member of any community"]),  # not the registry's shape
        # Another code wins over the old detail: the registry said something else.
        (403, {"detail": DETAIL, "code": "forbidden"}),
        (403, {"detail": "Access denied", "code": "member_not_found"}),
        (403, {"detail": DETAIL, "code": ["not_a_member"]}),  # not a code string
        (403, {"code": "Not_A_Member"}),  # codes are exact
        (401, {"detail": "Authentication required"}),
        (404, NOT_A_MEMBER),  # the words, on the wrong status
        (404, NOT_A_MEMBER_CODED),  # the code, on the wrong status
        (500, NOT_A_MEMBER),
        (500, NOT_A_MEMBER_CODED),
        (503, b"upstream unavailable"),
    ],
)
async def test_other_registry_answers_stay_errors(registry_replies, status, body):
    if isinstance(body, list):
        body = json.dumps(body).encode()
    registry_replies(status, body)

    with pytest.raises(RecRegistryApiError) as exc:
        await _resolve()
    assert exc.value.status_code == status


async def test_an_unreachable_registry_stays_an_error(monkeypatch):
    def _refuse(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("registry unreachable", request=request)

    original = rec_registry_module.RecRegistryUserClient._get_client

    def _get_client(self, token):
        client = original(self, token)
        client._httpx_args["transport"] = httpx.MockTransport(_refuse)
        return client

    monkeypatch.setattr(
        rec_registry_module.RecRegistryUserClient, "_get_client", _get_client
    )
    with pytest.raises(httpx.ConnectError):
        await _resolve()


def _refusal(status: int, body: bytes | dict | list) -> RecRegistryApiError:
    """The SDK's error for `status` and `body`, built as the SDK builds it."""
    content = body if isinstance(body, bytes) else json.dumps(body).encode()
    code, detail = RecRegistryApiError.refusal_of(content)
    return RecRegistryApiError(
        "refused", status_code=status, body=content, code=code, detail=detail
    )


def test_the_match_needs_the_status_and_the_detail():
    assert rec_registry_module._is_not_a_member(_refusal(403, NOT_A_MEMBER))
    assert not rec_registry_module._is_not_a_member(_refusal(401, NOT_A_MEMBER))
    assert not rec_registry_module._is_not_a_member(
        _refusal(403, {"detail": "Access denied"})
    )
    assert not rec_registry_module._is_not_a_member(RuntimeError("403"))


def test_the_match_reads_the_code_first():
    assert rec_registry_module._is_not_a_member(_refusal(403, NOT_A_MEMBER_CODED))
    assert rec_registry_module._is_not_a_member(_refusal(403, {"code": "not_a_member"}))
    assert not rec_registry_module._is_not_a_member(_refusal(401, NOT_A_MEMBER_CODED))
    # A null code is no code: the detail fallback applies.
    assert rec_registry_module._is_not_a_member(
        _refusal(403, {"detail": DETAIL, "code": None})
    )
    assert not rec_registry_module._is_not_a_member(
        _refusal(403, {"detail": DETAIL, "code": "x"})
    )


def test_a_code_the_sdk_drops_does_not_reach_the_detail_fallback():
    """The SDK reads `code` only as a string; a list code arrives as `None`.

    That body still named a code, not ours, so the detail beside it is not the
    old registry's answer.
    """
    exc = _refusal(403, {"detail": DETAIL, "code": ["not_a_member"]})
    assert exc.code is None
    assert not rec_registry_module._is_not_a_member(exc)
    assert not rec_registry_module._is_not_a_member(_refusal(403, {"detail": DETAIL, "code": 0}))


def test_only_the_sdks_error_is_read():
    """D57: the match is the SDK's `RecRegistryApiError`. A raw `UnexpectedStatus`
    — the handler's own error before, or a generated call's — is not read,
    even carrying the registry's answer."""
    coded = json.dumps(NOT_A_MEMBER_CODED).encode()
    assert not rec_registry_module._is_not_a_member(UnexpectedStatus(403, coded))


# ---------------------------------------------------------------------------
# Through the SDK's public call
# ---------------------------------------------------------------------------


async def test_the_handler_asks_through_the_public_get_my_assets(monkeypatch):
    """D57: the handler calls `get_my_assets` with the caller's token, and
    uses no private part of the client (`_get_client`, `_get_kwargs`)."""
    calls: list[str | None] = []

    async def _get_my_assets(self, *, token=None):
        calls.append(token)
        raise RecRegistryApiError(
            "refused",
            status_code=403,
            body=json.dumps(NOT_A_MEMBER_CODED).encode(),
            code="not_a_member",
            detail=DETAIL,
        )

    def _no_private_use(self, token):  # pragma: no cover - fails the test if hit
        raise AssertionError("the handler reached a private part of the client")

    monkeypatch.setattr(RecRegistryUserClient, "get_my_assets", _get_my_assets)
    monkeypatch.setattr(RecRegistryUserClient, "_get_client", _no_private_use)

    plan = await _resolve()

    assert calls == ["person-a-token"]
    assert plan.kind == "deny"


def test_the_handler_module_holds_no_private_sdk_use():
    source = open(rec_registry_module.__file__, encoding="utf-8").read()
    for private in ("_get_client", "_get_kwargs", "UnexpectedStatus", "openapi.rec_registry"):
        assert private not in source, private


async def test_a_registry_failure_is_logged_without_its_sentence(
    registry_replies, caplog
):
    """The error's message carries the registry's sentence; the log carries the
    status and code only (QE-03), and never the caller."""
    registry_replies(403, {"detail": "a sentence naming someone", "code": "forbidden"})
    with caplog.at_level("DEBUG"), pytest.raises(RecRegistryApiError):
        await _resolve()
    assert "status=403 code=forbidden" in caplog.text
    assert "a sentence naming someone" not in caplog.text
    assert "person-a" not in caplog.text


# ---------------------------------------------------------------------------
# The 200 half, on the wire
# ---------------------------------------------------------------------------


async def test_a_member_answer_on_the_wire_narrows_to_its_sensor_ids(
    registry_replies,
):
    registry_replies(
        200,
        {
            "items": [
                {"key": "meter-1", "name": "meter-1", "asset_type": "meter",
                 "sensor_id": "ex-00001"},
                {"key": "pv-1", "name": "pv-1", "asset_type": "pv_plant",
                 "sensor_id": None},
            ],
            "total": 2,
        },
    )

    plan = await _resolve()

    assert plan.kind == "predicate"
    assert [v.name for v in plan.predicate_template.expressions] == ["ex-00001"]


async def test_a_member_owning_nothing_on_the_wire_is_rf11s_deny(registry_replies):
    """A member with no asset is a 200 with no items, not RF-12's 403."""
    registry_replies(200, {"items": [], "total": 0})

    plan = await _resolve()

    assert plan.kind == "deny"


async def test_an_unreadable_member_answer_stays_an_error(registry_replies):
    registry_replies(200, b"<html>ok</html>")

    with pytest.raises(RecRegistryApiError) as exc:
        await _resolve()
    assert exc.value.status_code == 200
