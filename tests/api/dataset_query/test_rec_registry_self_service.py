"""RF-11 — a caller with no metered asset gets no rows (celine-eu/dataset-api#74).

On the self-service path (a person's own token, `principals` absent) the
`rec_registry` handler narrows rows to the sensor ids of the caller's assets. A
member with no meter yet, or with only a PV plant or a battery, owns no sensor
id, and the handler used to build `device_id IN ()` from the empty list — which
PostgreSQL rejects as a syntax error, so the query failed where it should have
answered empty. It is now a `deny` plan, the shape the delegated path uses.

The distinction RF-05 draws holds: a registry that fails, or gives no answer at
all, is still an error and never read as "owns nothing".
"""
from __future__ import annotations

import pytest
import sqlglot
from celine.sdk.openapi.rec_registry.schemas import (
    UserAssetSchema,
    UserAssetsResponseSchema,
)
from fastapi import HTTPException
from sqlalchemy import text
from sqlglot import exp

from celine.dataset.api.dataset_query.parser import parse_sql_query
from celine.dataset.api.dataset_query.row_filters.apply import apply_row_filter_plans
from celine.dataset.api.dataset_query.row_filters.handlers import RecRegistryHandler
from celine.dataset.api.dataset_query.row_filters.handlers import (
    rec_registry as rec_registry_module,
)
from celine.dataset.security.models import AuthenticatedUser

TABLE = "dataset_api.rf11_readings"
ARGS = {"column": "device_id", "url": "http://registry.invalid"}


def _member() -> AuthenticatedUser:
    return AuthenticatedUser(
        sub="member-a",
        claims={"preferred_username": "member-a"},
        token="member-a-token",
    )


def _asset(key: str, asset_type: str, sensor_id: str | None) -> UserAssetSchema:
    return UserAssetSchema(key=key, name=key, asset_type=asset_type, sensor_id=sensor_id)


def _page(*assets: UserAssetSchema) -> UserAssetsResponseSchema:
    return UserAssetsResponseSchema(items=list(assets), total=len(assets))


@pytest.fixture
def registry_answers(monkeypatch):
    """Make the SDK's `get_my_assets` answer `value` (or raise it), recording
    the token. The wire itself is driven in `test_rec_registry_not_a_member.py`."""
    calls: list[str | None] = []

    def _install(value):
        async def _get_my_assets(self, *, token=None):
            calls.append(token)
            if isinstance(value, BaseException):
                raise value
            return value

        monkeypatch.setattr(
            rec_registry_module.RecRegistryUserClient, "get_my_assets", _get_my_assets
        )
        return calls

    return _install


async def _resolve():
    return await RecRegistryHandler().resolve(
        table=TABLE, user=_member(), args=dict(ARGS), principals=None
    )


def _rendered(plan) -> str:
    parsed = parse_sql_query("SELECT device_id FROM readings")
    mapped = parsed.to_ast(tables_map={"readings": TABLE})
    return apply_row_filter_plans(mapped, [plan]).sql(dialect="postgres")


# ---------------------------------------------------------------------------
# The two cases the clause names
# ---------------------------------------------------------------------------


async def test_caller_with_no_assets_gets_a_deny(registry_answers):
    calls = registry_answers(_page())

    plan = await _resolve()

    assert calls == ["member-a-token"]
    assert plan.kind == "deny"
    assert plan.predicate_template is None


async def test_caller_whose_assets_have_no_sensor_id_gets_a_deny(registry_answers):
    registry_answers(
        _page(_asset("pv-1", "pv_plant", None), _asset("bat-1", "battery", ""))
    )

    plan = await _resolve()

    assert plan.kind == "deny"
    assert plan.predicate_template is None


async def test_the_deny_renders_without_an_in_predicate(registry_answers):
    registry_answers(_page(_asset("pv-1", "pv_plant", None)))

    rendered = _rendered(await _resolve())

    where = sqlglot.parse_one(rendered, read="postgres").find(exp.Where)
    assert where is not None
    assert list(where.find_all(exp.In)) == []
    assert "FALSE" in where.sql(dialect="postgres").upper()


async def test_the_deny_executes_and_answers_no_rows(registry_answers, test_session):
    """The failure the issue reports was a SQL error; this runs the statement."""
    registry_answers(_page())
    plan = await _resolve()
    try:
        await test_session.execute(text(f"CREATE TABLE {TABLE} (device_id TEXT)"))
        await test_session.execute(
            text(f"INSERT INTO {TABLE} (device_id) VALUES ('ex-00001'), ('ex-00002')")
        )
        rows = (await test_session.execute(text(_rendered(plan)))).all()
        assert rows == []
    finally:
        await test_session.rollback()


# ---------------------------------------------------------------------------
# What must not change
# ---------------------------------------------------------------------------


async def test_metered_assets_still_narrow_to_their_sensor_ids(registry_answers):
    registry_answers(
        _page(
            _asset("meter-1", "meter", "ex-00001"),
            _asset("pv-1", "pv_plant", None),
            _asset("meter-2", "meter", "ex-00002"),
        )
    )

    plan = await _resolve()

    assert plan.kind == "predicate"
    in_ = plan.predicate_template
    assert isinstance(in_, exp.In)
    assert sorted(v.name for v in in_.expressions) == ["ex-00001", "ex-00002"]
    assert plan.meta == {"items": 2}


async def test_no_answer_from_the_registry_is_still_an_error(registry_answers):
    """`None` is not an empty page: it is not read as "owns nothing"."""
    registry_answers(None)

    with pytest.raises(HTTPException) as exc:
        await _resolve()
    assert exc.value.status_code == 500


async def test_a_registry_failure_propagates(registry_answers):
    registry_answers(RuntimeError("registry unreachable"))

    with pytest.raises(RuntimeError):
        await _resolve()


def test_an_empty_page_is_not_falsy():
    """The note in #74: the old `if not assets` 500 never fired on an empty page.

    Pinned so a future SDK giving the page a `__len__` cannot turn every member
    without assets into a 500 through a truthiness test.
    """
    assert bool(_page()) is True
