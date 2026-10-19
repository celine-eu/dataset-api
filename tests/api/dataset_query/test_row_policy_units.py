"""GS-10, GS-11, GS-13 — the row policy's pieces, without a database.

The integration tests (`tests/routes/test_row_policy.py`,
`tests/api/test_edr_organization_layer.py`) drive the whole query path. These pin
the parts underneath: reading the declaration, splitting it into the person and
organization layers, the layer verdicts, the undeclared-layer default, the
plan-cache key on a dataspace request, and `organization_match` on one.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from celine.dataset.api.dataset_query import executor as executor_mod
from celine.dataset.api.dataset_query.executor import _check_row_policy
from celine.dataset.api.dataset_query.row_filters import (
    RowFilterSpecError,
    get_row_filter_specs,
    split_layers,
)
from celine.dataset.api.dataset_query.row_filters.cache import TTLCache
from celine.dataset.api.dataset_query.row_filters.handlers import (
    HANDLER_BINDS,
    AllowAllHandler,
    DenyAllHandler,
    OrganizationMatchHandler,
)
from celine.dataset.api.dataset_query.row_filters.models import RowFilterPlan
from celine.dataset.api.dataset_query.row_filters.registry import RowFilterRegistry
from celine.dataset.db.models.dataset_entry import DatasetEntry
from celine.dataset.security.audit import Refused
from celine.dataset.security.models import AuthenticatedUser

TABLE = "grid.lines"


def _entry(governance: dict | None, *, access_level: str = "internal") -> DatasetEntry:
    return DatasetEntry(
        dataset_id="grid_lines",
        title="grid_lines",
        backend_type="postgres",
        backend_config={"table": TABLE},
        expose=True,
        access_level=access_level,
        lineage={"facets": {"governance": governance or {}}},
    )


def _service_account() -> AuthenticatedUser:
    return AuthenticatedUser(
        sub="service-account-example-svc",
        claims={"preferred_username": "service-account-example-svc"},
    )


# ---------------------------------------------------------------------------
# GS-11 — a declaration that cannot be read is refused, never read as none
# ---------------------------------------------------------------------------


# @verifies GS-11
@pytest.mark.parametrize(
    "row_filters",
    [
        {"handler": "allow_all", "binds": "person"},  # not a list
        ["allow_all"],  # an item that is not a mapping
        [{"binds": "person"}],  # names no handler
        [{"handler": ""}],  # empty handler
        [{"handler": "organization_match", "args": ["dso_id"]}],  # args not a mapping
    ],
)
def test_an_unreadable_declaration_raises(row_filters) -> None:
    with pytest.raises(RowFilterSpecError):
        get_row_filter_specs(_entry({"rowFilters": row_filters}))


# @verifies GS-11
def test_absent_and_empty_are_both_nothing_declared() -> None:
    assert get_row_filter_specs(_entry({})) == []
    assert get_row_filter_specs(_entry({"rowFilters": []})) == []


# @verifies GS-11
def test_a_binds_outside_the_two_layers_raises() -> None:
    entry = _entry({})
    with pytest.raises(RowFilterSpecError):
        split_layers(entry, [{"handler": "allow_all", "binds": "household"}])


# @verifies GS-10
def test_split_layers_names_both_layers_even_when_undeclared() -> None:
    entry = _entry({})
    assert split_layers(entry, []) == {"person": [], "organization": []}
    org = {"handler": "organization_match", "binds": "organization", "args": {"column": "c"}}
    person = {"handler": "allow_all", "binds": "person"}
    assert split_layers(entry, [org, person]) == {"person": [person], "organization": [org]}


# ---------------------------------------------------------------------------
# GS-10 / GS-08 — the layer verdicts
# ---------------------------------------------------------------------------


# @verifies GS-10
async def test_allow_all_narrows_nothing() -> None:
    plan = await AllowAllHandler().resolve(table=TABLE, user=_service_account(), args={})
    assert plan.kind == "predicate"
    assert plan.predicate_template is None


# @verifies GS-10
async def test_deny_all_admits_no_row() -> None:
    plan = await DenyAllHandler().resolve(table=TABLE, user=_service_account(), args={})
    assert plan.kind == "deny"


# @verifies GS-08
def test_the_layer_verdicts_bind_no_layer_of_their_own() -> None:
    # Their `binds` comes from the declaration; the import refuses one without it.
    assert HANDLER_BINDS["allow_all"] is None
    assert HANDLER_BINDS["deny_all"] is None


# ---------------------------------------------------------------------------
# GS-10 — the undeclared-layer default
# ---------------------------------------------------------------------------


@pytest.fixture()
def default(monkeypatch):
    def _set(value: str) -> None:
        monkeypatch.setattr(executor_mod.get_settings(), "row_filters_default", value)

    return _set


def _layers(person=(), organization=()) -> dict:
    return {"person": list(person), "organization": list(organization)}


PERSON_ALL = {"handler": "allow_all", "binds": "person"}
ORG_ALL = {"handler": "allow_all", "binds": "organization"}


# @verifies GS-10
def test_deny_refuses_an_undeclared_layer(default) -> None:
    default("deny")
    with pytest.raises(Refused) as exc:
        _check_row_policy(_entry({}), TABLE, _layers(person=[PERSON_ALL]), None)
    assert exc.value.status_code == 403
    assert exc.value.reason == "row_policy_undeclared"
    assert "organization" in exc.value.detail


# @verifies GS-10
def test_allow_serves_an_undeclared_layer_and_says_so(default, caplog) -> None:
    default("allow")
    audit = SimpleNamespace(reasons={})
    _check_row_policy(_entry({}), TABLE, _layers(), audit)
    assert audit.reasons == {"grid_lines": "row_policy_undeclared"}
    assert any(r.levelname == "WARNING" and "served whole" in r.getMessage() for r in caplog.records)


# @verifies GS-10
def test_both_layers_declared_passes_under_deny(default) -> None:
    default("deny")
    _check_row_policy(_entry({}), TABLE, _layers([PERSON_ALL], [ORG_ALL]), None)


# @verifies GS-10
def test_an_open_dataset_admits_an_undeclared_layer(default) -> None:
    default("deny")
    _check_row_policy(_entry({}, access_level="open"), TABLE, _layers(), None)


# @verifies GS-10
@pytest.mark.parametrize("value", ["deny", "allow"])
def test_deny_all_refuses_whatever_the_default(default, value) -> None:
    default(value)
    deny = {"handler": "deny_all", "binds": "person"}
    with pytest.raises(Refused) as exc:
        _check_row_policy(
            _entry({}, access_level="open"), TABLE, _layers([deny], [ORG_ALL]), None
        )
    assert exc.value.status_code == 403
    assert exc.value.reason == "row_policy_denied"


# ---------------------------------------------------------------------------
# GS-13 — the consumer's organizations on a dataspace request
# ---------------------------------------------------------------------------


class _Recording:
    """A handler that records each call and answers a fresh plan."""

    name = "recording"
    binds = "organization"

    def __init__(self) -> None:
        self.calls: list[list[str] | None] = []

    async def resolve(self, *, table, user, args, request_context=None, principals=None,
                      keys=None, organizations=None) -> RowFilterPlan:
        self.calls.append(organizations)
        return RowFilterPlan(table=table, kind="predicate", meta={"n": len(self.calls)})


def _registry(handler) -> RowFilterRegistry:
    return RowFilterRegistry(handlers={handler.name: handler}, cache=TTLCache(maxsize=16))


# @verifies GS-13
# @verifies RF-07
async def test_the_plan_cache_keeps_two_consumer_organizations_apart() -> None:
    handler = _Recording()
    reg = _registry(handler)
    user = _service_account()

    async def _ask(orgs):
        return await reg.resolve_with_cache(
            handler_name="recording", table=TABLE, user=user, args={}, organizations=orgs
        )

    first = await _ask(["example-dso-a"])
    again = await _ask(["example-dso-a"])
    other = await _ask(["example-dso-b"])
    nobody = await _ask([])
    self_service = await _ask(None)

    assert again is first
    assert len({id(first), id(other), id(nobody), id(self_service)}) == 4
    assert handler.calls == [["example-dso-a"], ["example-dso-b"], [], None]


# @verifies GS-13
async def test_organization_match_on_a_dataspace_request_is_not_the_service_bypass() -> None:
    plan = await OrganizationMatchHandler().resolve(
        table=TABLE,
        user=_service_account(),
        args={"column": "dso_id"},
        organizations=["example-dso-b", "example-dso-a"],
    )
    assert plan.kind == "predicate"
    assert not (plan.meta or {}).get("service")
    assert plan.meta["delegated"] is True
    sql = plan.predicate_template.sql()
    assert "dso_id" in sql and "'example-dso-a'" in sql and "'example-dso-b'" in sql


# @verifies GS-13
async def test_organization_match_with_no_consumer_organization_admits_no_row() -> None:
    plan = await OrganizationMatchHandler().resolve(
        table=TABLE, user=_service_account(), args={"column": "dso_id"}, organizations=[]
    )
    assert plan.kind == "deny"


# @verifies GS-13
async def test_organization_match_still_bypasses_a_service_reading_for_itself() -> None:
    plan = await OrganizationMatchHandler().resolve(
        table=TABLE, user=_service_account(), args={"column": "dso_id"}
    )
    assert plan.kind == "predicate"
    assert plan.meta == {"service": True}
    assert plan.predicate_template is None
