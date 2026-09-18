"""RF-05, RF-07, RF-09 — what an allow-list means, and what a plan is cached by.

A delegated request is one where the rows belong to the consenting subjects and
not to the caller — who is a service identity owning none of it. The decision
names those subjects twice: as `principals` (identifiers native to this system)
and as `keys` (the values this holder already stores their rows under).

`[]` is a delegated list naming nobody. It is emphatically not the self-service
case, and until typed keys arrived every handler read it as one, because it
tested the list for truth rather than for presence. That mattered the day ds
gained a decision that names its subjects by key alone: `principals` is then
empty, and `rec_registry` answered it by taking the service-account bypass —
every row in the table, for a decision that named no member at all.
"""
from __future__ import annotations

import pytest

from celine.dataset.api.dataset_query.row_filters.cache import TTLCache
from celine.dataset.api.dataset_query.row_filters.handlers import (
    DirectUserMatchHandler,
    HttpInListHandler,
    RecRegistryHandler,
    SubjectKeyMatchHandler,
    TablePointerHandler,
)
from celine.dataset.api.dataset_query.row_filters.models import RowFilterPlan
from celine.dataset.api.dataset_query.row_filters.registry import RowFilterRegistry
from celine.dataset.security.models import AuthenticatedUser

TABLE = "grid.readings"
POD_KEY = "pod:EX000E00000001"


def _service_account() -> AuthenticatedUser:
    """The identity a dataspace query actually arrives on."""
    return AuthenticatedUser(
        sub="service-account-svc-ds-dataset-api",
        claims={"preferred_username": "service-account-svc-ds-dataset-api"},
    )


# ---------------------------------------------------------------------------
# RF-05 — an empty allow-list narrows to nothing
# ---------------------------------------------------------------------------


async def test_direct_user_match_denies_an_empty_delegated_list():
    plan = await DirectUserMatchHandler().resolve(
        table=TABLE,
        user=_service_account(),
        args={"column": "owner"},
        principals=[],
        keys=[POD_KEY],
    )

    # Not the caller's own rows, and not `IN ()` — which is a syntax error in
    # one dialect and a tautology in another.
    assert plan.kind == "deny"


async def test_direct_user_match_still_serves_the_caller_when_not_delegated():
    """The half that must not regress: `None` is self-service."""
    plan = await DirectUserMatchHandler().resolve(
        table=TABLE,
        user=AuthenticatedUser(sub="member-a"),
        args={"column": "owner"},
        principals=None,
    )

    assert plan.kind == "predicate"
    assert "member-a" in plan.predicate_template.sql()


async def test_rec_registry_denies_an_empty_delegated_list_without_asking_anyone():
    """The leak this closes: the bypass below is correct only for self-service."""
    handler = RecRegistryHandler()

    async def _never(*args, **kwargs):
        raise AssertionError("the registry must not be asked about nobody")

    handler._lookup_assets = _never  # type: ignore[method-assign]

    plan = await handler.resolve(
        table=TABLE,
        user=_service_account(),
        args={"column": "device_id", "url": "http://registry.invalid"},
        principals=[],
        keys=[POD_KEY],
    )

    assert plan.kind == "deny"


async def test_rec_registry_still_bypasses_for_a_service_account_of_its_own():
    """Unchanged: no delegation at all means the old self-service behaviour."""
    plan = await RecRegistryHandler().resolve(
        table=TABLE,
        user=_service_account(),
        args={"column": "device_id", "url": "http://registry.invalid"},
        principals=None,
    )

    assert plan.kind == "predicate"
    assert plan.predicate_template is None


@pytest.mark.parametrize(
    "handler", [HttpInListHandler(), TablePointerHandler()], ids=lambda h: h.name
)
@pytest.mark.parametrize(
    "delegated", [{"principals": []}, {"principals": ["member-a"]}, {"keys": [POD_KEY]}]
)
async def test_a_self_service_only_handler_refuses_every_delegated_shape(
    handler, delegated
):
    """Refusing beats falling through to the caller's own filter.

    In a delegated request the caller is a service identity — exactly the case
    that returns everything.
    """
    with pytest.raises(NotImplementedError):
        await handler.resolve(
            table=TABLE,
            user=_service_account(),
            args={"column": "device_id", "url": "http://elsewhere.invalid"},
            **delegated,
        )


# ---------------------------------------------------------------------------
# RF-07 — a plan is cached by whose rows it is for
# ---------------------------------------------------------------------------


class _Recorder:
    """Answers with a plan naming the call number, so a reuse is visible."""

    name = "recorder"

    def __init__(self) -> None:
        self.calls: list[dict] = []

    async def resolve(self, *, table, user, args, request_context=None,
                      principals=None, keys=None):
        self.calls.append({"principals": principals, "keys": keys})
        return RowFilterPlan(
            table=table, kind="predicate", meta={"call": len(self.calls)}
        )


def _registry(handler) -> RowFilterRegistry:
    return RowFilterRegistry(handlers={handler.name: handler}, cache=TTLCache(100))


async def _resolve(reg, handler_name="recorder", **kwargs):
    return await reg.resolve_with_cache(
        handler_name=handler_name,
        table=TABLE,
        user=None,
        args={"column": "pod"},
        ttl_override=60,
        **kwargs,
    )


async def test_two_consents_with_different_keys_do_not_share_a_plan():
    """The failure this prevents is the second consent being served the first's rows.

    A `subject_key_match` decision often carries no principals at all, so
    without the keys in the key every one of them collides into one entry.
    """
    handler = _Recorder()
    reg = _registry(handler)

    first = await _resolve(reg, principals=[], keys=[POD_KEY])
    second = await _resolve(reg, principals=[], keys=["pod:EX000E00000002"])

    assert first.meta == {"call": 1}
    assert second.meta == {"call": 2}


async def test_the_same_allow_list_is_resolved_once():
    handler = _Recorder()
    reg = _registry(handler)

    await _resolve(reg, principals=[], keys=[POD_KEY])
    again = await _resolve(reg, principals=[], keys=[POD_KEY])

    assert again.meta == {"call": 1}
    assert len(handler.calls) == 1


async def test_a_delegated_request_naming_nobody_is_not_the_self_service_plan():
    handler = _Recorder()
    reg = _registry(handler)

    self_service = await _resolve(reg, principals=None)
    delegated = await _resolve(reg, principals=[])

    assert self_service.meta == {"call": 1}
    assert delegated.meta == {"call": 2}


async def test_the_cache_key_does_not_carry_the_keys_in_clear():
    """Keys are personal data; a cache key is the kind of string that gets logged."""
    handler = _Recorder()
    reg = _registry(handler)

    await _resolve(reg, principals=[], keys=[POD_KEY])

    stored = list(reg.cache._store.keys())
    assert stored and all("EX000E" not in k for k in stored)


# ---------------------------------------------------------------------------
# RF-09 — a handler written before typed keys keeps working
# ---------------------------------------------------------------------------


class _Older:
    """A handler packaged elsewhere, whose `resolve` predates `keys`."""

    name = "older"

    async def resolve(self, *, table, user, args, request_context=None,
                      principals=None):
        return RowFilterPlan(table=table, kind="predicate", meta={"seen": principals})


async def test_a_handler_that_predates_typed_keys_is_not_handed_them():
    handler = _Older()
    reg = _registry(handler)

    plan = await _resolve(
        reg, handler_name="older", principals=["member-a"], keys=[POD_KEY]
    )

    assert plan.meta == {"seen": ["member-a"]}


async def test_a_handler_taking_kwargs_is_handed_them():
    class _Open:
        name = "open"

        async def resolve(self, **kw):
            return RowFilterPlan(table=TABLE, kind="predicate", meta={"got": "keys" in kw})

    reg = _registry(_Open())
    plan = await _resolve(reg, handler_name="open", principals=[], keys=[POD_KEY])

    assert plan.meta == {"got": True}


async def test_the_built_in_handlers_all_accept_keys():
    """So that a new one is never silently skipped by the compatibility path."""
    reg = RowFilterRegistry(handlers={}, cache=TTLCache(10))
    for handler in (
        DirectUserMatchHandler(),
        RecRegistryHandler(),
        SubjectKeyMatchHandler(),
        HttpInListHandler(),
        TablePointerHandler(),
    ):
        assert reg._accepts_keys(handler), handler.name
