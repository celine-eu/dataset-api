"""RF-03, RF-04, RF-05, RF-08 — the row filter matches on the consent's data keys.

A grid operator's readings are keyed by supply point. Which supply point belongs
to whom is known to the organisation that collected the members' consent and to
nobody at the operator, so the allow-list arrives in the column's own vocabulary
— typed data keys, `"pod:…"` — and this handler's whole job is to match it.

Every POD here is obviously fake (`EX000E…`); the repository is public.
"""
from __future__ import annotations

import pytest
import sqlglot
from sqlglot import exp

from celine.dataset.api.dataset_query.row_filters.handlers import (
    SubjectKeyMatchHandler,
)
from celine.dataset.api.dataset_query.row_filters.keys import (
    split_key,
    values_of_type,
)

COLUMN = "pod"
TABLE = "grid.readings"
MINE = "pod:EX000E00000001"
ALSO_MINE = "pod:EX000E00000002"
SOMEONE_ELSE = "pod:EX000E00000009"


async def _resolve(keys, *, args=None, principals=None):
    return await SubjectKeyMatchHandler().resolve(
        table=TABLE,
        user=None,
        args=args if args is not None else {"column": COLUMN, "key_type": "pod"},
        request_context={},
        principals=principals,
        keys=keys,
    )


def _literals(plan) -> list[str]:
    return [lit.this for lit in plan.predicate_template.find_all(exp.Literal)]


# ---------------------------------------------------------------------------
# RF-03 — a typed data key is "<type>:<value>"
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "key,expected",
    [
        ("pod:EX000E00000001", ("pod", "EX000E00000001")),
        # Split at the FIRST colon: a value may contain one, and a type may not.
        ("pod:EX000E:00000001", ("pod", "EX000E:00000001")),
        ("meter_id:EX000E00000001", ("meter_id", "EX000E00000001")),
        ("x:y", ("x", "y")),
    ],
)
def test_a_key_splits_at_its_first_colon(key, expected):
    assert split_key(key) == expected


@pytest.mark.parametrize(
    "key",
    [
        "",
        "EX000E00000001",  # no type
        ":EX000E00000001",  # empty type
        "pod:",  # empty value
        "POD:EX000E00000001",  # the type is a lowercase token
        "pod:EX000E 00000001",  # the value holds no space
        "1pod:EX000E00000001",  # a type starts with a letter
        "p" * 33 + ":EX000E00000001",  # 32 characters at most
    ],
)
def test_a_malformed_key_is_not_a_key(key):
    with pytest.raises(ValueError):
        split_key(key)


def test_values_of_type_selects_one_type_and_skips_what_it_cannot_read():
    """One unreadable entry narrows the answer by one subject, never widens it.

    Refusing the whole list instead would turn a single bad row at the
    collecting organisation into an outage for everyone who consented.
    """
    assert values_of_type(
        [MINE, "meter_id:EX000E00000003", "not-a-key", ALSO_MINE], "pod"
    ) == {"EX000E00000001", "EX000E00000002"}


# ---------------------------------------------------------------------------
# RF-04 — the handler matches the column against the keys of its type
# ---------------------------------------------------------------------------


async def test_the_predicate_names_the_keys_of_the_configured_type():
    plan = await _resolve([MINE, ALSO_MINE, "meter_id:EX000E00000003"])

    assert plan.kind == "predicate"
    assert _literals(plan) == ["EX000E00000001", "EX000E00000002"]
    rendered = plan.predicate_template.sql(dialect="postgres")
    assert sqlglot.parse_one(rendered, read="postgres") is not None
    assert "EX000E00000003" not in rendered  # a key of another type is not ours


async def test_the_principals_are_ignored_entirely():
    """They name the same people in a vocabulary this column does not speak.

    Matching a username against a supply point either returns nothing or, worse,
    returns something by coincidence.
    """
    plan = await _resolve([MINE], principals=["member-a", "EX000E00000009"])

    assert _literals(plan) == ["EX000E00000001"]


async def test_two_orderings_of_the_same_allow_list_render_the_same_sql():
    first = await _resolve([MINE, ALSO_MINE])
    second = await _resolve([ALSO_MINE, MINE, MINE])

    assert first.predicate_template.sql() == second.predicate_template.sql()


@pytest.mark.parametrize(
    "args",
    [
        {"key_type": "pod"},  # no column
        {"column": COLUMN},  # no key type
        {"column": COLUMN, "key_type": ""},
        {"column": "", "key_type": "pod"},
    ],
)
async def test_an_unusable_filter_is_an_error_not_an_empty_one(args):
    """A filter that cannot be applied is never permission to serve every row."""
    with pytest.raises(ValueError):
        await _resolve([MINE], args=args)


# ---------------------------------------------------------------------------
# RF-05 — an empty allow-list narrows to nothing
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "keys",
    [
        [],
        None,
        ["meter_id:EX000E00000003"],  # keys, but none of this type
        ["not-a-key"],
    ],
)
async def test_no_key_of_this_type_denies_rather_than_passing_everything(keys):
    plan = await _resolve(keys)

    assert plan.kind == "deny"
    assert plan.predicate_template is None


# ---------------------------------------------------------------------------
# RF-08 — keys never leave the predicate
# ---------------------------------------------------------------------------


async def test_the_plans_meta_carries_a_count_and_never_a_key():
    """`meta` is the half of a plan that reaches an audit record."""
    plan = await _resolve([MINE, ALSO_MINE, SOMEONE_ELSE])

    assert plan.meta == {"items": 3, "key_type": "pod"}
    assert "EX000E" not in repr(plan.meta)
