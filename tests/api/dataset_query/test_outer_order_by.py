"""QE-04 — the statement's top-level ORDER BY is carried onto the outer query.

The engine pages by wrapping the statement: `SELECT * FROM (<statement>) AS q
… LIMIT :limit OFFSET :offset`. These pin which ordering keys are rewritten to
name `q`'s columns, and that anything that cannot be named on `q` adds no outer
`ORDER BY` at all (the statement's own stays, as before). No database: the
executed half is `tests/routes/test_paginated_order.py`.
"""

from __future__ import annotations

import pytest
import sqlglot
from sqlglot import exp

from celine.dataset.api.dataset_query.pagination import outer_order_by, paginated_sql
from celine.dataset.api.dataset_query.parser import parse_sql_query


def _outer(sql: str) -> str | None:
    return outer_order_by(parse_sql_query(sql).ast)


def _keys(clause: str) -> list[tuple[str, bool]]:
    """(key, descending) per ordering term of a rendered `ORDER BY` clause."""
    order = sqlglot.parse_one(f"SELECT 1 FROM x {clause}", read="postgres").args["order"]
    return [(o.this.sql(dialect="postgres"), bool(o.args.get("desc"))) for o in order.expressions]


# ---------------------------------------------------------------------------
# Rewritten onto q
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("sql", "keys"),
    [
        # A selected column, both directions.
        ("SELECT cod_ac FROM t ORDER BY cod_ac", [("q.cod_ac", False)]),
        ("SELECT cod_ac FROM t ORDER BY cod_ac DESC", [("q.cod_ac", True)]),
        # An alias: the outer query names the output column.
        ("SELECT value AS v FROM t ORDER BY v DESC", [("q.v", True)]),
        # A key written as the selected expression, aliased.
        ("SELECT value AS v FROM t ORDER BY value", [("q.v", False)]),
        ("SELECT t.ts FROM t ORDER BY t.ts", [("q.ts", False)]),
        ("SELECT t.ts AS at FROM t ORDER BY t.ts", [("q.at", False)]),
        (
            "SELECT x, COUNT(*) AS n FROM t GROUP BY x ORDER BY COUNT(*) DESC, x",
            [("q.n", True), ("q.x", False)],
        ),
        # A position.
        ("SELECT a, b FROM t ORDER BY 2 DESC, 1", [("q.b", True), ("q.a", False)]),
        # A lone star over one source: q has that source's columns.
        ("SELECT * FROM t ORDER BY ts DESC", [("q.ts", True)]),
        ("SELECT * FROM t ORDER BY t.ts", [("q.ts", False)]),
        ("SELECT * FROM (SELECT a FROM t) AS s ORDER BY a", [("q.a", False)]),
        ("WITH c AS (SELECT a FROM t) SELECT * FROM c ORDER BY a", [("q.a", False)]),
        # Several keys, all kept, in order.
        (
            "SELECT a, b, c FROM t ORDER BY c, a DESC, b",
            [("q.c", False), ("q.a", True), ("q.b", False)],
        ),
    ],
)
def test_the_order_is_rewritten_onto_q(sql, keys):
    clause = _outer(sql)
    assert clause is not None
    assert _keys(clause) == keys


def test_a_bare_name_means_the_output_column_first():
    """As in PostgreSQL: `ORDER BY a` names the output `a`, here column `b`."""
    assert _keys(_outer("SELECT a AS b, b AS a FROM t ORDER BY a")) == [("q.a", False)]


def test_nulls_ordering_is_kept_as_the_statement_renders_it():
    clause = _outer("SELECT a, b FROM t ORDER BY a NULLS LAST, b DESC NULLS FIRST")
    inner = parse_sql_query(
        "SELECT a, b FROM t ORDER BY a NULLS LAST, b DESC NULLS FIRST"
    ).ast.args["order"].sql(dialect="postgres")
    # Same terms, same NULLS placement, only the key renamed to q's column.
    assert clause.replace("q.", "") == inner


def test_identifier_case_is_kept():
    assert _keys(_outer('SELECT "TS" FROM t ORDER BY "TS"')) == [('q."TS"', False)]
    # Unquoted folds in PostgreSQL on both sides, so the spelling may differ.
    assert _outer("SELECT TS FROM t ORDER BY ts") is not None


# ---------------------------------------------------------------------------
# Not rewritten: no outer ORDER BY, the statement's own order stays
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT x FROM t",  # nothing to carry
        "SELECT x FROM t ORDER BY y",  # a column not selected
        "SELECT lower(name) FROM t ORDER BY lower(name)",  # unnamed output
        "SELECT x FROM t ORDER BY x + 1",  # an expression not selected
        "SELECT a, a FROM t ORDER BY a",  # an output name selected twice
        "SELECT t.a, u.a FROM t JOIN u ON t.k = u.k ORDER BY t.a",  # ditto, by join
        "SELECT * FROM t JOIN u ON t.k = u.k ORDER BY t.a",  # star over a join
        "SELECT *, x AS y FROM t ORDER BY y",  # star beside another projection
        "SELECT t.* FROM t ORDER BY a",  # a qualified star
        "SELECT * FROM t ORDER BY lower(a)",  # star, and a key that is no column
        "SELECT a, b FROM t ORDER BY 3",  # a position past the projection
        "SELECT a, b FROM t ORDER BY 0",
        "SELECT a FROM t ORDER BY 'a'",  # a string is not a position
        "SELECT a, b FROM t ORDER BY a, y",  # one key unresolved: none at all
    ],
)
def test_no_outer_order_when_a_key_cannot_be_named_on_q(sql):
    assert _outer(sql) is None


def test_a_partial_order_is_never_added():
    """A prefix of the keys would reorder rows the full order ties apart."""
    assert _outer("SELECT a, b FROM t ORDER BY a, y") is None


# ---------------------------------------------------------------------------
# The page query
# ---------------------------------------------------------------------------


def test_the_page_query_orders_before_it_cuts():
    ast = parse_sql_query("SELECT cod_ac FROM t ORDER BY cod_ac DESC").ast
    page = paginated_sql(ast.sql(dialect="postgres"), ast)
    outer = sqlglot.parse_one(page.replace(":limit", "1").replace(":offset", "0"), read="postgres")
    assert isinstance(outer.args["from_"].this, exp.Subquery)
    assert outer.args["from_"].this.alias == "q"
    assert _keys(outer.args["order"].sql(dialect="postgres")) == [("q.cod_ac", True)]
    assert outer.args["limit"] is not None and outer.args["offset"] is not None
    # The statement keeps its own ORDER BY inside the subquery.
    assert outer.args["from_"].this.this.args.get("order") is not None


def test_the_page_query_without_a_carried_order_is_unchanged():
    ast = parse_sql_query("SELECT x FROM t ORDER BY y").ast
    page = paginated_sql(ast.sql(dialect="postgres"), ast)
    outer = sqlglot.parse_one(page.replace(":limit", "1").replace(":offset", "0"), read="postgres")
    assert outer.args.get("order") is None
    assert outer.args["from_"].this.this.args.get("order") is not None
