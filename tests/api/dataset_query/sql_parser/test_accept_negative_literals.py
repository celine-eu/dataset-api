"""QE-02 — a unary minus is admitted on a numeric literal, and on nothing else.

A coordinate west of Greenwich or south of the equator is a negative number, and
`ST_Point(-0.05, 0.05)` used to be refused as an unsupported `Neg`. The widening
is the narrowest that serves it: `exp.Neg` over a numeric `exp.Literal`. A minus
over anything else — a column, a parenthesised expression, a subquery, another
minus, a string, a cast — is still refused. None of those is a constant number,
and no reader needs them: `a - b` is subtraction, a different node, admitted as
before.
"""

import pytest
import sqlglot
from fastapi import HTTPException

from celine.dataset.api.dataset_query.parser import parse_sql_query


def _negs(sql: str) -> list[sqlglot.exp.Neg]:
    return list(sqlglot.parse_one(sql).find_all(sqlglot.exp.Neg))


# ---------------------------------------------------------------------------
# Accepted
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT -1 AS n FROM readings",
        "SELECT -0.5 AS n FROM readings",
        "SELECT -1e3 AS n FROM readings",
        "SELECT value FROM readings WHERE value > -1",
        "SELECT value FROM readings WHERE value BETWEEN -0.5 AND 0.5",
        "SELECT value - -1 AS n FROM readings",  # subtraction of a negative literal
        "SELECT value FROM readings WHERE value IN (-1, -2)",
        "SELECT GREATEST(value, -1) FROM readings",
    ],
)
def test_negative_numeric_literal_accepted(sql):
    parsed = parse_sql_query(sql)
    assert parsed.tables == {"readings"}
    assert _negs(parsed.sql), "the minus survives into the rendered statement"


def test_negative_coordinates_inside_st_point():
    """The case D49 was decided for: a point with negative coordinates."""
    parsed = parse_sql_query(
        "SELECT cod_ac FROM boundaries "
        "WHERE ST_Intersects(geometry, ST_SetSRID(ST_Point(-0.05, -0.1), 4326)) "
        "ORDER BY cod_ac"
    )
    assert parsed.tables == {"boundaries"}
    rendered = parsed.sql
    assert "-0.05" in rendered and "-0.1" in rendered


# ---------------------------------------------------------------------------
# Refused
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT -value FROM readings",  # a column
        "SELECT -(value) FROM readings",  # a parenthesised expression
        "SELECT -(1) FROM readings",  # even a parenthesised literal
        "SELECT -(value + 1) FROM readings",
        "SELECT -(SELECT MAX(value) FROM readings) FROM readings",  # a subquery
        "SELECT - -1 FROM readings",  # double negation
        "SELECT -'1' FROM readings",  # a string literal
        "SELECT -CAST(1 AS INT) FROM readings",  # a cast
        "SELECT -1::int FROM readings",  # parsed as a minus over a cast
        "SELECT -ABS(value) FROM readings",  # a function call
        "SELECT ST_Point(-lon, lat) FROM readings",  # a column inside an admitted call
    ],
)
def test_unary_minus_on_anything_else_refused(sql):
    with pytest.raises(HTTPException) as exc:
        parse_sql_query(sql)
    assert exc.value.status_code == 400
    assert "Unary minus" in exc.value.detail


def test_double_dash_is_still_a_comment():
    """`--1` is not a double negation: it opens a comment, refused before parsing."""
    with pytest.raises(HTTPException) as exc:
        parse_sql_query("SELECT --1\nvalue FROM readings")
    assert exc.value.status_code == 400
    assert "comment" in exc.value.detail.lower()


def test_negative_literal_does_not_smuggle_a_forbidden_function():
    """The minus admits its literal operand; a sibling call is still checked."""
    with pytest.raises(HTTPException) as exc:
        parse_sql_query("SELECT -1, pg_read_file('/etc/passwd') FROM readings")
    assert exc.value.status_code == 400
