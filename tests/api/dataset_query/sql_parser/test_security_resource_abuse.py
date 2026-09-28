
import pytest
import sqlglot
from fastapi import HTTPException
from celine.dataset.api.dataset_query.parser import parse_sql_query

def ast(sql: str):
    return sqlglot.parse_one(sql)

def test_deep_subquery_allowed_but_parses():
    parsed = parse_sql_query(
        "SELECT * FROM solar WHERE lat > (SELECT avg(lat) FROM solar)"
    )
    assert ast(parsed.sql)
    assert parsed.tables == {"solar"}


# QE-01 — `ST_Simplify` over a whole table.
#
# Simplifying every shape in a table is CPU, not reach. The parser admits it,
# like any aggregate over a whole table; what bounds it is the execution guards
# behind the parser (`MAX_LIMIT` and the statement timeout), not a rule here.
# What the parser does still bound is structure: a call nested without end is
# refused, admitted function or not.


def test_st_simplify_over_whole_table_parses():
    parsed = parse_sql_query(
        "SELECT id, ST_AsGeoJSON(ST_Simplify(geometry, 0.0)) FROM boundaries"
    )
    assert parsed.tables == {"boundaries"}


def test_st_simplify_nested_without_end_refused():
    expr = "geometry"
    for _ in range(300):
        expr = f"ST_Simplify({expr}, 0.001)"
    with pytest.raises(HTTPException) as exc:
        parse_sql_query(f"SELECT ST_AsGeoJSON({expr}) FROM boundaries")
    assert exc.value.status_code == 400
