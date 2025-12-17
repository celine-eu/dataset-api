# tests/test_sql_filter_core.py
import pytest
from sqlalchemy import Column, DateTime, Integer, MetaData, String, Table, select
from sqlalchemy.sql.elements import ColumnElement

from celine.dataset.api.dataset_query.parser import parse_sql_filter


@pytest.fixture
def test_table():
    md = MetaData()
    return Table(
        "t",
        md,
        Column("id", Integer),
        Column("temperature", Integer),
        Column("city", String),
        Column("created_at", DateTime),
    )


def render(stmt):
    return stmt.compile(compile_kwargs={"literal_binds": True})


def test_simple_eq(test_table):
    f = parse_sql_filter("city = 'Rome'", test_table)
    assert f is not None
    assert isinstance(f, ColumnElement)
    sql = str(
        select(test_table).where(f).compile(compile_kwargs={"literal_binds": True})
    )
    assert "city = 'Rome'" in sql


def test_and_condition(test_table):
    f = parse_sql_filter("temperature > 22 AND city = 'Milan'", test_table)
    assert f is not None
    sql = str(
        select(test_table).where(f).compile(compile_kwargs={"literal_binds": True})
    )
    assert "temperature > 22" in sql
    assert "city = 'Milan'" in sql


def test_or_condition(test_table):
    f = parse_sql_filter("city = 'Rome' OR city = 'Milan'", test_table)
    assert f is not None
    sql = str(
        select(test_table).where(f).compile(compile_kwargs={"literal_binds": True})
    )
    assert "city = 'Rome'" in sql
    assert "city = 'Milan'" in sql


def test_datetime_literal(test_table):
    f = parse_sql_filter("created_at >= '2025-01-01T00:00:00Z'", test_table)
    assert f is not None
    sql = str(
        select(test_table).where(f).compile(compile_kwargs={"literal_binds": True})
    )
    assert "2025-01-01" in sql


def test_nested_parens(test_table):
    f = parse_sql_filter(
        "(city = 'Rome' OR city = 'Milan') AND temperature > 20", test_table
    )
    assert f is not None
    sql = str(
        select(test_table).where(f).compile(compile_kwargs={"literal_binds": True})
    )
    assert "city = 'Rome'" in sql
    assert "city = 'Milan'" in sql
    assert "temperature > 20" in sql


def test_precedence(test_table):
    f = parse_sql_filter(
        "city = 'Rome' OR city = 'Milan' AND temperature > 30", test_table
    )
    assert f is not None
    sql = str(
        select(test_table).where(f).compile(compile_kwargs={"literal_binds": True})
    )
    assert "city = 'Rome'" in sql
    assert "city = 'Milan'" in sql
    assert "temperature > 30" in sql


def test_deep_nesting(test_table):
    f = parse_sql_filter(
        "((city = 'Rome') AND ((temperature > 20) AND (temperature < 40)))", test_table
    )
    assert f is not None
    sql = str(
        select(test_table).where(f).compile(compile_kwargs={"literal_binds": True})
    )
    assert "city = 'Rome'" in sql
    assert "temperature > 20" in sql
    assert "temperature < 40" in sql


def test_not(test_table):
    f = parse_sql_filter("NOT (city = 'Rome')", test_table)
    assert f is not None
    sql = str(
        select(test_table).where(f).compile(compile_kwargs={"literal_binds": True})
    )
    assert "city != 'Rome'" in sql or "NOT" in sql


def test_mixed_operators(test_table):
    f = parse_sql_filter(
        "temperature >= 10 AND temperature <= 30 AND city != 'Paris'", test_table
    )
    assert f is not None
    sql = str(
        select(test_table).where(f).compile(compile_kwargs={"literal_binds": True})
    )
    assert "temperature >= 10" in sql
    assert "temperature <= 30" in sql
    assert "city != 'Paris'" in sql


def test_invalid_column_raises(test_table):
    with pytest.raises(Exception):
        parse_sql_filter("nonexistent = 1", test_table)


def test_invalid_syntax_raises(test_table):
    with pytest.raises(Exception):
        parse_sql_filter("city = ", test_table)


def test_semicolon_rejected(test_table):
    with pytest.raises(Exception):
        parse_sql_filter("city = 'Rome'; DROP TABLE x", test_table)


def test_invalid_function(test_table):
    with pytest.raises(Exception):
        parse_sql_filter("hack(city)", test_table)


def test_geometry_functions_allowed(test_table):
    f = parse_sql_filter("st_distance(city, 'POINT(0 0)') > 10", test_table)
    assert f is not None
    assert isinstance(f, ColumnElement)


def test_multi_layer_boolean(test_table):
    f = parse_sql_filter(
        "(city = 'Milan' OR city = 'Rome') AND (temperature > 10 OR temperature < -5)",
        test_table,
    )
    assert f is not None
    sql = str(
        select(test_table).where(f).compile(compile_kwargs={"literal_binds": True})
    )
    assert "city = 'Milan'" in sql
    assert "city = 'Rome'" in sql
    assert "temperature > 10" in sql
    assert "temperature < -5" in sql


def test_three_level_nesting(test_table):
    f = parse_sql_filter(
        "((city = 'Rome' AND temperature > 20) OR (city = 'Milan' AND temperature < 5)) "
        "AND created_at > '2024-01-01T00:00:00Z'",
        test_table,
    )
    assert f is not None
    sql = str(
        select(test_table).where(f).compile(compile_kwargs={"literal_binds": True})
    )
    assert "city = 'Rome'" in sql
    assert "temperature > 20" in sql
    assert "city = 'Milan'" in sql
    assert "temperature < 5" in sql
    assert "2024-01-01" in sql
