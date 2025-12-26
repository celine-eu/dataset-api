# tests/dataset/api/dataset_query/test_sql_filter_core.py

import pytest
from datetime import datetime
import sqlglot

from sqlalchemy import (
    MetaData,
    Table,
    Column,
    Float,
    DateTime,
)
import sqlglot.expressions

from celine.dataset.api.dataset_query.parser import parse_sql_query, _function_name


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def solar_table():
    md = MetaData()
    return Table(
        "dwd_icon_d2_solar_energy",
        md,
        Column("run_time_utc", DateTime),
        Column("interval_end_utc", DateTime),
        Column("lat", Float),
        Column("lon", Float),
        Column("solar_energy_kwh_per_m2", Float),
    )


# ---------------------------------------------------------------------------
# Happy-path tests
# ---------------------------------------------------------------------------


def test_simple_select(solar_table):
    sql = """
    SELECT run_time_utc, lat, lon
    FROM dwd_icon_d2_solar_energy
    WHERE lat > 45
    """

    stmt = parse_sql_query(sql, solar_table)

    compiled = str(stmt)
    assert "SELECT" in compiled
    assert "FROM dwd_icon_d2_solar_energy" in compiled
    assert stmt.whereclause is not None


def test_select_star(solar_table):
    sql = """
    SELECT *
    FROM dwd_icon_d2_solar_energy
    """

    stmt = parse_sql_query(sql, solar_table)
    assert len(stmt.selected_columns) == len(solar_table.c)


def test_select_distinct(solar_table):
    sql = """
    SELECT DISTINCT run_time_utc
    FROM dwd_icon_d2_solar_energy
    """

    stmt = parse_sql_query(sql, solar_table)
    assert stmt._distinct is True


def test_where_between_and(solar_table):
    sql = """
    SELECT lat, lon
    FROM dwd_icon_d2_solar_energy
    WHERE lat BETWEEN 45 AND 46
      AND lon BETWEEN 11 AND 12
    """

    stmt = parse_sql_query(sql, solar_table)
    assert stmt.whereclause is not None


def test_order_by(solar_table):
    sql = """
    SELECT run_time_utc
    FROM dwd_icon_d2_solar_energy
    ORDER BY run_time_utc DESC
    """

    stmt = parse_sql_query(sql, solar_table)
    assert stmt._order_by_clauses


def test_allowed_function(solar_table):
    sql = """
    SELECT abs(lat)
    FROM dwd_icon_d2_solar_energy
    """

    stmt = parse_sql_query(sql, solar_table)
    assert stmt is not None


def test_scalar_subquery_same_table(solar_table):
    sql = """
    SELECT run_time_utc
    FROM dwd_icon_d2_solar_energy
    WHERE run_time_utc = (
        SELECT max(run_time_utc)
        FROM dwd_icon_d2_solar_energy
    )
    """

    stmt = parse_sql_query(sql, solar_table)
    assert stmt.whereclause is not None


def test_cte_same_table(solar_table):
    sql = """
    WITH latest_run AS (
        SELECT max(run_time_utc) AS run_time_utc
        FROM dwd_icon_d2_solar_energy
        WHERE run_time_utc <= '2025-12-26T04:52:29+00:00'
    )
    SELECT run_time_utc
    FROM dwd_icon_d2_solar_energy
    WHERE run_time_utc = (
        SELECT run_time_utc FROM latest_run
    )
    """

    stmt = parse_sql_query(sql, solar_table)
    compiled = str(stmt)
    assert "WITH latest_run AS" in compiled


def test_iso_datetime_literal_parsing(solar_table):
    sql = """
    SELECT *
    FROM dwd_icon_d2_solar_energy
    WHERE run_time_utc > '2025-01-01T00:00:00+00:00'
    """

    stmt = parse_sql_query(sql, solar_table)
    assert stmt.whereclause is not None


# ---------------------------------------------------------------------------
# Security / rejection tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "sql",
    [
        "DELETE FROM dwd_icon_d2_solar_energy",
        "UPDATE dwd_icon_d2_solar_energy SET lat = 0",
        "INSERT INTO dwd_icon_d2_solar_energy VALUES (1)",
        "DROP TABLE dwd_icon_d2_solar_energy",
    ],
)
def test_reject_non_select(sql, solar_table):
    with pytest.raises(Exception):
        parse_sql_query(sql, solar_table)


def test_reject_join(solar_table):
    sql = """
    SELECT *
    FROM dwd_icon_d2_solar_energy a
    JOIN other_table b ON a.lat = b.lat
    """
    with pytest.raises(Exception):
        parse_sql_query(sql, solar_table)


def test_reject_other_table(solar_table):
    sql = """
    SELECT *
    FROM other_table
    """
    with pytest.raises(Exception):
        parse_sql_query(sql, solar_table)


def test_reject_qualified_table(solar_table):
    sql = """
    SELECT *
    FROM public.dwd_icon_d2_solar_energy
    """
    with pytest.raises(Exception):
        parse_sql_query(sql, solar_table)


def test_reject_qualified_column(solar_table):
    sql = """
    SELECT dwd_icon_d2_solar_energy.lat
    FROM dwd_icon_d2_solar_energy
    """
    with pytest.raises(Exception):
        parse_sql_query(sql, solar_table)


def test_reject_forbidden_function(solar_table):
    sql = """
    SELECT pg_sleep(10)
    FROM dwd_icon_d2_solar_energy
    """
    with pytest.raises(Exception):
        parse_sql_query(sql, solar_table)


def test_reject_distinct_on(solar_table):
    sql = """
    SELECT DISTINCT ON (run_time_utc) run_time_utc
    FROM dwd_icon_d2_solar_energy
    """
    with pytest.raises(Exception):
        parse_sql_query(sql, solar_table)


def test_reject_multiple_from_sources(solar_table):
    sql = """
    SELECT *
    FROM dwd_icon_d2_solar_energy, other_table
    """
    with pytest.raises(Exception):
        parse_sql_query(sql, solar_table)


def test_reject_non_scalar_subquery(solar_table):
    sql = """
    SELECT run_time_utc
    FROM dwd_icon_d2_solar_energy
    WHERE run_time_utc = (
        SELECT run_time_utc, lat
        FROM dwd_icon_d2_solar_energy
    )
    """
    with pytest.raises(Exception):
        parse_sql_query(sql, solar_table)


def test_reject_semicolon_injection(solar_table):
    sql = """
    SELECT * FROM dwd_icon_d2_solar_energy;
    DROP TABLE users
    """
    with pytest.raises(Exception):
        parse_sql_query(sql, solar_table)


def test_reject_table_alias(solar_table):
    sql = """
    SELECT *
    FROM dwd_icon_d2_solar_energy AS t
    """
    with pytest.raises(Exception):
        parse_sql_query(sql, solar_table)


def test_from_single_source_sqlglot_shape(solar_table):
    sql = "SELECT * FROM dwd_icon_d2_solar_energy"
    stmt = parse_sql_query(sql, solar_table)
    assert stmt is not None


def test_postgis_function_name_extraction():
    expr = sqlglot.parse_one("select ST_Intersects(a, b)")
    fn = next(e for e in expr.find_all(sqlglot.expressions.Anonymous))
    assert _function_name(fn) == "st_intersects"
