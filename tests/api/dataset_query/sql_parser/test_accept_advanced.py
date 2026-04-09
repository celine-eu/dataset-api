import logging

import sqlglot
import sqlglot.expressions
from celine.dataset.api.dataset_query.parser import parse_sql_query


def ast(sql: str):
    return sqlglot.parse_one(sql)


def test_join():
    parsed = parse_sql_query(
        "SELECT s.lat, w.temp FROM solar s JOIN weather w ON s.lat = w.lat"
    )
    assert len(list(ast(parsed.sql).find_all(sqlglot.expressions.Join))) == 1
    assert parsed.tables == {"solar", "weather"}


def test_cast():
    parsed = parse_sql_query(
        "SELECT CAST(value AS DOUBLE) FROM measurements"
    )
    assert parsed.tables == {"measurements"}


def test_is_null():
    parsed = parse_sql_query(
        "SELECT * FROM measurements WHERE value IS NULL"
    )
    assert parsed.tables == {"measurements"}


def test_is_not_null():
    parsed = parse_sql_query(
        "SELECT * FROM measurements WHERE value IS NOT NULL"
    )
    assert parsed.tables == {"measurements"}


def test_array_agg_with_filter():
    parsed = parse_sql_query(
        """
        SELECT
            array_agg(DISTINCT parent_substation_name ORDER BY parent_substation_name)
                FILTER (WHERE parent_substation_name IS NOT NULL) AS parent_substations,
            array_agg(DISTINCT line_name ORDER BY line_name)
                FILTER (WHERE line_name IS NOT NULL) AS lines
        FROM ds_dev_gold.grid_network_topology
        """
    )
    assert parsed.tables == {"ds_dev_gold.grid_network_topology"}


def test_tautological_predicate_allowed_with_warning(caplog):
    """WHERE 1=1 is a common dynamic-query pattern — allowed but logged as a warning."""
    with caplog.at_level(logging.WARNING, logger="celine.dataset.api.dataset_query.parser"):
        parsed = parse_sql_query(
            """
            SELECT risk_level, COUNT(*) AS events
            FROM ds_dev_gold.grid_heat_risks
            WHERE 1=1
            GROUP BY risk_level
            ORDER BY events DESC
            """
        )
    assert parsed.tables == {"ds_dev_gold.grid_heat_risks"}
    assert any("Tautological predicate" in r.message for r in caplog.records)


def test_cte_and_subquery():
    parsed = parse_sql_query(
        """
        WITH latest AS (
            SELECT max(run_time_utc) AS ts FROM solar
        )
        SELECT * FROM solar
        WHERE run_time_utc = (SELECT ts FROM latest)
        """
    )
    assert ast(parsed.sql).find(sqlglot.expressions.With) is not None
    assert parsed.tables == {"solar"}
