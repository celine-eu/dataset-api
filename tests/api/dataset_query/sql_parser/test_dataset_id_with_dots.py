import pytest
import sqlglot
from fastapi import HTTPException
import sqlglot.expressions

from celine.dataset.api.dataset_query.parser import parse_sql_query


def ast(sql: str):
    return sqlglot.parse_one(sql)


# ------------------------------------------------------------------------------
# Basic correctness
# ------------------------------------------------------------------------------


def test_single_dotted_dataset_id_preserved():
    sql = "SELECT * FROM prod.energy.solar_readings"
    parsed = parse_sql_query(sql)

    assert parsed.tables == {"prod.energy.solar_readings"}
    assert "prod.energy.solar_readings" in parsed.sql


def test_multiple_dotted_dataset_ids_join():
    sql = """
    SELECT s.id, w.temp
    FROM prod.energy.solar_readings s
    JOIN prod.weather.daily w
      ON s.id = w.id
    """
    parsed = parse_sql_query(sql)

    assert parsed.tables == {
        "prod.energy.solar_readings",
        "prod.weather.daily",
    }


# ------------------------------------------------------------------------------
# CTE handling
# ------------------------------------------------------------------------------


def test_cte_with_dotted_dataset_id():
    sql = """
    WITH latest AS (
        SELECT max(ts) AS ts
        FROM prod.energy.solar_readings
    )
    SELECT *
    FROM prod.energy.solar_readings
    WHERE ts = (SELECT ts FROM latest)
    """
    parsed = parse_sql_query(sql)

    # CTE name must NOT appear as a dataset
    assert parsed.tables == {"prod.energy.solar_readings"}
    assert "WITH latest AS" in parsed.sql


# ------------------------------------------------------------------------------
# Rewriting to physical tables
# ------------------------------------------------------------------------------


def test_dotted_dataset_id_rewritten_to_physical_table():
    sql = "SELECT * FROM prod.energy.solar_readings"
    parsed = parse_sql_query(sql)

    rewritten = parsed.to_sql(
        tables_map={"prod.energy.solar_readings": "dataset_api.solar_tbl"}
    )

    rewritten_ast = ast(rewritten)

    table = rewritten_ast.find(sqlglot.expressions.Table)
    assert table is not None
    assert table.name == "solar_tbl"
    assert table.db == "dataset_api"
    assert table.catalog is None or table.catalog == ""


# ------------------------------------------------------------------------------
# Stress / edge cases
# ------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "sql",
    [
        # deep namespace
        "SELECT * FROM a.b.c.d.e.f",
        # join with aliases
        "SELECT * FROM a.b.c t JOIN x.y.z u ON t.id = u.id",
        # subquery
        "SELECT * FROM a.b.c WHERE id IN (SELECT id FROM x.y.z)",
        # mixed dotted + simple
        "SELECT * FROM a.b.c JOIN simple_ds ON a.b.c.id = simple_ds.id",
    ],
)
def test_dotted_dataset_id_stress_cases(sql):
    parsed = parse_sql_query(sql)

    # Ensure NO partial stripping happens
    for name in parsed.tables:
        assert "." in name or name == "simple_ds"


# ------------------------------------------------------------------------------
# Security invariants
# ------------------------------------------------------------------------------


def test_dotted_dataset_id_does_not_enable_schema_escape():
    """
    Dotted identifiers must still be treated as dataset IDs,
    not real schema-qualified tables.
    """
    sql = "SELECT * FROM pg_catalog.pg_tables"

    parsed = parse_sql_query(sql)

    # Parser allows it syntactically,
    # but governance / resolution must catch it later
    assert parsed.tables == {"pg_catalog.pg_tables"}


def test_semicolon_still_rejected_with_dots():
    with pytest.raises(HTTPException):
        parse_sql_query("SELECT * FROM prod.energy.solar_readings; DROP TABLE x")


# ------------------------------------------------------------------------------
# Fuzz-like combinatorial stress
# ------------------------------------------------------------------------------


def test_many_dotted_tables_in_single_query():
    parts = [f"ns{i}.schema{i}.table{i}" for i in range(2)]

    sql = " SELECT * FROM " + " JOIN ".join(
        f"{p} t{i} ON t{i}.id = t0.id" if i > 0 else f"{p} t0"
        for i, p in enumerate(parts)
    )

    parsed = parse_sql_query(sql)

    assert parsed.tables == set(parts)
