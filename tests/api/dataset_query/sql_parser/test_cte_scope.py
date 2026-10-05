"""QE-05: a table reference is a CTE only where PostgreSQL would resolve it to one."""
import pytest

from celine.dataset.api.dataset_query.parser import parse_sql_query

DATASET = "ds_dev_gold.meters_data_15m"


# @verifies QE-05
@pytest.mark.parametrize("sql, physical", [
    # The CTE's own body cannot see the CTE: there the name is a table.
    (f"WITH audit_log AS (SELECT * FROM audit_log) "
     f"SELECT p.query FROM audit_log p, {DATASET} m", "audit_log"),
    # A CTE defined in a subquery is invisible outside it.
    (f"SELECT * FROM audit_log, "
     f"(WITH audit_log AS (SELECT 1 AS a) SELECT * FROM audit_log) x, {DATASET}",
     "audit_log"),
    # An earlier CTE cannot see a later one.
    (f"WITH b AS (SELECT * FROM a), a AS (SELECT * FROM {DATASET}) SELECT * FROM b", "a"),
    # A qualified name is never a CTE.
    (f"WITH audit_log AS (SELECT 1 AS a) "
     f"SELECT * FROM internal.audit_log, {DATASET}", "internal.audit_log"),
])
def test_a_name_a_cte_cannot_reach_here_is_a_dataset_reference(sql, physical):
    assert parse_sql_query(sql).tables == {DATASET, physical}


# @verifies QE-05
@pytest.mark.parametrize("sql", [
    f"WITH m AS (SELECT * FROM {DATASET}) SELECT * FROM m",
    f"WITH a AS (SELECT * FROM {DATASET}), b AS (SELECT * FROM a) SELECT * FROM b JOIN a ON true",
    f"WITH c AS (SELECT 1 AS x) SELECT * FROM {DATASET} WHERE 1 IN (SELECT x FROM c)",
    f"SELECT * FROM (WITH m AS (SELECT * FROM {DATASET}) SELECT * FROM m) s",
])
def test_a_visible_cte_is_not_a_dataset(sql):
    assert parse_sql_query(sql).tables == {DATASET}


# @verifies QE-05
def test_only_dataset_references_are_rewritten_to_physical_tables():
    """A CTE that happens to share a dataset's name keeps its name in scope."""
    parsed = parse_sql_query(
        "WITH meters AS (SELECT 1 AS a) SELECT * FROM meters, other.meters"
    )
    assert parsed.tables == {"other.meters"}

    sql = parsed.to_sql({"other.meters": "phys_other_meters", "meters": "phys_meters"})

    assert "phys_other_meters" in sql
    assert "phys_meters" not in sql.replace("phys_other_meters", "")
