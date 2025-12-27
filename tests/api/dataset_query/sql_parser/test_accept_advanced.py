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
