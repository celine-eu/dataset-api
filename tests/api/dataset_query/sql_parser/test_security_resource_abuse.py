
import sqlglot
from celine.dataset.api.dataset_query.parser import parse_sql_query

def ast(sql: str):
    return sqlglot.parse_one(sql)

def test_deep_subquery_allowed_but_parses():
    parsed = parse_sql_query(
        "SELECT * FROM solar WHERE lat > (SELECT avg(lat) FROM solar)"
    )
    assert ast(parsed.sql)
    assert parsed.tables == {"solar"}
