import sqlglot
import sqlglot.expressions
from celine.dataset.api.dataset_query.parser import parse_sql_query


def ast(sql: str):
    return sqlglot.parse_one(sql)


def test_simple_select():
    parsed = parse_sql_query("SELECT * FROM solar")
    assert ast(parsed.sql).find(sqlglot.expressions.Select)
    assert parsed.tables == {"solar"}


def test_simple_where():
    parsed = parse_sql_query("SELECT * FROM solar WHERE lat > 45 AND lon < 12")
    assert ast(parsed.sql).args.get("where") is not None
    assert parsed.tables == {"solar"}
