
import sqlglot
from celine.dataset.api.dataset_query.parser import parse_sql_query

def ast(sql: str):
    return sqlglot.parse_one(sql)

def test_distinct():
    parsed = parse_sql_query("SELECT DISTINCT lat FROM solar")
    assert ast(parsed.sql).args.get("distinct") is not None
    assert parsed.tables == {"solar"}

def test_order_by():
    parsed = parse_sql_query("SELECT * FROM solar ORDER BY lat DESC")
    assert ast(parsed.sql).args.get("order") is not None
    assert parsed.tables == {"solar"}
