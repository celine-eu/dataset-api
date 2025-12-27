import pytest
from celine.dataset.api.dataset_query.parser import parse_sql_query


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM solar WHERE 1=1 OR 1=1",
        "SELECT * FROM solar UNION SELECT * FROM weather",
        "SELECT * FROM solar WHERE EXISTS (SELECT 1 FROM weather)",
        "SELECT * FROM solar WHERE pg_sleep(10) IS NULL",
        "SELECT * FROM solar -- comment",
    ],
)
def test_parser_rejects_structural_injection_vectors(sql: str):
    with pytest.raises(Exception):
        parse_sql_query(sql)
