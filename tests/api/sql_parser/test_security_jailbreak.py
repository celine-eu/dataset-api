import pytest
from celine.dataset.api.dataset_query.parser import parse_sql_filter

@pytest.mark.parametrize("sql", [
    "a::int = 1",
    "CAST(a AS int) = 1",
    "sel/**/ect 1",
])
def test_jailbreak(sql, sample_table):
    with pytest.raises(Exception):
        parse_sql_filter(sql, sample_table)
