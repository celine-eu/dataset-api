import pytest
from celine.dataset.api.dataset_query.parser import parse_sql_filter

@pytest.mark.parametrize("sql", [
    "",
    ";",
    "a = 1; DROP TABLE users",
])
def test_reject_bad_syntax(sql, sample_table):
    with pytest.raises(Exception):
        parse_sql_filter(sql, sample_table)
