import pytest
from celine.dataset.api.dataset_query.parser import parse_sql_filter

def test_deep_nesting(sample_table):
    sql = "(" * 50 + "a = 1" + ")" * 50
    with pytest.raises(Exception):
        parse_sql_filter(sql, sample_table)

def test_boolean_explosion(sample_table):
    sql = " OR ".join([f"a = {i}" for i in range(1000)])
    with pytest.raises(Exception):
        parse_sql_filter(sql, sample_table)
