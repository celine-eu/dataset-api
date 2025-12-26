from celine.dataset.api.dataset_query.parser import parse_sql_filter

def test_simple_comparison(sample_table):
    expr = parse_sql_filter("a = 1", sample_table)
    assert expr is not None

def test_and_or(sample_table):
    expr = parse_sql_filter("a = 1 AND b = 2", sample_table)
    assert expr is not None
