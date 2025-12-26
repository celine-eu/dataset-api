from celine.dataset.api.dataset_query.parser import parse_sql_filter

def test_parentheses(sample_table):
    expr = parse_sql_filter("(a = 1 OR b = 2) AND value > 3", sample_table)
    assert expr is not None

def test_not(sample_table):
    expr = parse_sql_filter("NOT (a = 1)", sample_table)
    assert expr is not None
