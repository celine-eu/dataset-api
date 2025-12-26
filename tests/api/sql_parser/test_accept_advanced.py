from celine.dataset.api.dataset_query.parser import parse_sql_filter

def test_function_allowed(sample_table):
    expr = parse_sql_filter("abs(value) > 1", sample_table)
    assert expr is not None

def test_subquery_scalar(sample_table):
    expr = parse_sql_filter(
        "a = (SELECT max(a) FROM sample)",
        sample_table
    )
    assert expr is not None
