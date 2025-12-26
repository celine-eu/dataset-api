import pytest
from celine.dataset.api.dataset_query.parser import parse_sql_filter


@pytest.mark.parametrize(
    "sql",
    [
        "a = 1 OR 1=1",
        "a = 1 AND 1=1",
    ],
)
def test_logical_expressions_allowed(sql, sample_table):
    assert parse_sql_filter(sql, sample_table) is not None


def test_comments_are_ignored(sample_table):
    assert parse_sql_filter("a = 1 -- comment", sample_table) is not None
    assert parse_sql_filter("a = 1 /* evil */", sample_table) is not None


def test_literal_with_sql_keywords_is_safe(sample_table):
    expr = parse_sql_filter("a = 'abc''; DROP TABLE t; --'", sample_table)
    assert expr is not None
