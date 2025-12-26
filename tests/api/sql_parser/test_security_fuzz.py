import pytest
from hypothesis import given, strategies as st
from sqlalchemy import MetaData, Table, Column, Integer, Float, String, DateTime
from fastapi import HTTPException
import sqlglot
import sqlglot.errors

from celine.dataset.api.dataset_query.parser import parse_sql_filter


def make_sample_table():
    md = MetaData()
    return Table(
        "sample",
        md,
        Column("id", Integer),
        Column("a", Integer),
        Column("b", Integer),
        Column("name", String),
        Column("ts", DateTime),
        Column("geom", String),
        Column("value", Float),
    )


@given(sql=st.text(max_size=500))
def test_parser_never_panics(sql):
    table = make_sample_table()
    try:
        parse_sql_filter(sql, table)

    # ✅ Expected, controlled failures
    except (
        HTTPException,
        sqlglot.errors.ParseError,
        sqlglot.errors.TokenError,
        ValueError,
        TypeError,
    ):
        pass

    except Exception as e:
        pytest.fail(f"Unexpected exception type: {type(e).__name__}: {e}")
