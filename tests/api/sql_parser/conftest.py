import pytest
from sqlalchemy import MetaData, Table, Column, Integer, Float, String, DateTime

@pytest.fixture
def sample_table():
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
