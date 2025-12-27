
import pytest
from sqlalchemy import MetaData, Table, Column, Integer, Float, DateTime, String


@pytest.fixture
def tables():
    md = MetaData()
    return {
        "solar": Table(
            "solar",
            md,
            Column("run_time_utc", DateTime),
            Column("lat", Float),
            Column("lon", Float),
            Column("value", Float),
        ),
        "weather": Table(
            "weather",
            md,
            Column("ts", DateTime),
            Column("lat", Float),
            Column("lon", Float),
            Column("temp", Float),
            Column("city", String),
        ),
    }
