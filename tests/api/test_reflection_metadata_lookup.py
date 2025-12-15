import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker

from dataset.db.reflection import reflect_table_async


@pytest.mark.asyncio
async def test_reflection_handles_schema_qualified_key(test_engine):
    """
    This test uses a REAL PostgreSQL database, REAL schemas, REAL tables, REAL
    SQLAlchemy reflection — no mocks, no SQLite.

    It validates the exact production scenario:
    - Table lives in schema 'myschema'
    - SQLAlchemy registers metadata key 'myschema.mytable'
    - reflect_table_async must resolve:
        db.myschema.mytable
    """

    # -------------------------------------------
    # 1. Build async session
    # -------------------------------------------
    async_session = async_sessionmaker(test_engine, expire_on_commit=False)

    # -------------------------------------------
    # 2. Create a real schema + table in PostgreSQL
    # -------------------------------------------
    async with test_engine.begin() as conn:
        await conn.execute(text("CREATE SCHEMA IF NOT EXISTS myschema"))
        await conn.execute(
            text(
                """
            CREATE TABLE IF NOT EXISTS myschema.mytable (
                id INTEGER PRIMARY KEY
            )
        """
            )
        )

    async with async_session() as session:

        # -------------------------------------------
        # 3. Call the real reflection code
        # -------------------------------------------
        table = await reflect_table_async(
            session,
            "db.myschema.mytable",  # 3-part input like in your logs
        )

        # -------------------------------------------
        # 4. Assertions
        # -------------------------------------------
        assert table.name == "mytable"
        assert table.schema == "myschema"
        assert "id" in table.columns
        assert len(table.columns) == 1
