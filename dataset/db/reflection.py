# dataset/db/reflection.py
from __future__ import annotations

from sqlalchemy import MetaData, Table
from sqlalchemy.ext.asyncio import AsyncSession

from geoalchemy2 import Geometry
from fastapi import HTTPException
import logging

logger = logging.getLogger(__name__)


async def reflect_table_async(db: AsyncSession, table_name: str) -> Table:
    metadata = MetaData()

    parts = table_name.split(".")
    if len(parts) == 3:
        _, schema, tbl = parts
    elif len(parts) == 2:
        schema, tbl = parts
    else:
        schema, tbl = None, parts[0]

    def _reflect(sync_conn):
        metadata.reflect(bind=sync_conn, only=[tbl], schema=schema, views=True)

    conn = await db.connection()
    await conn.run_sync(_reflect)

    table = metadata.tables.get(tbl)

    # Try qualified
    if table is None:
        qualified = f"{schema}.{tbl}" if schema else tbl
        table = metadata.tables.get(qualified)

    if table is None:
        raise HTTPException(500, f"Failed to lookup requested table {table_name}")

    # geometry types
    for col in table.columns:
        if (
            getattr(col.type, "datatype", None) == "geometry"
            or col.type.__class__.__name__.lower() == "geometry"
        ):
            col.type = Geometry(geometry_type="GEOMETRY", srid=4326)

    return table
