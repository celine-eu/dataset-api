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

    if "." in table_name:
        schema, tbl = table_name.split(".", 1)
    else:
        schema, tbl = None, table_name

    def _reflect(sync_conn):
        metadata.reflect(bind=sync_conn, only=[tbl], schema=schema)

    conn = await db.connection()
    await conn.run_sync(_reflect)

    key = f"{schema}.{tbl}" if schema else tbl
    table = metadata.tables.get(key)
    if table is None:
        raise HTTPException(500, f"Table '{table_name}' not found")

    for col in table.columns:
        if (
            getattr(col.type, "datatype", None) == "geometry"
            or col.type.__class__.__name__.lower() == "geometry"
        ):
            col.type = Geometry(geometry_type="GEOMETRY", srid=4326)

    logger.debug("Successfully reflected table %s", key)
    return table
