# dataset/routes/metadata.py
from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from celine.dataset.db.engine import get_session
from celine.dataset.db.models.dataset_entry import DatasetEntry
from celine.dataset.db.reflection import reflect_table_async
from celine.dataset.api.metadata.schema_builder import build_json_schema

router = APIRouter()
tags = ["catalogue"]

logger = logging.getLogger(__name__)


async def _get_entry(dataset_id: str, db: AsyncSession) -> DatasetEntry:
    stmt = (
        select(DatasetEntry)
        .where(DatasetEntry.dataset_id == dataset_id)
        .where(DatasetEntry.expose.is_(True))
    )
    res = await db.execute(stmt)
    entry = res.scalars().first()

    if not entry:
        raise HTTPException(status_code=404, detail="Dataset not found")

    return entry


@router.get("/catalogue/{dataset_id}/schema")
async def dataset_metadata(
    dataset_id: str,
    db: AsyncSession = Depends(get_session),
):
    """
    Return ONLY the JSON schema describing the dataset's table.
    """
    entry = await _get_entry(dataset_id, db)

    table = None
    backend_table: Optional[str] = None

    if isinstance(entry.backend_config, dict):
        backend_table = entry.backend_config.get("table")

    if entry.backend_type == "postgres" and backend_table:
        try:
            table = await reflect_table_async(db, backend_table)
        except Exception as exc:
            logger.exception("Failed to reflect table %s: %s", backend_table, exc)
            raise HTTPException(status_code=500, detail="Failed to reflect table")

    schema = build_json_schema(table)
    return schema
