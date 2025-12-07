# dataset/api/dataset.py
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from dataset.catalogue.db import get_session
from dataset.catalogue.models import DatasetEntry
from dataset.catalogue.dcat_formatter import build_dataset

router = APIRouter()


async def _get_entry(dataset_id: str, db: AsyncSession) -> DatasetEntry:
    stmt = select(DatasetEntry).where(
        DatasetEntry.dataset_id == dataset_id,
        DatasetEntry.expose.is_(True),
    )
    res = await db.execute(stmt)
    entry = res.scalars().first()
    if not entry:
        raise HTTPException(status_code=404, detail="Dataset not found")
    return entry


@router.get("/{dataset_id}/metadata")
async def get_metadata(dataset_id: str, db: AsyncSession = Depends(get_session)):
    entry = await _get_entry(dataset_id, db)
    return await build_dataset(entry)
