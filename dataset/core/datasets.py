# dataset/core/datasets.py
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from fastapi import HTTPException

from dataset.db.models.dataset_entry import DatasetEntry


async def load_dataset_entry(*, db: AsyncSession, dataset_id: str) -> DatasetEntry:
    res = await db.execute(
        select(DatasetEntry).where(DatasetEntry.dataset_id == dataset_id)
    )
    entry = res.scalars().first()
    if not entry:
        raise HTTPException(status_code=404, detail="Dataset not found")
    return entry
