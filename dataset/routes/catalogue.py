# dataset/routes/catalogue.py
from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dataset.db.engine import get_session
from dataset.db.models.dataset_entry import DatasetEntry
from dataset.api.catalogue.dcat_formatter import build_catalog

router = APIRouter()


@router.get("/catalogue")
async def list_catalogue(db: AsyncSession = Depends(get_session)):
    stmt = select(DatasetEntry).where(DatasetEntry.expose.is_(True))
    res = await db.execute(stmt)
    entries = res.scalars().all()
    return build_catalog(entries)
