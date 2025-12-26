# dataset/routes/catalogue.py
from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from celine.dataset.db.models.dataset_entry import DatasetEntry
from celine.dataset.api.catalogue.dcat_formatter import build_catalog
from celine.dataset.db.engine import get_session

router = APIRouter()


@router.get("/catalogue")
async def list_catalogue(db: AsyncSession = Depends(get_session)):
    stmt = select(DatasetEntry).where(DatasetEntry.expose.is_(True))
    res = await db.execute(stmt)
    entries = res.scalars().all()
    return build_catalog(entries)
