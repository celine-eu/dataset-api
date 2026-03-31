# dataset/routes/catalogue.py
from __future__ import annotations

from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from sqlalchemy import select, or_
from sqlalchemy.ext.asyncio import AsyncSession

from celine.dataset.db.models.dataset_entry import DatasetEntry
from celine.dataset.api.catalogue.dcat_formatter import build_catalog, build_dataset
from celine.dataset.db.engine import get_session

router = APIRouter()

tags = ["catalogue"]

_LD_MEDIA_TYPE = "application/ld+json"


class CatalogueSearchRequest(BaseModel):
    q: Optional[str] = None
    access_level: Optional[str] = None
    keywords: Optional[List[str]] = None


@router.get("/catalogue")
async def list_catalogue(db: AsyncSession = Depends(get_session)):
    """Return the full DCAT-AP 3 catalog as JSON-LD (application/ld+json).

    Only includes entries with expose=True. Entries with access_level='secret'
    are silently omitted even when expose=True.
    """
    stmt = select(DatasetEntry).where(DatasetEntry.expose.is_(True))
    res = await db.execute(stmt)
    entries = res.scalars().all()
    return JSONResponse(content=build_catalog(entries), media_type=_LD_MEDIA_TYPE)


@router.get("/catalogue/{dataset_id}")
async def get_catalogue_entry(
    dataset_id: str,
    db: AsyncSession = Depends(get_session),
):
    """Return a single dcat:Dataset JSON-LD document.

    Only exposed, non-secret entries are accessible here.
    """
    stmt = select(DatasetEntry).where(
        DatasetEntry.dataset_id == dataset_id,
        DatasetEntry.expose.is_(True),
    )
    res = await db.execute(stmt)
    entry = res.scalar_one_or_none()
    if entry is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found")
    if entry.access_level == "secret":
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found")
    return JSONResponse(content=build_dataset(entry), media_type=_LD_MEDIA_TYPE)


@router.post("/catalogue/search")
async def search_catalogue(
    body: CatalogueSearchRequest,
    db: AsyncSession = Depends(get_session),
):
    """Search the exposed catalogue.

    Filters (all optional, ANDed together):
    - q: full-text substring match on title and description
    - access_level: exact match on access_level
    - keywords: at least one keyword must appear in tags.keywords
    """
    stmt = select(DatasetEntry).where(DatasetEntry.expose.is_(True))
    res = await db.execute(stmt)
    entries = list(res.scalars().all())

    # Post-filter in Python (small catalogue; avoids DB-specific JSON operators)
    if body.q:
        q = body.q.lower()
        entries = [
            e for e in entries
            if q in (e.title or "").lower() or q in (e.description or "").lower()
        ]
    if body.access_level:
        entries = [e for e in entries if e.access_level == body.access_level]
    if body.keywords:
        wanted = {k.lower() for k in body.keywords}
        entries = [
            e for e in entries
            if wanted & {kw.lower() for kw in ((e.tags or {}).get("keywords") or [])}
        ]

    return JSONResponse(content=build_catalog(entries), media_type=_LD_MEDIA_TYPE)
