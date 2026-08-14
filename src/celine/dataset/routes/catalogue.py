# dataset/routes/catalogue.py
from __future__ import annotations

from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from sqlalchemy import select, or_
from sqlalchemy.ext.asyncio import AsyncSession

from celine.dataset.db.models.dataset_entry import DatasetEntry
from celine.dataset.api.catalogue.dcat_formatter import build_catalog, build_dataset
from celine.dataset.core.datasets import catalogue_visible, load_catalogue_entry
from celine.dataset.core.negotiation import wants_html
from celine.dataset.db.engine import get_session
from celine.dataset.routes.views import render_dataset_page

router = APIRouter()

tags = ["catalogue"]

_LD_MEDIA_TYPE = "application/ld+json"


class CatalogueSearchRequest(BaseModel):
    q: Optional[str] = None
    access_level: Optional[str] = None
    keywords: Optional[List[str]] = None


@router.get("/catalogue")
async def list_catalogue(request: Request, db: AsyncSession = Depends(get_session)):
    """Return the full DCAT-AP 3 catalog as JSON-LD (application/ld+json).

    Only includes entries with expose=True. Entries with access_level='secret'
    are silently omitted even when expose=True.
    """
    owners = getattr(request.app.state, "owners", None)
    res = await db.execute(catalogue_visible(select(DatasetEntry)))
    entries = res.scalars().all()
    return JSONResponse(content=build_catalog(entries, owners=owners), media_type=_LD_MEDIA_TYPE)


@router.get("/catalogue/{dataset_id}")
async def get_catalogue_entry(
    dataset_id: str,
    request: Request,
    db: AsyncSession = Depends(get_session),
):
    """Return a single dcat:Dataset JSON-LD document.

    Only exposed, non-secret entries are accessible here.

    One path, two representations: a browser gets the HTML page rendered by
    `routes/views.py`, everyone else gets the document. The route is declared
    here and only here — a second declaration of the same path in another router
    would be dead code decided by include order, not by what the client asked
    for.
    """
    entry = await load_catalogue_entry(db=db, dataset_id=dataset_id)

    if wants_html(request):
        return await render_dataset_page(request=request, dataset=entry, db=db)

    owners = getattr(request.app.state, "owners", None)
    return JSONResponse(content=build_dataset(entry, owners=owners), media_type=_LD_MEDIA_TYPE)


@router.post("/catalogue/search")
async def search_catalogue(
    body: CatalogueSearchRequest,
    request: Request,
    db: AsyncSession = Depends(get_session),
):
    """Search the exposed catalogue.

    Filters (all optional, ANDed together):
    - q: full-text substring match on title and description
    - access_level: exact match on access_level
    - keywords: at least one keyword must appear in tags.keywords
    """
    owners = getattr(request.app.state, "owners", None)
    res = await db.execute(catalogue_visible(select(DatasetEntry)))
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

    return JSONResponse(content=build_catalog(entries, owners=owners), media_type=_LD_MEDIA_TYPE)
