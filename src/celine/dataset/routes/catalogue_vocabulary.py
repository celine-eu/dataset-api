"""`GET /catalogue/{dataset_id}/vocabulary` — what this dataset's columns mean."""
from __future__ import annotations

import json
import logging

from fastapi import APIRouter, Depends, HTTPException, Response
from sqlalchemy.ext.asyncio import AsyncSession

from celine.dataset.api.catalogue.vocabulary import VocabularyError, build_vocabulary
from celine.dataset.core.datasets import load_catalogue_entry
from celine.dataset.db.engine import get_session

router = APIRouter()
tags = ["catalogue"]

logger = logging.getLogger(__name__)


@router.get("/catalogue/{dataset_id}/vocabulary")
async def dataset_vocabulary(
    dataset_id: str,
    db: AsyncSession = Depends(get_session),
):
    """The JSON-LD context for this dataset, derived from its mapping spec.

    Unauthenticated, like `/catalogue`. A consumer decides whether it *can* read
    a dataset before it negotiates access, so gating the vocabulary would gate
    discovery — and the vocabulary describes the shape of the data, not the data.

    404 means the dataset is not exposed **or** declares no mapping. That is not
    the same as "this dataset has no semantic model", and a consumer should not
    read it as such; the catalogue entry's `dct:conformsTo` is where a declared
    model is stated.
    """
    entry = await load_catalogue_entry(db=db, dataset_id=dataset_id)

    if not entry.ontology_mapping:
        raise HTTPException(
            status_code=404, detail="Dataset declares no semantic model"
        )

    conforms_to = None
    if isinstance(entry.tags, dict):
        conforms_to = entry.tags.get("conformsTo")

    try:
        document = build_vocabulary(entry.ontology_mapping, conforms_to=conforms_to)
    except VocabularyError as exc:
        # The mapping was validated at import, so reaching here means the
        # registry no longer covers a prefix it used — a vocabulary was removed
        # under a dataset that still references it. 500, because the catalogue is
        # internally inconsistent and the fix is not the caller's.
        logger.exception("Cannot derive vocabulary for %s: %s", dataset_id, exc)
        raise HTTPException(
            status_code=500, detail="Cannot derive vocabulary for this dataset"
        )

    # Explicit Response rather than returning the dict: the media type has to be
    # `application/ld+json`, and FastAPI would otherwise serve `application/json`.
    # A consumer content-negotiating for JSON-LD would skip it.
    return Response(
        content=json.dumps(document, ensure_ascii=False),
        media_type="application/ld+json",
    )
