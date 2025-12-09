# dataset/routes/admin.py
from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from pydantic import BaseModel

from dataset.db.engine import get_session
from dataset.db.models import DatasetEntry
from dataset.schemas.catalogue_import import CatalogueImportModel

logger = logging.getLogger(__name__)

router = APIRouter()


class CatalogueImportResponse(BaseModel):
    created: int
    updated: int


@router.post(
    "/catalogue",
    response_model=CatalogueImportResponse,
    status_code=status.HTTP_200_OK,
)
async def import_catalogue(
    body: CatalogueImportModel,
    db: AsyncSession = Depends(get_session),
):
    """Import or update datasets in the internal catalogue.

    Intended for use by the CLI. Idempotent upserts on dataset_id.
    """
    created = 0
    updated = 0

    for ds in body.datasets:
        # Check if dataset already exists
        stmt = select(DatasetEntry).where(DatasetEntry.dataset_id == ds.dataset_id)
        res = await db.execute(stmt)
        existing = res.scalars().first()

        backend_config = ds.backend_config.model_dump() if ds.backend_config else None
        lineage = ds.lineage.model_dump() if ds.lineage else None
        tags = ds.tags.model_dump() if ds.tags else None

        if existing:
            existing.title = ds.title
            existing.description = ds.description
            existing.backend_type = ds.backend_type
            existing.backend_config = backend_config
            existing.lineage = lineage
            existing.tags = tags
            existing.ontology_path = ds.ontology_path
            existing.schema_override_path = ds.schema_override_path
            existing.expose = ds.expose
            existing.publisher_uri = ds.publisher_uri
            existing.rights_holder_uri = ds.rights_holder_uri
            existing.license_uri = ds.license_uri
            existing.landing_page = ds.landing_page
            existing.language_uris = ds.language_uris
            existing.spatial_uris = ds.spatial_uris
            updated += 1
        else:
            entry = DatasetEntry(
                dataset_id=ds.dataset_id,
                title=ds.title,
                description=ds.description,
                backend_type=ds.backend_type,
                backend_config=backend_config,
                tags=tags,
                lineage=lineage,
                ontology_path=ds.ontology_path,
                schema_override_path=ds.schema_override_path,
                expose=ds.expose,
                publisher_uri=ds.publisher_uri,
                rights_holder_uri=ds.rights_holder_uri,
                license_uri=ds.license_uri,
                landing_page=ds.landing_page,
                language_uris=ds.language_uris,
                spatial_uris=ds.spatial_uris,
            )
            db.add(entry)
            created += 1

    try:
        await db.commit()
    except Exception as exc:  # pragma: no cover - defensive
        logger.exception("Failed to import catalogue: %s", exc)
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to import catalogue.",
        )

    logger.info("Catalogue import completed. created=%d updated=%d", created, updated)
    return CatalogueImportResponse(created=created, updated=updated)
