# dataset/routes/admin.py
from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from pydantic import BaseModel

from celine.dataset.db.models.dataset_entry import DatasetEntry
from celine.dataset.db.engine import get_session
from celine.dataset.db.reflection import reflect_table_async
from celine.dataset.schemas.catalogue_import import CatalogueImportModel

logger = logging.getLogger(__name__)

router = APIRouter()
tags = ["catalogue"]


class CatalogueImportResponse(BaseModel):
    created: int
    updated: int


async def postgres_table_exists_via_reflection(
    db: AsyncSession,
    table_name: str,
) -> bool:
    try:
        await reflect_table_async(db, table_name)
        return True
    except HTTPException:
        return False
    except Exception:
        return False


async def _cleanup_entries(
    db: AsyncSession,
    *,
    skip_tables: set[str] | None = None,
) -> int:
    """
    Remove catalogue entries whose physical backend no longer exists.

    Returns the number of removed entries.
    """
    removed = 0
    skip_tables = skip_tables or set()

    stmt = select(DatasetEntry)
    res = await db.execute(stmt)
    entries = res.scalars().all()

    for entry in entries:
        # Only physical backends are checked for now
        if entry.backend_type != "postgres":
            continue

        backend_config = entry.backend_config or {}
        table = backend_config.get("table")
        if not table:
            logger.info(
                "Removing dataset %s: missing backend table reference",
                entry.dataset_id,
            )
            await db.delete(entry)
            removed += 1
            continue

        if table in skip_tables:
            logger.debug(
                "Skipping cleanup for dataset %s (table %s validated this run)",
                entry.dataset_id,
                table,
            )
            continue

        exists = await postgres_table_exists_via_reflection(db, table)
        if not exists:
            logger.info(
                "Removing dataset %s: postgres table %s no longer exists",
                entry.dataset_id,
                table,
            )
            await db.delete(entry)
            removed += 1

    return removed


@router.post(
    "/admin/catalogue",
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
    validated_tables: set[str] = set()

    for ds in body.datasets:

        if ds.backend_type == "postgres":
            table = ds.backend_config.table if ds.backend_config else None
            if table and not await postgres_table_exists_via_reflection(db, table):
                logger.info(
                    "Skipping dataset %s: postgres table %s does not exist",
                    ds.dataset_id,
                    table,
                )
                continue
            if table:
                validated_tables.add(table)

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
            existing.access_level = ds.access_level
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
                access_level=ds.access_level,
            )
            db.add(entry)
            created += 1

    removed = await _cleanup_entries(db, skip_tables=validated_tables)
    if removed:
        logger.info("Catalogue cleanup removed %d stale entries", removed)

    try:
        await db.commit()
    except Exception as exc:  # pragma: no cover - defensive
        logger.exception("Failed to import catalogue: %s", exc)
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to import catalogue.",
        )

    logger.info(
        "Catalogue import completed. created=%d updated=%d removed=%d",
        created,
        updated,
        removed,
    )

    return CatalogueImportResponse(created=created, updated=updated)
