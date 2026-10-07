# dataset/routes/admin.py
from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from pydantic import BaseModel

from celine.dataset.db.models.dataset_entry import DatasetEntry
from celine.dataset.db.engine import get_session, get_datasets_session
from celine.dataset.db.reflection import reflect_table_async
from celine.dataset.schemas.catalogue_import import CatalogueImportModel
from celine.dataset.core.datasets import physical_table
from celine.dataset.security.audit import CATALOGUE_IMPORT, SERVICE
from celine.dataset.security.auth import require_catalogue_admin
from celine.dataset.security.disclosure import AccessLevel
from celine.dataset.security.models import AuthenticatedUser
from celine.sdk.audit import audit_route

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


def _row_filters(ds) -> list:
    """The governance row filters an entry declares, as the query path reads them."""
    facets = (ds.lineage.facets if ds.lineage else None) or {}
    governance = facets.get("governance") or {}
    filters = governance.get("rowFilters") or governance.get("row_filters") or []
    return filters if isinstance(filters, list) else []


def _missing_filter_columns(ds, table) -> list[str]:
    """A refusal per row filter whose `args.column` the physical table lacks (GS-09).

    Such a filter would answer 400 at query time, and only to the callers it narrows
    (an organization's reader, a person); services and the platform administrator
    emit no predicate and stay green. A filter that names no column is not checked.
    """
    columns = {c.name for c in table.columns}
    refusals = []
    for index, spec in enumerate(_row_filters(ds)):
        if not isinstance(spec, dict):
            continue
        column = (spec.get("args") or {}).get("column")
        if column and column not in columns:
            refusals.append(
                f"{ds.dataset_id}: rowFilters[{index}] ({spec.get('handler')}) names "
                f"column '{column}', which {table.schema}.{table.name} does not have"
            )
    return refusals


async def _cleanup_entries(
    db: AsyncSession,
    *,
    datasets_db: AsyncSession,
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
        # Only queryable backends have a table to check; others are catalogue-only.
        table = physical_table(entry.dataset_id, entry.backend_type, entry.backend_config)
        if table is None:
            continue

        if table in skip_tables:
            logger.debug(
                "Skipping cleanup for dataset %s (table %s validated this run)",
                entry.dataset_id,
                table,
            )
            continue

        exists = await postgres_table_exists_via_reflection(datasets_db, table)
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
    # The import rewrites the gates every read passes; who ran it is recorded
    # (GS-03). A caller refused by `require_catalogue_admin` is recorded there.
    dependencies=[
        Depends(audit_route(CATALOGUE_IMPORT, user=require_catalogue_admin, service=SERVICE))
    ],
)
async def import_catalogue(
    body: CatalogueImportModel,
    db: AsyncSession = Depends(get_session),
    datasets_db: AsyncSession = Depends(get_datasets_session),
    admin: AuthenticatedUser = Depends(require_catalogue_admin),
):
    """Import or update datasets in the internal catalogue.

    Intended for use by the CLI. Idempotent upserts on dataset_id.
    """
    created = 0
    updated = 0
    validated_tables: set[str] = set()

    # Every entry is checked before anything is written: a refused import changes
    # nothing — no create, no update, no stale-entry removal (GS-09).
    accepted = []
    refusals: list[str] = []
    for ds in body.datasets:

        # The table the entry would be queried through, stated or derived from
        # its id; a postgres entry is only catalogued once that table exists.
        table = physical_table(
            ds.dataset_id,
            ds.backend_type,
            ds.backend_config.model_dump() if ds.backend_config else None,
        )
        if table is not None:
            try:
                reflected = await reflect_table_async(datasets_db, table)
            except Exception:
                logger.info(
                    "Skipping dataset %s: postgres table %s does not exist",
                    ds.dataset_id,
                    table,
                )
                continue
            refusals.extend(_missing_filter_columns(ds, reflected))
            validated_tables.add(table)
        accepted.append(ds)

    if refusals:
        logger.warning("Catalogue import refused: %s", "; ".join(refusals))
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=refusals,
        )

    for ds in accepted:

        # An entry that states no level is `internal`, stored as such, so the
        # catalogue, its search and the query path all read the same level.
        access_level = ds.access_level or AccessLevel.INTERNAL.value

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
            existing.ontology_mapping = ds.ontology_mapping
            existing.schema_override_path = ds.schema_override_path
            existing.expose = ds.expose
            existing.dataspace_expose = ds.dataspace_expose
            existing.publisher_uri = ds.publisher_uri
            existing.rights_holder_uri = ds.rights_holder_uri
            existing.license_uri = ds.license_uri
            existing.landing_page = ds.landing_page
            existing.language_uris = ds.language_uris
            existing.spatial_uris = ds.spatial_uris
            existing.access_level = access_level
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
                ontology_mapping=ds.ontology_mapping,
                schema_override_path=ds.schema_override_path,
                expose=ds.expose,
                dataspace_expose=ds.dataspace_expose,
                publisher_uri=ds.publisher_uri,
                rights_holder_uri=ds.rights_holder_uri,
                license_uri=ds.license_uri,
                landing_page=ds.landing_page,
                language_uris=ds.language_uris,
                spatial_uris=ds.spatial_uris,
                access_level=access_level,
            )
            db.add(entry)
            created += 1

    removed = await _cleanup_entries(db, datasets_db=datasets_db, skip_tables=validated_tables)
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
