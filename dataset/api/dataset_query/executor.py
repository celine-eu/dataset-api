# dataset/api/dataset_query/executor.py
from __future__ import annotations

import json
import logging
from typing import Optional

from fastapi import HTTPException
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from dataset.db.models import DatasetEntry
from dataset.db.reflection import reflect_table_async
from dataset.security.opa import authorize_dataset_query
from dataset.api.dataset_query.parser import parse_sql_filter
from dataset.core.jsonld import rows_to_jsonld  # optional JSON-LD wrapper

logger = logging.getLogger(__name__)


async def _get_entry(dataset_id: str, db: AsyncSession) -> DatasetEntry:
    from sqlalchemy import select  # local import to avoid circulars in some setups

    stmt = (
        select(DatasetEntry)
        .where(DatasetEntry.dataset_id == dataset_id)
        .where(DatasetEntry.expose.is_(True))
    )
    res = await db.execute(stmt)
    entry = res.scalars().first()
    if not entry:
        raise HTTPException(404, "Dataset not found")
    return entry


async def execute_query(
    *,
    db: AsyncSession,
    dataset_id: str,
    filter_str: Optional[str],
    limit: int,
    offset: int,
    user: Optional[dict],
):
    """
    Core query execution path:

    - resolve dataset entry
    - reflect table
    - OPA authorization
    - parse SQL filter
    - execute query
    - post-process geometry columns
    - return JSON-LD-like response
    """
    entry = await _get_entry(dataset_id, db)

    if entry.backend_type != "postgres":
        raise HTTPException(400, "Querying only supported for postgres backend")

    table_name = entry.backend_config.get("table") if entry.backend_config else None
    if not table_name:
        raise HTTPException(500, "Dataset missing backend table definition")

    table = await reflect_table_async(db, table_name)

    # OPA check
    allowed = await authorize_dataset_query(
        entry=entry, user=user, raw_filter=filter_str
    )
    if not allowed:
        raise HTTPException(403, "Not authorized to query this dataset.")

    sa_filter = parse_sql_filter(filter_str, table) if filter_str else None

    stmt = select(table)
    if sa_filter is not None:
        stmt = stmt.where(sa_filter)

    stmt = stmt.limit(limit).offset(offset)

    from sqlalchemy.dialects import postgresql

    logger.debug(
        "Rendered SQL for dataset %s:\n%s",
        dataset_id,
        stmt.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        ),
    )

    try:
        result = await db.execute(stmt)
        rows = result.mappings().all()
    except Exception as exc:  # pragma: no cover - defensive
        raise HTTPException(500, f"Query execution failed: {exc}")

    items = []
    for r in rows:
        row = dict(r)
        for col, val in list(row.items()):
            if val is None:
                continue
            if hasattr(val, "__geo_interface__"):
                row[col] = val.__geo_interface__
            elif val.__class__.__name__ == "WKBElement":
                geojson = await db.scalar(select(func.ST_AsGeoJSON(val)))
                if geojson:
                    row[col] = json.loads(geojson)
        items.append(row)

    # Keep original simple structure, or use JSON-LD helper
    # return {
    #     "@context": "https://example.org/dataset-query",
    #     "dataset": dataset_id,
    #     "limit": limit,
    #     "offset": offset,
    #     "items": items,
    # }

    # Use generic JSON-LD wrapper for consistency
    return rows_to_jsonld(
        entry=entry,
        rows=items,
        limit=limit,
        offset=offset,
        total=None,
    )
