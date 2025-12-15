# dataset/api/dataset_query/executor.py
from __future__ import annotations

import json
import logging
from typing import Optional, Any

from fastapi import HTTPException, Depends, status
from fastapi.security import HTTPAuthorizationCredentials
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession


from dataset.schemas.dataset_query import DatasetQueryResult
from dataset.db.models.dataset_entry import DatasetEntry
from dataset.db.reflection import reflect_table_async
from dataset.db.engine import get_session
from dataset.security.governance import enforce_dataset_access
from dataset.api.dataset_query.parser import parse_sql_filter
from dataset.security.auth import (
    requires_auth,
    bearer_scheme,
    get_current_user,
    get_optional_user,
)
from dataset.core.datasets import load_dataset_entry
from dataset.security.governance import enforce_dataset_access

logger = logging.getLogger(__name__)


async def get_entry_dep(
    dataset_id: str,
    db: AsyncSession = Depends(get_session),
) -> DatasetEntry:
    res = await db.execute(
        select(DatasetEntry).where(DatasetEntry.dataset_id == dataset_id)
    )
    entry = res.scalars().first()
    if not entry:
        raise HTTPException(404, "Dataset not found")
    return entry


async def dataset_user_dep(
    entry: DatasetEntry = Depends(get_entry_dep),
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(bearer_scheme),
) -> Optional[dict[str, Any]]:
    if requires_auth(entry.access_level):
        if credentials is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Authentication required for this dataset",
            )
        return await get_current_user(credentials)

    return await get_optional_user(credentials)


async def execute_query(
    *,
    db: AsyncSession,
    dataset_id: str,
    filter_str: Optional[str],
    limit: int,
    offset: int,
    user: Optional[dict],
) -> DatasetQueryResult:
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
    entry = await load_dataset_entry(db=db, dataset_id=dataset_id)

    await enforce_dataset_access(entry=entry, user=user)

    if entry.backend_type != "postgres":
        raise HTTPException(400, "Querying only supported for postgres backend")

    table_name = entry.backend_config.get("table") if entry.backend_config else None
    if not table_name:
        raise HTTPException(500, "Dataset missing backend table definition")

    table = await reflect_table_async(db, table_name)

    sa_filter = parse_sql_filter(filter_str, table) if filter_str else None

    # COUNT total results
    count_stmt = select(func.count()).select_from(table)
    if sa_filter is not None:
        count_stmt = count_stmt.where(sa_filter)
    total = await db.scalar(count_stmt)

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

    return DatasetQueryResult(
        dataset_id=entry.dataset_id,
        items=items,
        offset=offset,
        limit=limit,
        count=len(items),
        total=total,
    )
