from __future__ import annotations

import json
import logging
from typing import Optional, Dict, Sequence
from fastapi import HTTPException
from sqlalchemy import RowMapping, Table, text, select, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import DBAPIError

from celine.dataset.schemas.dataset_query import DatasetQueryResult
from celine.dataset.db.models.dataset_entry import DatasetEntry
from celine.dataset.db.reflection import reflect_table_async
from celine.dataset.core.datasets import load_dataset_entry
from celine.dataset.security.governance import (
    enforce_dataset_access,
    resolve_datasets_for_tables,
)
from celine.dataset.security.models import AuthenticatedUser
from celine.dataset.api.dataset_query.parser import parse_sql_query

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Limits
# ---------------------------------------------------------------------------

DEFAULT_LIMIT = 100
MAX_LIMIT = 10_000
STATEMENT_TIMEOUT_MS = 2000  # 2 seconds

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _clamp_limit(limit: int) -> int:
    if limit <= 0:
        return DEFAULT_LIMIT
    return min(limit, MAX_LIMIT)


# ---------------------------------------------------------------------------
# Main executor
# ---------------------------------------------------------------------------


async def _execute_sql_with_timeout(
    db,
    sql: str,
    params: dict | None = None,
    timeout: int = STATEMENT_TIMEOUT_MS,
):
    try:
        await db.execute(text(f"SET LOCAL statement_timeout = {timeout}"))

        return await db.execute(text(sql), params or {})

    except DBAPIError as exc:
        logger.debug(f"Query failed sql={sql} exception={exc}")
        if "statement timeout" in str(exc).lower():
            raise HTTPException(400, "Query exceeded time limit") from None
        raise HTTPException(400, "Database query failed") from None


async def execute_rows_with_timeout(
    db,
    sql: str,
    params: dict | None = None,
) -> Sequence[RowMapping]:
    result = await _execute_sql_with_timeout(db, sql, params)
    return result.mappings().all()


async def execute_scalar_with_timeout(
    db,
    sql: str,
    params: dict | None = None,
) -> int:
    result = await _execute_sql_with_timeout(db, sql, params)
    return int(result.scalar_one())


async def execute_query(
    *,
    db: AsyncSession,
    raw_sql: Optional[str],
    limit: int,
    offset: int,
    user: Optional[AuthenticatedUser],
) -> DatasetQueryResult:
    """
    Execute a validated SQL query against a dataset.

    Guarantees:
    - dataset access enforced (OPA / disclosure)
    - SQL validated (SELECT-only, table allowlist)
    - LIMIT/OFFSET enforced server-side
    - hard row cap applied
    """
    # ------------------------------------------------------------------
    # Validate SQL
    # ------------------------------------------------------------------

    if raw_sql is None or raw_sql.strip() == "":
        raise HTTPException(400, "sql query not provided")

    logger.debug(f"Parsing raw SQL: {raw_sql}")
    try:
        parsed = parse_sql_query(raw_sql)
    except HTTPException as exc:
        logger.error(f"SQL validation failed: {exc}")
        raise
    except Exception as exc:
        logger.exception("SQL validation failed")
        raise HTTPException(400, str(exc)) from exc

    if not parsed.tables:
        raise HTTPException(400, "Query references no datasets")

    datasets = await resolve_datasets_for_tables(db=db, table_names=parsed.tables)
    tables_map: dict[str, str] = {}
    for ref_table, ds in datasets.items():
        if not ds.expose:
            raise HTTPException(403, "Dataset not available")
        await enforce_dataset_access(entry=ds, user=user)

        if ds.backend_config is None:
            logger.warning(f"Table {ref_table} has no backend_config table mapping")
            continue

        phy_table_name = ds.backend_config.get("table", None)
        if phy_table_name is None:
            logger.warning(
                f"Table {ref_table} has no backend_config.table value configured"
            )
            continue

        logger.debug(f"Mapped SQL table {ref_table} -> {phy_table_name}")
        tables_map[ref_table] = phy_table_name

    # Replace tables ID with physical tables
    complete_sql = parsed.to_sql(tables_map=tables_map)
    logger.debug(f"Complete SQL: {complete_sql}")

    # ------------------------------------------------------------------
    # Pagination & caps
    # ------------------------------------------------------------------
    limit = _clamp_limit(limit)
    offset = max(offset, 0)

    paginated_sql = f"""
        SELECT *
        FROM (
            {complete_sql}
        ) AS q
        LIMIT :limit OFFSET :offset
    """

    count_sql = f"""
        SELECT COUNT(*) FROM (
            {complete_sql}
        ) AS q
    """

    # ------------------------------------------------------------------
    # Execute count
    # ------------------------------------------------------------------
    try:
        total = await execute_scalar_with_timeout(
            db,
            count_sql,
        )
    except HTTPException:
        raise
    except Exception as exc:  # safety net
        logger.exception("Count query failed")
        raise HTTPException(500, "Query failed") from None

    # ------------------------------------------------------------------
    # Execute data query
    # ------------------------------------------------------------------
    try:
        rows = await execute_rows_with_timeout(
            db,
            paginated_sql,
            {"limit": limit, "offset": offset},
        )
    except HTTPException as e:
        logger.error(f"Query execution failed: {e}")
        raise
    except Exception as exc:  # safety net
        logger.error("Query execution failed: {e}")
        raise HTTPException(500, "Query execution failed") from None

    # ------------------------------------------------------------------
    # Post-process rows (geometry → GeoJSON)
    # ------------------------------------------------------------------
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

    logger.debug(f"SQL items={len(items)} total={total} offset={offset} limit={limit}")

    return DatasetQueryResult(
        items=items,
        offset=offset,
        limit=limit,
        count=len(items),
        total=total,
    )
