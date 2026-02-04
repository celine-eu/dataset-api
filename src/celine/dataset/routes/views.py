from __future__ import annotations

from collections import defaultdict
import logging
from pathlib import Path
from typing import Any, List, Dict

from fastapi import APIRouter, Request, Depends, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from celine.dataset.db.engine import get_session
from celine.dataset.db.models.dataset_entry import DatasetEntry
from celine.dataset.core.datasets import load_dataset_entry
from celine.dataset.db.reflection import reflect_table_async

# ------------------------------------------------------------------------------
# Router & templates
# ------------------------------------------------------------------------------

logger = logging.getLogger(__name__)

router = APIRouter(include_in_schema=False)

templates = Jinja2Templates(directory=Path(__file__).parent.parent / "templates")

# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------


def _format_column_default(column) -> str | None:
    """
    Return a human-readable default value for a SQLAlchemy column.
    Safe for static typing and runtime.
    """
    if column.default is not None:
        return str(column.default)

    if column.server_default is not None:
        return str(column.server_default)

    return None


async def get_dataset_metadata(
    *,
    db: AsyncSession,
    entry: DatasetEntry,
) -> List[Dict[str, Any]]:
    """
    Return column-level metadata for a dataset backend.

    Output is template-friendly and backend-agnostic.
    """

    if entry.backend_type != "postgres":
        return []

    backend_table = entry.backend_config.get("table") if entry.backend_config else None

    if not backend_table:
        return []

    try:
        table = await reflect_table_async(db, backend_table)
    except Exception as exc:
        logger.exception(
            "Failed to reflect table %s for dataset %s",
            backend_table,
            entry.dataset_id,
        )
        raise HTTPException(
            status_code=500,
            detail="Failed to reflect dataset metadata",
        )

    metadata: list[dict[str, Any]] = []

    for column in table.columns:
        metadata.append(
            {
                "name": column.name,
                "type": str(column.type),
                "nullable": column.nullable,
                "primary_key": column.primary_key,
                "default": _format_column_default(column),
            }
        )

    return metadata


async def list_dataset_entries(db: AsyncSession) -> List[DatasetEntry]:
    """
    List all dataset entries for the catalogue HTML view.
    """
    result = await db.execute(select(DatasetEntry).order_by(DatasetEntry.dataset_id))
    return list(result.scalars().all())


# ------------------------------------------------------------------------------
# Routes
# ------------------------------------------------------------------------------


@router.get("/", response_class=HTMLResponse)
async def catalogue_view(
    request: Request,
    db: AsyncSession = Depends(get_session),
):
    datasets = await list_dataset_entries(db)

    grouped: dict[str, list[DatasetEntry]] = defaultdict(list)

    for ds in datasets:
        # dataset = everything except last segment
        parts = ds.dataset_id.split(".")

        ns: str | None = None
        if ds.lineage:
            ns = str(ds.lineage["namespace"])

        dataset_name = ns or ".".join(parts[:-1]) if len(parts) > 1 else ds.dataset_id
        ds.title = ds.title or ds.dataset_id
        grouped[dataset_name].append(ds)

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "datasets": grouped,
        },
    )


@router.get("/catalogue/{dataset_id}", response_class=HTMLResponse)
async def dataset_view(
    dataset_id: str,
    request: Request,
    db: AsyncSession = Depends(get_session),
):
    dataset = await load_dataset_entry(db=db, dataset_id=dataset_id)

    dataset.title = dataset.title or dataset.dataset_id

    if dataset.description == dataset.dataset_id:
        dataset.description = ""

    base_url = str(request.base_url).rstrip("/")
    root_path = request.scope.get("root_path", "")
    query_url = f"{base_url}{root_path}/query"

    sql_example = f"SELECT * FROM {dataset.dataset_id}"

    needs_auth = dataset.access_level != "open"

    auth_header = '-H "Authorization: Bearer <YOUR_TOKEN>" \\\n' if needs_auth else ""

    example_curl = (
        "curl -X POST \\\n"
        f"  '{query_url}' \\\n"
        f"{auth_header}"
        '  -H "Content-Type: application/json" \\\n'
        "  -d '{\n"
        f'        "sql": "{sql_example}",\n'
        '        "limit": 100,\n'
        '        "offset": 0\n'
        "      }'"
    )

    metadata = await get_dataset_metadata(
        db=db,
        entry=dataset,
    )

    return templates.TemplateResponse(
        "dataset.html",
        {
            "request": request,
            "dataset": dataset,
            "sql_example": sql_example,
            "example_curl": example_curl,
            "needs_auth": needs_auth,
            "metadata": metadata,
        },
    )
