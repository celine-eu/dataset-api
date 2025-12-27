from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from typing import List

from fastapi import APIRouter, Request, Depends, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from celine.dataset.db.engine import get_session
from celine.dataset.db.models.dataset_entry import DatasetEntry
from celine.dataset.core.datasets import load_dataset_entry

# ------------------------------------------------------------------------------
# Router & templates
# ------------------------------------------------------------------------------

router = APIRouter(include_in_schema=False)

templates = Jinja2Templates(directory=Path(__file__).parent.parent / "templates")

# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------


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
        dataset_name = ".".join(parts[:-1]) if len(parts) > 1 else ds.dataset_id
        ds.title = ds.title or ds.dataset_id
        grouped[dataset_name].append(ds)

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "datasets": grouped,
        },
    )


@router.get("/dataset/{dataset_id}", response_class=HTMLResponse)
async def dataset_view(
    dataset_id: str,
    request: Request,
    db: AsyncSession = Depends(get_session),
):
    try:
        dataset = await load_dataset_entry(db=db, dataset_id=dataset_id)
    except KeyError:
        raise HTTPException(status_code=404, detail="Dataset not found")

    # Example SQL
    sql = f"SELECT * FROM {dataset_id} LIMIT 100"

    # Pagination example
    limit = 100
    offset = 0

    # Base URL (proxy-safe)
    base_url = str(request.base_url).rstrip("/")
    root_path = request.scope.get("root_path", "")
    api_url = f"{base_url}{root_path}/dataset/{dataset_id}/query"

    # Auth header if dataset is not public
    needs_auth = dataset.access_level != "public"

    auth_header = '-H "Authorization: Bearer <YOUR_TOKEN>" \\\n' if needs_auth else ""

    example_curl = (
        "curl -X POST \\\n"
        f"  '{api_url}' \\\n"
        f"{auth_header}"
        '  -H "Content-Type: application/json" \\\n'
        "  -d '{\n"
        f'        "sql": "{sql}",\n'
        f'        "limit": {limit},\n'
        f'        "offset": {offset}\n'
        "      }'"
    )

    return templates.TemplateResponse(
        "dataset.html",
        {
            "request": request,
            "dataset": dataset,
            "example_queries": [sql],
            "example_curl": example_curl,
            "needs_auth": needs_auth,
        },
    )
