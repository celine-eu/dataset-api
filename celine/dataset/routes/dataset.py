# dataset/routes/dataset.py
from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from celine.dataset.db.engine import get_session
from celine.dataset.schemas.dataset_query import DatasetQueryModel, DatasetQueryResult
from celine.dataset.security.auth import get_optional_user
from celine.dataset.api.dataset_query.executor import execute_query
from celine.dataset.security.auth import (
    get_optional_user,
)


router = APIRouter()


@router.post("/dataset/{dataset_id}/query", response_model=DatasetQueryResult)
async def query_dataset_post(
    dataset_id: str,
    body: DatasetQueryModel,
    db: AsyncSession = Depends(get_session),
    user: Optional[dict] = Depends(get_optional_user),
):
    return await execute_query(
        db=db,
        dataset_id=dataset_id,
        filter_str=body.filter,
        limit=body.limit,
        offset=body.offset,
        user=user,
    )


@router.get("/dataset/{dataset_id}/query")
async def query_dataset_get(
    dataset_id: str,
    filter: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
    db: AsyncSession = Depends(get_session),
    user: Optional[dict] = Depends(get_optional_user),
):
    body = DatasetQueryModel(filter=filter, limit=limit, offset=offset)
    return await query_dataset_post(dataset_id, body, db, user)
