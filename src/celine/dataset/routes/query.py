# dataset/routes/dataset.py
from __future__ import annotations

from turtle import title
from typing import Optional

from fastapi import APIRouter, Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession

from celine.dataset.db.engine import get_session
from celine.dataset.schemas.dataset_query import DatasetQueryModel, DatasetQueryResult
from celine.dataset.security.auth import get_optional_user
from celine.dataset.api.dataset_query.executor import execute_query
from celine.dataset.security.auth import (
    get_optional_user,
)
from celine.dataset.security.models import AuthenticatedUser


router = APIRouter()
tags = ["catalogue"]


@router.post(
    "/query",
    response_model=DatasetQueryResult,
    description="Query available datasets",
    name="Dataset query",
)
async def query_post(
    body: DatasetQueryModel,
    db: AsyncSession = Depends(get_session),
    user: Optional[AuthenticatedUser] = Depends(get_optional_user),
):
    return await execute_query(
        db=db,
        raw_sql=body.sql,
        limit=body.limit,
        offset=body.offset,
        user=user,
    )
