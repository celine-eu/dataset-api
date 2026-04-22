# dataset/routes/dataset.py
from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, Header
from sqlalchemy.ext.asyncio import AsyncSession

from celine.dataset.core.config import settings
from celine.dataset.db.engine import get_session, get_datasets_session
from celine.dataset.schemas.dataset_query import DatasetQueryModel, DatasetQueryResult
from celine.dataset.security.auth import get_optional_user
from celine.dataset.api.dataset_query.executor import execute_query
from celine.dataset.security.edr import EDRRequestContext
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
    catalogue_db: AsyncSession = Depends(get_session),
    datasets_db: AsyncSession = Depends(get_datasets_session),
    user: Optional[AuthenticatedUser] = Depends(get_optional_user),
    edc_contract_agreement_id: Optional[str] = Header(default=None),
    edc_bpn: Optional[str] = Header(default=None),
):
    edr_context: Optional[EDRRequestContext] = None
    if settings.edr_enabled and edc_contract_agreement_id:
        edr_context = EDRRequestContext(
            agreement_id=edc_contract_agreement_id,
            consumer_id=edc_bpn or "",
        )

    return await execute_query(
        catalogue_db=catalogue_db,
        datasets_db=datasets_db,
        raw_sql=body.sql,
        limit=body.limit,
        offset=body.offset,
        user=user,
        edr_context=edr_context,
        skip_count=body.skip_count,
    )
