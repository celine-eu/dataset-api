# dataset/routes/dataset.py
from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, Header
from sqlalchemy.ext.asyncio import AsyncSession

from celine.dataset.core.config import get_settings
from celine.dataset.db.engine import get_session, get_datasets_session
from celine.dataset.schemas.dataset_query import DatasetQueryModel, DatasetQueryResult
from celine.dataset.security.auth import get_optional_user
from celine.dataset.api.dataset_query.executor import execute_query
from celine.dataset.security.edr import EDRRequestContext, verify_edr_consumer
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
    authorization: Optional[str] = Header(default=None),
    edc_contract_agreement_id: Optional[str] = Header(default=None),
    edc_transfer_process_id: Optional[str] = Header(default=None),
    edc_purpose: Optional[str] = Header(default=None),
    edc_bpn: Optional[str] = Header(default=None),
):
    # `Edc-Contract-Agreement-Id` selects dataspace mode. Its absence is the
    # ordinary path — user auth and policies — which this change leaves alone.
    #
    # Dataspace mode never falls back to that path on failure: a fallback
    # between two authorization regimes is a bypass with extra steps.
    edr_context: Optional[EDRRequestContext] = None
    if get_settings().edr_enabled and edc_contract_agreement_id:
        edr_context = EDRRequestContext(
            agreement_id=edc_contract_agreement_id,
            # From the **verified** token, never from `Edc-Bpn`: the consumer's
            # identity is what ds checks the agreement against, so a header
            # would let a caller name someone else's contract.
            consumer_id=await verify_edr_consumer(authorization),
            transfer_id=edc_transfer_process_id,
            purpose=[p.strip() for p in (edc_purpose or "").split(",") if p.strip()],
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
