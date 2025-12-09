# dataset/routes/health.py
from __future__ import annotations

from fastapi import APIRouter, HTTPException

from dataset.core.logging import logging
from dataset.core.healthcheck import is_healthly

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/health")
async def healthcheck():
    failed = await is_healthly()
    if failed:
        raise HTTPException(status_code=404, detail="Unhealthly")
    return {"status": "ready"}
