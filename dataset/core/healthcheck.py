# dataset/api/dataset.py
from __future__ import annotations

from dataset.catalogue.db import get_engine
from sqlalchemy import text

from dataset.core.logging import logging

logger = logging.getLogger(__name__)


async def is_healthly():
    failed = False
    try:
        engine = get_engine()
        async with engine.begin() as conn:
            await conn.execute(text("SELECT 1"))
        logger.info("Database connectivity: OK")
    except Exception as e:
        logger.error("Database connectivity failed: %s", e)
        failed = True

    return failed
