# dataset/core/healthcheck.py
from __future__ import annotations

from sqlalchemy import text

from celine.dataset.db.engine import get_engine
from celine.dataset.core.logging import logging

logger = logging.getLogger(__name__)


async def is_healthly():
    failed = False
    try:
        engine = get_engine()
        async with engine.begin() as conn:
            await conn.execute(text("SELECT 1"))
        logger.info("Database connectivity: OK")
    except Exception as e:  # pragma: no cover - defensive
        logger.error("Database connectivity failed: %s", e)
        failed = True

    return failed
