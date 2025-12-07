# dataset/main.py
from __future__ import annotations

import logging
from fastapi import FastAPI
from contextlib import asynccontextmanager
from sqlalchemy import text

from dataset.core.logging import setup_logging
from dataset.core.config import settings
from dataset.api import catalogue, dataset, admin
from dataset.catalogue.db import get_engine

setup_logging()
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Production-grade lifespan manager.

    - Run checks before accepting traffic
    - Initialize global state (cache, clients)
    - Clean shutdown
    """
    logger.info("▶️ Starting %s (%s mode)", settings.app_name, settings.env)

    # --- Optional: Database connectivity check ---

    failed = False
    try:
        engine = get_engine()
        async with engine.begin() as conn:
            await conn.execute(text("SELECT 1"))
        logger.info("Database connectivity: OK")
    except Exception as e:
        logger.error("Database connectivity failed: %s", e)
        failed = True

    # --- Yield control to allow app serving ---
    if not failed:
        yield

    # --- Shutdown tasks ---
    logger.info("⏹ Shutting down %s", settings.app_name)


def create_app(use_lifespan: bool = True) -> FastAPI:

    app = FastAPI(
        title=settings.app_name,
        version="0.1.0",
        docs_url="/docs",
        redoc_url="/redoc",
        lifespan=lifespan if use_lifespan else None,
    )

    app.include_router(catalogue.router, prefix="/catalogue", tags=["catalogue"])
    app.include_router(dataset.router, prefix="/dataset", tags=["dataset"])
    app.include_router(admin.router, prefix="/admin", tags=["admin"])

    return app


if __name__ == "__main__":
    app = create_app()
