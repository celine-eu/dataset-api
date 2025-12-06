# dataset_api/main.py
from __future__ import annotations

import logging
from fastapi import FastAPI
from contextlib import asynccontextmanager
from sqlalchemy import text

from dataset.core.logging import setup_logging
from dataset.core.config import settings
from dataset.catalogue.db import engine
from dataset.api import catalogue, dataset

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
    try:
        async with engine.begin() as conn:
            await conn.execute(text("SELECT 1"))
        logger.info("Database connectivity: OK")
    except Exception as e:
        logger.error("Database connectivity failed: %s", e)
        raise

    # --- Optional: external service warmup (e.g. Marquez) ---
    if settings.marquez_url:
        logger.info("Configured Marquez endpoint: %s", settings.marquez_url)

    # --- Yield control to allow app serving ---
    yield

    # --- Shutdown tasks ---
    logger.info("⏹ Shutting down %s", settings.app_name)


app = FastAPI(
    title=settings.app_name,
    version="0.1.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

# Routers
app.include_router(catalogue.router, prefix="/catalogue", tags=["catalogue"])
app.include_router(dataset.router, prefix="/dataset", tags=["dataset"])
