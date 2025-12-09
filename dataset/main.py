# dataset/main.py
from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from dataset.core.config import settings
from dataset.core.healthcheck import is_healthly
from dataset.core.logging import setup_logging
from dataset.routes import admin, catalogue, dataset, health

setup_logging()
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan manager.
    """
    logger.info("Starting %s (%s mode)", settings.app_name, settings.env)

    failed = await is_healthly()

    if not failed:
        yield

    logger.info("Shutting down %s", settings.app_name)


def create_app(use_lifespan: bool = True) -> FastAPI:
    app = FastAPI(
        title=settings.app_name,
        version="0.1.0",
        docs_url="/docs",
        redoc_url="/redoc",
        lifespan=lifespan if use_lifespan else None,
    )

    app.include_router(catalogue.router, tags=["catalogue"])
    app.include_router(dataset.router, prefix="/dataset", tags=["dataset"])
    app.include_router(admin.router, prefix="/admin", tags=["admin"])
    app.include_router(health.router, tags=["health"])

    return app


if __name__ == "__main__":
    app = create_app()
