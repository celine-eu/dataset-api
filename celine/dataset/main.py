# dataset/main.py
from __future__ import annotations

import logging
from contextlib import asynccontextmanager
import os
from fastapi import FastAPI

from celine.dataset.core.config import settings
from celine.dataset.api.healthcheck import is_healthly
from celine.dataset.core.logging import setup_logging
from celine.dataset.routes import register_routes

setup_logging()
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan manager.
    """
    logger.info("Starting %s (%s mode)", settings.app_name, settings.env)

    failed = await is_healthly()

    if failed:
        raise RuntimeError("System failed health check at startup")

    yield

    logger.info("Shutting down %s", settings.app_name)


def create_app(use_lifespan: bool = True) -> FastAPI:

    if os.getenv("DEBUG_ATTACH") == "1":
        import debugpy

        debugpy.listen(("0.0.0.0", 5678))
        print("Debugger listening on 0.0.0.0:5678")

    app = FastAPI(
        title=settings.app_name,
        version="0.1.0",
        docs_url="/docs",
        redoc_url="/redoc",
        lifespan=lifespan if use_lifespan else None,
    )

    register_routes(app)

    return app


if __name__ == "__main__":
    app = create_app()
