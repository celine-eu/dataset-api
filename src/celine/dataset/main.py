# dataset/main.py
from __future__ import annotations

import logging
from contextlib import asynccontextmanager
import os
from pathlib import Path
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from celine.dataset.core.config import settings
from celine.dataset.api.healthcheck import is_healthly
from celine.dataset.core.logging import setup_logging
from celine.dataset.routes import register_routes
from celine.utils.pipelines.owners import OwnersRegistry, load_owners_yaml

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

    try:
        app.state.owners = load_owners_yaml(settings.owners_yaml_path)
        logger.info(
            "Loaded %d owner(s) from %s", len(app.state.owners), settings.owners_yaml_path
        )
    except FileNotFoundError:
        logger.warning(
            "owners.yaml not found at %s — publisher enrichment disabled",
            settings.owners_yaml_path,
        )
        app.state.owners = None
    except Exception as exc:
        logger.warning("Could not load owners registry: %s — continuing without it", exc)
        app.state.owners = None

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

    register_routes(app)

    return app


if __name__ == "__main__":
    app = create_app()
