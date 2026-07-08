# dataset/main.py
from __future__ import annotations

import logging
from contextlib import asynccontextmanager
import os
from pathlib import Path
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from celine.dataset.core.config import Settings, configure, get_settings
from celine.dataset.api.healthcheck import is_healthly
from celine.dataset.core.logging import setup_logging
from celine.dataset.routes import register_routes
from celine.dataset.core.owners import OwnersRegistry, load_owners_yaml

setup_logging()
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan manager.
    """
    s = get_settings()
    logger.info("Starting %s (%s mode)", s.app_name, s.env)

    failed = await is_healthly()

    if failed:
        raise RuntimeError("System failed health check at startup")

    try:
        app.state.owners = load_owners_yaml(s.owners_yaml_path)
        logger.info(
            "Loaded %d owner(s) from %s", len(app.state.owners), s.owners_yaml_path
        )
    except FileNotFoundError:
        logger.warning(
            "owners.yaml not found at %s — publisher enrichment disabled",
            s.owners_yaml_path,
        )
        app.state.owners = None
    except Exception as exc:
        logger.warning("Could not load owners registry: %s — continuing without it", exc)
        app.state.owners = None

    yield

    logger.info("Shutting down %s", s.app_name)


def create_app(
    *,
    use_lifespan: bool = True,
    extra_routers: list | None = None,
    settings_override: Settings | None = None,
    lifespan_override=None,
) -> FastAPI:
    if settings_override is not None:
        configure(settings_override)

    active_lifespan = lifespan_override if lifespan_override is not None else lifespan

    app = FastAPI(
        title=get_settings().app_name,
        version="0.1.0",
        docs_url="/docs",
        redoc_url="/redoc",
        lifespan=active_lifespan if use_lifespan else None,
    )

    register_routes(app, extra_routers=extra_routers)

    return app


if __name__ == "__main__":
    app = create_app()
