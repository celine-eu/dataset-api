from importlib import import_module
from pathlib import Path
from dataset.core.logging import logging
from fastapi import FastAPI

logger = logging.getLogger(__name__)


def register_routes(app: FastAPI):
    routes = []

    # Discover all modules in this directory
    pkg_path = Path(__file__).parent

    for file in pkg_path.glob("*.py"):
        if file.name.startswith("_") or file.name == "__init__.py":
            continue

        module_name = f"{__name__}.{file.stem}"
        module = import_module(module_name)

        route = {}
        # Convention: each route module must expose `router`
        if hasattr(module, "router"):
            route["router"] = module.router
        else:
            logger.warning(
                f"Router {module_name} does not expose 'router' variable, skipping"
            )
            continue

        route["tags"] = []
        if hasattr(module, "tags"):
            route["tags"] = module.tags

        routes.append(route)

    for route in routes:
        app.include_router(route["router"], tags=route["tags"])
