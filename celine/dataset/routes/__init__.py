from importlib import import_module
from pathlib import Path

from fastapi.staticfiles import StaticFiles
from celine.dataset.core.logging import logging
from celine.dataset.routes.views import router as views_router
from fastapi import FastAPI


logger = logging.getLogger(__name__)


def register_routes(app: FastAPI):

    # register views router before APIs
    static_path = (Path(__file__).resolve().parent.parent / "static").absolute()
    assert static_path.exists()

    logger.debug(f"Static path {static_path}")
    app.mount(
        "/static",
        StaticFiles(directory=static_path),
        name="static",
    )
    app.include_router(views_router)

    routes = []

    # Discover all modules in this directory
    pkg_path = Path(__file__).parent

    for file in pkg_path.glob("*.py"):
        if (
            file.name.startswith("_")
            or file.name == "__init__.py"
            or file.name == "views.py"
        ):
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
