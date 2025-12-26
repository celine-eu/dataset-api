import logging
import sys

from celine.dataset.core.config import settings


def setup_logging() -> None:
    """
    Configure logging with:
    - root logger = INFO
    - application logs (celine.*) = LOG_LEVEL
    - noisy libraries reduced
    """

    app_level = getattr(logging, settings.log_level.upper(), logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )
    handler.setFormatter(formatter)

    # ------------------------------------------------------------------
    # Root logger: safe default
    # ------------------------------------------------------------------
    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(logging.INFO)
    root.addHandler(handler)

    # ------------------------------------------------------------------
    # Application logs
    # ------------------------------------------------------------------
    logging.getLogger("celine").setLevel(app_level)

    # ------------------------------------------------------------------
    # Framework / server logs
    # ------------------------------------------------------------------
    logging.getLogger("uvicorn").setLevel(logging.INFO)
    logging.getLogger("uvicorn.error").setLevel(logging.INFO)
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)

    # ------------------------------------------------------------------
    # Common noisy libraries (tune as needed)
    # ------------------------------------------------------------------
    logging.getLogger("sqlalchemy").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)
