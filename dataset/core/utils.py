# dataset_api/core/utils.py
from __future__ import annotations

from dataset.core.config import settings


def url_str(url) -> str:
    return "" if url is None else str(url)


def get_dataset_uri(dataset_id: str) -> str:
    base = str(settings.dataset_base_uri).rstrip("/")
    return f"{base}/{dataset_id}"
