# datasets/dcat/openlineage_client.py
from __future__ import annotations

import logging
from typing import Any, Optional

import httpx

from dataset.core.config import settings

logger = logging.getLogger(__name__)


async def fetch_marquez_dataset(dataset_name: str) -> Optional[dict[str, Any]]:
    if not settings.marquez_url or not settings.marquez_namespace:
        logger.debug("Marquez not configured; skipping OpenLineage metadata")
        return None

    base_url = str(settings.marquez_url).rstrip("/")
    ns = settings.marquez_namespace
    url = f"{base_url}/api/v1/namespaces/{ns}/datasets/{dataset_name}"

    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(url)
            if resp.status_code == 404:
                logger.info(
                    "Dataset %s not found in Marquez namespace=%s", dataset_name, ns
                )
                return None
            resp.raise_for_status()
            return resp.json()
    except Exception as exc:
        logger.warning("Failed Marquez request %s: %s", url, exc)
        return None
