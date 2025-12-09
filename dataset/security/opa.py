# dataset/security/opa.py
from __future__ import annotations

import logging
from typing import Any, Optional

import httpx

from dataset.catalogue.models import DatasetEntry
from dataset.core.config import settings

logger = logging.getLogger(__name__)


async def authorize_dataset_query(
    *,
    entry: DatasetEntry,
    user: Optional[dict[str, Any]],
    raw_filter: Optional[str],
) -> bool:
    """
    Call OPA to decide if the given user may run this query on this dataset.

    Row-level policies can be implemented in OPA; here we only care about
    an allow/deny boolean.
    """
    opa_url: str | None = str(settings.opa_url) if settings.opa_url else None

    if not opa_url:
        # If you prefer "fail closed", flip this to False and adjust error handling.
        logger.debug("OPA URL not configured, skipping external policy check.")
        return True

    policy_path = settings.opa_dataset_policy_path.strip("/")
    url = f"{opa_url.rstrip('/')}/v1/data/{policy_path}"

    input_doc = {
        "dataset": {
            "id": entry.dataset_id,
            "backend_type": entry.backend_type,
            "access_level": entry.access_level or "open",
            "tags": entry.tags or {},
        },
        "user": user or {},
        "query": {
            "filter": raw_filter,
        },
    }

    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.post(url, json={"input": input_doc})
            resp.raise_for_status()
            data = resp.json()
    except Exception as exc:
        logger.error("OPA evaluation failed: %s", exc)
        # Fail closed by default for non-open data; caller can decide based on dataset.
        return False

    # Convention: OPA returns {"result": {"allow": true/false}}
    allowed = bool(data.get("result", {}).get("allow", False))
    return allowed
