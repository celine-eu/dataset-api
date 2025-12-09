# dataset/core/jsonld.py
from __future__ import annotations

from typing import Any, Iterable

from dataset.db.models.dataset_entry import DatasetEntry
from dataset.core.config import settings
from dataset.core.utils import get_dataset_uri


def rows_to_jsonld(
    entry: DatasetEntry,
    rows: Iterable[dict[str, Any]],
    *,
    limit: int,
    offset: int,
    total: int | None = None,
) -> dict[str, Any]:
    """
    VERY simple stub JSON-LD mapper.
    """
    dataset_uri = get_dataset_uri(entry.dataset_id)
    collection_uri = f"{dataset_uri}/query"

    items = list(rows)

    doc: dict[str, Any] = {
        "@context": {
            "@vocab": "https://schema.org/",
            "dcat": "http://www.w3.org/ns/dcat#",
        },
        "@id": collection_uri,
        "@type": "dcat:Distribution",
        "dcat:accessService": dataset_uri,
        "dcat:endpointURL": f"{settings.api_base_url}/dataset/{entry.dataset_id}/query",
        "dcat:keyword": (entry.tags or {}).get("keywords") or [],
        "items": items,
        "limit": limit,
        "offset": offset,
    }

    if total is not None:
        doc["total"] = total

    return doc
