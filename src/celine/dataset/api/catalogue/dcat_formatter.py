# dataset/catalogue/dcat_formatter.py
from __future__ import annotations

import datetime as dt
import logging
from typing import Any, Iterable, Optional

from celine.dataset.db.models.dataset_entry import DatasetEntry
from celine.dataset.core.config import settings
from celine.dataset.core.utils import get_dataset_uri

logger = logging.getLogger(__name__)

DCAT_CONTEXT = {
    "@vocab": "http://www.w3.org/ns/dcat#",
    "dcat": "http://www.w3.org/ns/dcat#",
    "dct": "http://purl.org/dc/terms/",
    "foaf": "http://xmlns.com/foaf/0.1/",
    "prov": "http://www.w3.org/ns/prov#",
    "adms": "http://www.w3.org/ns/adms#",
    "owl": "http://www.w3.org/2002/07/owl#",
    "xsd": "http://www.w3.org/2001/XMLSchema#",
    "vcard": "http://www.w3.org/2006/vcard/ns#",
}

# --------------------------------------------------------
# Helpers
# --------------------------------------------------------


def _iso_date(value: Optional[str | dt.datetime]) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        return value.date().isoformat()
    try:
        return (
            dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            .date()
            .isoformat()
        )
    except Exception:
        return value


def build_catalog(entries: Iterable[DatasetEntry]) -> dict[str, Any]:
    """
    Build DCAT-AP Catalog where:

    - DCAT Dataset = OL namespace
    - DCAT Distribution = each DatasetEntry row within that namespace

    The catalog MUST embed distributions directly.
    """

    catalog_uri = str(settings.catalog_uri)
    entries = list(entries)

    # Group by lineage namespace
    by_ns: dict[str, list[DatasetEntry]] = {}

    for e in entries:
        ns = (e.lineage or {}).get("namespace")
        if not ns:
            raise ValueError(f"DatasetEntry {e.dataset_id} missing namespace")
        by_ns.setdefault(ns, []).append(e)

    dataset_nodes = []

    for namespace, dist_entries in by_ns.items():

        dataset_uri = get_dataset_uri(namespace)

        # Build DCAT Dataset node
        node = {
            "@id": dataset_uri,
            "@type": "dcat:Dataset",
            "dct:title": namespace,
            "dct:identifier": namespace,
            "dcat:distribution": [],
        }

        # Attach each DatasetEntry as a distribution
        for e in dist_entries:
            dist_id = e.dataset_id
            backend = e.backend_config or {}
            media_type = backend.get("format", "application/json")

            dist_node = {
                "@id": f"{dataset_uri}#{dist_id}",
                "@type": "dcat:Distribution",
                "dct:title": e.title or dist_id,
                "dct:identifier": dist_id,
                "dct:format": media_type or "application/json",
                "dcat:mediaType": media_type,
                "dcat:accessURL": get_dataset_uri(dist_id) + "/query",
                "dcat:landingPage": get_dataset_uri(dist_id) + "/metadata",
            }

            # governance fields
            if e.license_uri:
                dist_node["dct:license"] = e.license_uri
            if e.rights_holder_uri:
                dist_node["dct:rightsHolder"] = e.rights_holder_uri
            if e.access_level:
                dist_node["dct:accessRights"] = e.access_level
            if e.tags and e.tags.get("keywords", []):
                dist_node["dcat:keyword"] = e.tags.get("keywords", [])

            # Optional fields
            if backend.get("public_url"):
                dist_node["dcat:downloadURL"] = backend["public_url"]
            if isinstance(backend.get("size_bytes"), int):
                dist_node["dcat:byteSize"] = backend["size_bytes"]

            node["dcat:distribution"].append(dist_node)

        dataset_nodes.append(node)

    # Full catalog
    return {
        "@context": DCAT_CONTEXT,
        "@id": catalog_uri,
        "@type": "dcat:Catalog",
        "dct:title": settings.app_name,
        "dcat:dataset": dataset_nodes,
    }
