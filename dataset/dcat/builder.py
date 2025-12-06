# dataset_api/dcat/builder.py
from __future__ import annotations

import datetime as dt
import logging
from typing import Any, Iterable, Optional

from dataset.catalogue.models import DatasetEntry
from dataset.core.config import settings
from dataset.dcat.openlineage_client import fetch_marquez_dataset

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
}


def _iso_date(value: Optional[str | dt.datetime]) -> Optional[str]:
    """Normalize dates to ISO 8601 date strings."""
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        return value.date().isoformat()
    # string from Marquez like "2019-05-09T19:49:24.201361Z"
    try:
        return (
            dt.datetime.fromisoformat(value.replace("Z", "+00:00")).date().isoformat()
        )
    except Exception:  # best-effort
        logger.debug("Could not parse date %s; returning raw", value)
        return value


def build_catalog(datasets: Iterable[DatasetEntry]) -> dict[str, Any]:
    """Build DCAT-AP catalog JSON-LD for /catalogue.

    For performance, we *do not* call Marquez here; this is a lightweight catalog listing.
    Detailed lineage & provenance is provided by /dataset/{id}/metadata via build_dataset().
    """
    catalog_uri = str(settings.catalog_uri)
    dataset_nodes: list[dict[str, Any]] = []

    for d in datasets:
        dataset_uri = f"{settings.dataset_base_uri}{d.dataset_id}"

        node: dict[str, Any] = {
            "@id": dataset_uri,
            "@type": "dcat:Dataset",
            "dct:title": d.title,
        }

        if d.description:
            node["dct:description"] = d.description

        keywords = (d.tags or {}).get("keywords")
        if isinstance(keywords, list) and keywords:
            node["dcat:keyword"] = keywords

        # Minimal distribution pointing back to API
        node["dcat:distribution"] = [
            {
                "@id": f"{dataset_uri}#distribution-api",
                "@type": "dcat:Distribution",
                "dct:title": "API access (JSON)",
                "dcat:accessURL": f"{settings.api_base_url}/dataset/{d.dataset_id}/query",
                "dct:format": "application/json",
            }
        ]

        dataset_nodes.append(node)

    catalog: dict[str, Any] = {
        "@context": DCAT_CONTEXT,
        "@id": catalog_uri,
        "@type": "dcat:Catalog",
        "dct:title": settings.app_name,
        "dcat:dataset": dataset_nodes,
    }

    return catalog


async def build_dataset(entry: DatasetEntry) -> dict[str, Any]:
    """Build a DCAT-AP JSON-LD node for a single dataset, with lineage if possible.

    This is used for /dataset/{dataset_id}/metadata.
    """
    dataset_uri = f"{settings.dataset_base_uri}{entry.dataset_id}"

    dcat_dataset: dict[str, Any] = {
        "@context": DCAT_CONTEXT,
        "@id": dataset_uri,
        "@type": "dcat:Dataset",
        "dct:title": entry.title,
    }

    if entry.description:
        dcat_dataset["dct:description"] = entry.description

    # Keywords / theme
    tags = entry.tags or {}
    keywords = tags.get("keywords")
    if isinstance(keywords, list) and keywords:
        dcat_dataset["dcat:keyword"] = keywords

    themes = tags.get("themes")
    if isinstance(themes, list) and themes:
        # Should be URIs to SKOS concepts
        dcat_dataset["dcat:theme"] = themes

    # Publisher, rights holder, license (URIs)
    if entry.publisher_uri:
        dcat_dataset["dct:publisher"] = {"@id": entry.publisher_uri}

    if entry.rights_holder_uri:
        dcat_dataset["dct:rightsHolder"] = {"@id": entry.rights_holder_uri}

    if entry.license_uri:
        dcat_dataset["dct:license"] = {"@id": entry.license_uri}

    # Optional landing page
    if entry.landing_page:
        dcat_dataset["dcat:landingPage"] = {"@id": entry.landing_page}

    # Languages
    if entry.language_uris:
        dcat_dataset["dct:language"] = [{"@id": u} for u in entry.language_uris]

    # Spatial coverage
    if entry.spatial_uris:
        dcat_dataset["dct:spatial"] = [{"@id": u} for u in entry.spatial_uris]

    # Distributions
    dcat_dataset["dcat:distribution"] = _build_distributions(entry, dataset_uri)

    # Enrich with OpenLineage / Marquez (best-effort)
    mq = await fetch_marquez_dataset(entry.dataset_id)
    if mq:
        _enrich_with_marquez(dcat_dataset, mq)

    return dcat_dataset


def _build_distributions(entry: DatasetEntry, dataset_uri: str) -> list[dict[str, Any]]:
    """Build dcat:Distribution objects for a dataset."""

    distributions: list[dict[str, Any]] = []

    # API distribution
    api_dist = {
        "@id": f"{dataset_uri}#distribution-api",
        "@type": "dcat:Distribution",
        "dct:title": "API access (JSON)",
        "dcat:accessURL": f"{settings.api_base_url}/dataset/{entry.dataset_id}/query",
        "dct:format": "application/json",
    }
    distributions.append(api_dist)

    # Optionally expose a landing page as distribution (e.g. HTML)
    if entry.landing_page:
        distributions.append(
            {
                "@id": f"{dataset_uri}#distribution-landing",
                "@type": "dcat:Distribution",
                "dct:title": "Landing page",
                "dcat:accessURL": entry.landing_page,
                "dct:format": "text/html",
            }
        )

    # You can add more distributions based on backend type / config (e.g. file download).
    # Example for S3/FS:
    if entry.backend_type in ("s3", "fs"):
        # This is intentionally generic; you can specialize based on backend_config.
        file_dist = {
            "@id": f"{dataset_uri}#distribution-file",
            "@type": "dcat:Distribution",
            "dct:title": "Raw file access",
            "dct:description": "Direct access to raw file(s) backing this dataset.",
            "dct:format": entry.backend_config.get(
                "format", "application/octet-stream"
            ),
        }
        # If you have a public URL or signed URL generator, add dcat:downloadURL / accessURL here.
        raw_url = entry.backend_config.get("public_url")
        if raw_url:
            file_dist["dcat:downloadURL"] = raw_url

        distributions.append(file_dist)

    return distributions


def _enrich_with_marquez(dcat_dataset: dict[str, Any], mq: dict[str, Any]) -> None:
    """Map Marquez dataset metadata into DCAT-AP / PROV terms.

    This is intentionally conservative: it only uses stable fields and does not
    rely on specific facets.
    """
    # Description: prefer catalogue description; if missing, use Marquez
    if not dcat_dataset.get("dct:description") and mq.get("description"):
        dcat_dataset["dct:description"] = mq["description"]

    created = mq.get("createdAt")
    updated = mq.get("lastModifiedAt") or mq.get("updatedAt")

    iso_created = _iso_date(created)
    iso_updated = _iso_date(updated)

    if iso_created:
        dcat_dataset["dct:issued"] = {
            "@type": "xsd:date",
            "@value": iso_created,
        }

    if iso_updated:
        dcat_dataset["dct:modified"] = {
            "@type": "xsd:date",
            "@value": iso_updated,
        }

    # Source as provenance: map Marquez sourceName → prov:wasDerivedFrom
    source_name = mq.get("sourceName")
    if source_name:
        dcat_dataset.setdefault("prov:wasDerivedFrom", []).append(
            {
                "@id": f"{settings.catalog_uri}/source/{source_name}",
                "@type": "prov:Entity",
            }
        )

    # Tags -> keywords (merge)
    mq_tags = mq.get("tags") or []
    if mq_tags:
        existing_keywords = set(dcat_dataset.get("dcat:keyword", []))
        all_keywords = sorted(existing_keywords.union(mq_tags))
        dcat_dataset["dcat:keyword"] = all_keywords

    # Fields could be mapped to adms:Identifier / schema org / etc; for now, we leave
    # them out of the top-level dataset to keep responses small. You could expose
    # them under a non-standard key like "ml:fields" if needed.
