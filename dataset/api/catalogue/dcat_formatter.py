# dataset/catalogue/dcat_formatter.py
from __future__ import annotations

import datetime as dt
import logging
from typing import Any, Iterable, Optional

from dataset.db.models import DatasetEntry
from dataset.core.config import settings
from dataset.core.utils import get_dataset_uri

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
        logger.debug("Could not parse date %s; returning raw", value)
        return value


def build_catalog(datasets: Iterable[DatasetEntry]) -> dict[str, Any]:
    """Build DCAT-AP Catalog JSON-LD for /catalogue."""
    catalog_uri = str(settings.catalog_uri)
    dataset_nodes: list[dict[str, Any]] = []

    for d in datasets:
        dataset_uri = get_dataset_uri(d.dataset_id)

        node: dict[str, Any] = {}
        node["@id"] = dataset_uri
        node["@type"] = "dcat:Dataset"
        node["dct:title"] = d.title

        if d.description:
            node["dct:description"] = d.description

        tags = d.tags or {}

        keywords = tags.get("keywords")
        if keywords:
            node["dcat:keyword"] = keywords

        themes = tags.get("themes")
        if themes:
            node["dcat:theme"] = themes

        identifier = tags.get("identifier")
        if identifier:
            node["dct:identifier"] = identifier
        else:
            node["dct:identifier"] = d.dataset_id

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
    """Build a DCAT-AP JSON-LD node for a single Dataset."""
    dataset_uri = get_dataset_uri(entry.dataset_id)

    tags = entry.tags or {}
    lineage = entry.lineage or {}

    dcat_dataset: dict[str, Any] = {}
    dcat_dataset["@context"] = DCAT_CONTEXT
    dcat_dataset["@id"] = dataset_uri
    dcat_dataset["@type"] = "dcat:Dataset"

    dcat_dataset["dct:title"] = entry.title

    if entry.description:
        dcat_dataset["dct:description"] = entry.description

    identifier = tags.get("identifier") or entry.dataset_id
    dcat_dataset["dct:identifier"] = identifier

    if tags.get("keywords"):
        dcat_dataset["dcat:keyword"] = tags["keywords"]
    if tags.get("themes"):
        dcat_dataset["dcat:theme"] = tags["themes"]

    if entry.publisher_uri:
        dcat_dataset["dct:publisher"] = {"@id": entry.publisher_uri}
    if entry.rights_holder_uri:
        dcat_dataset["dct:rightsHolder"] = {"@id": entry.rights_holder_uri}
    if entry.license_uri:
        dcat_dataset["dct:license"] = {"@id": entry.license_uri}

    access_rights = tags.get("accessRights")
    if access_rights:
        dcat_dataset["dct:accessRights"] = {"@id": access_rights}

    if entry.landing_page:
        dcat_dataset["dcat:landingPage"] = {"@id": entry.landing_page}

    if entry.language_uris:
        dcat_dataset["dct:language"] = [{"@id": u} for u in entry.language_uris]

    if entry.spatial_uris:
        dcat_dataset["dct:spatial"] = [{"@id": u} for u in entry.spatial_uris]

    temporal = tags.get("temporal") or {}
    start = temporal.get("start")
    end = temporal.get("end")
    if start or end:
        temporal_node: dict[str, Any] = {"@type": "dct:PeriodOfTime"}
        if start:
            temporal_node["dcat:startDate"] = start
        if end:
            temporal_node["dcat:endDate"] = end
        dcat_dataset["dct:temporal"] = temporal_node

    accrual = tags.get("accrualPeriodicity")
    if accrual:
        dcat_dataset["dct:accrualPeriodicity"] = {"@id": accrual}

    conforms_to = tags.get("conformsTo")
    if conforms_to:
        dcat_dataset["dct:conformsTo"] = {"@id": conforms_to}

    contact = tags.get("contactPoint") or {}
    if contact:
        contact_node: dict[str, Any] = {"@type": "vcard:Individual"}
        fn = contact.get("fn")
        email = contact.get("email")
        if fn:
            contact_node["vcard:fn"] = fn
        if email:
            contact_node["vcard:hasEmail"] = email
        dcat_dataset["dcat:contactPoint"] = contact_node

    dcat_dataset["dcat:distribution"] = _build_distributions(entry, dataset_uri)

    if lineage:
        _apply_stored_lineage(dcat_dataset, lineage)

    return dcat_dataset


def _apply_stored_lineage(
    dcat_dataset: dict[str, Any], lineage: dict[str, Any]
) -> None:
    created = lineage.get("createdAt")
    updated = lineage.get("updatedAt")
    source = lineage.get("sourceName") or lineage.get("namespace")
    tags = lineage.get("tags") or []
    facets = lineage.get("facets") or {}

    iso_created = _iso_date(created)
    iso_updated = _iso_date(updated)

    if iso_created:
        dcat_dataset["dct:issued"] = {"@type": "xsd:date", "@value": iso_created}
    if iso_updated:
        dcat_dataset["dct:modified"] = {"@type": "xsd:date", "@value": iso_updated}

    if source:
        dcat_dataset.setdefault("prov:wasDerivedFrom", []).append(
            {
                "@id": f"{settings.catalog_uri}/source/{source}",
                "@type": "prov:Entity",
            }
        )

    if tags:
        existing = set(dcat_dataset.get("dcat:keyword", []))
        dcat_dataset["dcat:keyword"] = sorted(existing.union(tags))

    schema_facet = facets.get("schema") or {}
    schema_url = schema_facet.get("_schemaURL")
    if schema_url and "dct:conformsTo" not in dcat_dataset:
        dcat_dataset["dct:conformsTo"] = {"@id": schema_url}

    doc_facet = facets.get("documentation") or {}
    doc_desc = doc_facet.get("description")
    if doc_desc and not dcat_dataset.get("dct:description"):
        dcat_dataset["dct:description"] = doc_desc

    ownership = facets.get("ownership") or {}
    owners = ownership.get("owners") or []
    if owners and "dct:publisher" not in dcat_dataset:
        owner = owners[0]
        name = owner.get("name")
        if name:
            dcat_dataset["dct:publisher"] = {
                "@id": f"{settings.catalog_uri}/agent/{name}"
            }

    version = facets.get("version") or {}
    ver = version.get("version")
    if ver:
        dcat_dataset["adms:version"] = ver


def _build_distributions(entry: DatasetEntry, dataset_uri: str) -> list[dict[str, Any]]:
    distributions: list[dict[str, Any]] = []

    api_dist: dict[str, Any] = {
        "@id": f"{dataset_uri}#distribution-api",
        "@type": "dcat:Distribution",
        "dct:title": "API access (JSON)",
        "dcat:accessURL": f"{settings.api_base_url}/dataset/{entry.dataset_id}/query",
        "dct:format": "application/json",
        "dcat:mediaType": "application/json",
    }

    if entry.license_uri:
        api_dist["dct:license"] = {"@id": entry.license_uri}

    distributions.append(api_dist)

    if entry.landing_page:
        landing_dist: dict[str, Any] = {
            "@id": f"{dataset_uri}#distribution-landing",
            "@type": "dcat:Distribution",
            "dct:title": "Landing page",
            "dcat:accessURL": entry.landing_page,
            "dct:format": "text/html",
            "dcat:mediaType": "text/html",
        }
        distributions.append(landing_dist)

    if entry.backend_type in ("s3", "fs"):
        file_dist: dict[str, Any] = {
            "@id": f"{dataset_uri}#distribution-file",
            "@type": "dcat:Distribution",
            "dct:title": "Raw file",
        }

        if entry.backend_config:
            fmt = entry.backend_config.get("format", "application/octet-stream")
            file_dist["dct:format"] = fmt
            file_dist["dcat:mediaType"] = fmt

            raw_url = entry.backend_config.get("public_url")
            if raw_url:
                file_dist["dcat:downloadURL"] = raw_url

            size_bytes = entry.backend_config.get("size_bytes")
            if isinstance(size_bytes, int):
                file_dist["dcat:byteSize"] = size_bytes

        distributions.append(file_dist)

    return distributions
