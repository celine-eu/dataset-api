# dataset/catalogue/dcat_formatter.py
from __future__ import annotations

import datetime as dt
import logging
from typing import Any, Iterable, Optional

from celine.dataset.db.models.dataset_entry import DatasetEntry
from celine.dataset.core.config import settings
from celine.dataset.core.utils import get_dataset_uri
from celine.utils.pipelines.owners import OwnersRegistry

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
    "odrl": "http://www.w3.org/ns/odrl/2/",
    "skos": "http://www.w3.org/2004/02/skos/core#",
    "schema": "https://schema.org/",
    "ds": "https://dataspaces.localhost/ns/energy#",
}

# EU Publications Office access-right authority table
_ACCESS_RIGHTS_URI = {
    "open": "http://publications.europa.eu/resource/authority/access-right/PUBLIC",
    "internal": "http://publications.europa.eu/resource/authority/access-right/RESTRICTED",
    "restricted": "http://publications.europa.eu/resource/authority/access-right/NON_PUBLIC",
}

# DSSC namespace shortcut (resolved via DCAT_CONTEXT above)
_DS_ACCESS_SCOPE = "ds:accessScope"
_DS_CONSENT_STATUS = "ds:consentStatus"


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


def _gov_facet(entry: DatasetEntry) -> dict[str, Any]:
    """Extract the governance facet from lineage JSON."""
    return ((entry.lineage or {}).get("facets") or {}).get("governance") or {}


def _build_agent_node(
    uri: str,
    owners: Optional[OwnersRegistry],
) -> dict[str, Any]:
    """Build a foaf:Agent JSON-LD node.

    When the owners registry has a matching entry the node is inlined with
    ``foaf:name``, ``foaf:homepage``, and the Schema.org subtype alongside
    ``foaf:Organization`` (required by DCAT-AP for ``dct:publisher``).
    Falls back to a bare ``{"@id": uri}`` when no match is found.
    """
    if owners is not None:
        entry = owners.by_uri(uri)
        if entry is not None:
            types: list[str] = ["foaf:Organization"]
            if entry.type and entry.type != "foaf:Organization":
                types.append(entry.type)
            node: dict[str, Any] = {"@id": uri, "@type": types}
            if entry.name:
                node["foaf:name"] = entry.name
            if entry.url:
                node["foaf:homepage"] = {"@id": entry.url}
            return node
    return {"@id": uri}


def _build_odrl_offer(entry_uri: str, entry: DatasetEntry) -> dict[str, Any]:
    """Build a minimal ODRL Offer derived from access_level + governance facet.

    - open:       no constraints
    - internal:   ds:accessScope eq dataspaces.query
    - restricted: ds:accessScope eq dataspaces.query + ds:consentStatus eq active
    consent_required also adds ds:consentStatus when set via governance facet.
    """
    level = entry.access_level or "internal"
    gov = _gov_facet(entry)
    consent_required = bool(
        gov.get("userFilterColumn") or gov.get("consentRequired")
    )

    constraints: list[dict[str, Any]] = []
    if level in ("internal", "restricted"):
        constraints.append({
            "odrl:leftOperand": {"@id": _DS_ACCESS_SCOPE},
            "odrl:operator": {"@id": "odrl:eq"},
            "odrl:rightOperand": "dataspaces.query",
        })
    if level == "restricted" or consent_required:
        constraints.append({
            "odrl:leftOperand": {"@id": _DS_CONSENT_STATUS},
            "odrl:operator": {"@id": "odrl:eq"},
            "odrl:rightOperand": "active",
        })

    permission: dict[str, Any] = {
        "@type": "odrl:Permission",
        "odrl:action": {"@id": "odrl:use"},
    }
    if constraints:
        permission["odrl:constraint"] = constraints

    return {
        "@id": f"{entry_uri}#offer",
        "@type": "odrl:Offer",
        "odrl:permission": [permission],
    }


def _build_dataset_node(
    entry: DatasetEntry,
    query_service_id: str,
    api_base: str,
    owners: Optional[OwnersRegistry] = None,
) -> dict[str, Any]:
    """Convert a single DatasetEntry to a dcat:Dataset JSON-LD node."""
    entry_uri = get_dataset_uri(entry.dataset_id)
    backend = entry.backend_config or {}
    tags = entry.tags or {}
    ns = (entry.lineage or {}).get("namespace")
    gov = _gov_facet(entry)
    level = entry.access_level or "internal"

    # ── Distribution node ──────────────────────────────────────────────────
    dist: dict[str, Any] = {
        "@id": f"{entry_uri}#distribution",
        "@type": "dcat:Distribution",
        "dct:title": entry.title or entry.dataset_id,
        "dct:identifier": entry.dataset_id,
        "dcat:mediaType": "application/json",
        "dcat:accessURL": {"@id": f"{api_base}/query"},
        "dcat:accessService": {"@id": query_service_id},
        "odrl:hasPolicy": _build_odrl_offer(entry_uri, entry),
    }

    ar_uri = _ACCESS_RIGHTS_URI.get(level)
    if ar_uri:
        dist["dct:accessRights"] = {"@id": ar_uri}

    if entry.license_uri:
        dist["dct:license"] = {"@id": entry.license_uri}
    if entry.rights_holder_uri:
        dist["dct:rightsHolder"] = _build_agent_node(entry.rights_holder_uri, owners)
    if tags.get("keywords"):
        dist["dcat:keyword"] = tags["keywords"]
    if isinstance(backend.get("size_bytes"), int):
        dist["dcat:byteSize"] = backend["size_bytes"]

    # BC-4: downloadURL only for open datasets
    if level == "open" and backend.get("public_url"):
        dist["dcat:downloadURL"] = {"@id": backend["public_url"]}

    # ── Dataset node ───────────────────────────────────────────────────────
    dataset: dict[str, Any] = {
        "@id": entry_uri,
        "@type": "dcat:Dataset",
        "dct:title": entry.title or entry.dataset_id,
        "dct:identifier": entry.dataset_id,
        "dcat:distribution": [dist],
    }

    if entry.description:
        dataset["dct:description"] = entry.description
    if ns:
        dataset["dct:isPartOf"] = {"@id": get_dataset_uri(ns)}
    publisher = entry.publisher_uri or str(settings.catalog_uri)
    dataset["dct:publisher"] = _build_agent_node(publisher, owners)
    if entry.landing_page:
        dataset["dcat:landingPage"] = {"@id": entry.landing_page}
    if entry.language_uris:
        dataset["dct:language"] = [{"@id": u} for u in entry.language_uris]
    if entry.spatial_uris:
        dataset["dct:spatial"] = [{"@id": u} for u in entry.spatial_uris]

    if tags.get("themes"):
        dataset["dcat:theme"] = [{"@id": t} for t in tags["themes"]]
    if tags.get("accrualPeriodicity"):
        dataset["dct:accrualPeriodicity"] = {"@id": tags["accrualPeriodicity"]}
    if tags.get("conformsTo"):
        dataset["dct:conformsTo"] = {"@id": tags["conformsTo"]}
    if tags.get("temporal"):
        temp: dict[str, Any] = {}
        if tags["temporal"].get("start"):
            temp["dcat:startDate"] = tags["temporal"]["start"]
        if tags["temporal"].get("end"):
            temp["dcat:endDate"] = tags["temporal"]["end"]
        if temp:
            dataset["dct:temporal"] = temp
    if tags.get("contactPoint"):
        cp = tags["contactPoint"]
        vcard: dict[str, Any] = {"@type": "vcard:Kind"}
        if cp.get("fn"):
            vcard["vcard:fn"] = cp["fn"]
        if cp.get("email"):
            vcard["vcard:hasEmail"] = cp["email"]
        dataset["dcat:contactPoint"] = vcard

    # Medallion from governance facet or dataset name inference
    medallion = gov.get("medallion") or _infer_medallion(
        (entry.lineage or {}).get("name") or entry.dataset_id
    )
    if medallion:
        dataset["ds:medallion"] = medallion

    return dataset


def _infer_medallion(name: str) -> Optional[str]:
    for level in ("gold", "silver", "bronze"):
        if level in name.lower():
            return level
    return None


def build_catalog(
    entries: Iterable[DatasetEntry],
    owners: Optional[OwnersRegistry] = None,
) -> dict[str, Any]:
    """Build a DCAT-AP 3 Catalog where each DatasetEntry is a dcat:Dataset.

    BC-2: Each DatasetEntry becomes its own dcat:Dataset (not grouped by namespace).
          Namespace lineage is expressed via dct:isPartOf.
    BC-3: dct:accessRights uses EU authority URIs, not raw strings.
    BC-4: dcat:downloadURL only appears on open-access datasets.
    """
    catalog_uri = str(settings.catalog_uri)
    api_base = str(settings.api_base_url).rstrip("/")
    query_service_id = f"{catalog_uri}/service"

    data_service: dict[str, Any] = {
        "@id": query_service_id,
        "@type": "dcat:DataService",
        "dct:title": f"{settings.app_name} Query Service",
        "dcat:endpointURL": {"@id": f"{api_base}/query"},
        "dcat:servesDataset": [],
    }

    dataset_nodes = []
    for e in list(entries):
        if e.access_level == "secret":
            continue
        node = _build_dataset_node(e, query_service_id, api_base, owners=owners)
        data_service["dcat:servesDataset"].append({"@id": node["@id"]})
        dataset_nodes.append(node)

    return {
        "@context": DCAT_CONTEXT,
        "@id": catalog_uri,
        "@type": "dcat:Catalog",
        "dct:title": settings.app_name,
        "dct:issued": dt.date.today().isoformat(),
        "dcat:service": [data_service],
        "dcat:dataset": dataset_nodes,
    }


def build_dataset(
    entry: DatasetEntry,
    owners: Optional[OwnersRegistry] = None,
) -> dict[str, Any]:
    """Build a single dcat:Dataset JSON-LD document for GET /catalogue/{id}."""
    api_base = str(settings.api_base_url).rstrip("/")
    catalog_uri = str(settings.catalog_uri)
    query_service_id = f"{catalog_uri}/service"

    node = _build_dataset_node(entry, query_service_id, api_base, owners=owners)
    return {
        "@context": DCAT_CONTEXT,
        **node,
    }
