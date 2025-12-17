# dataset/cli/export_openlineage.py
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, List, Optional

import httpx
import typer
import yaml

from celine.dataset.cli.utils import resolve_namespaces, setup_cli_logging
from celine.dataset.core.config import settings

logger = logging.getLogger(__name__)
export_app = typer.Typer(name="export", help="Export OpenLineage datasets into YAML")


def fetch_namespaces(marquez_url: str) -> list[str]:
    url = f"{marquez_url}/api/v1/namespaces"
    with httpx.Client(timeout=10.0) as client:
        resp = client.get(url)
        resp.raise_for_status()
        data = resp.json()
        return [ns["name"] for ns in data.get("namespaces", [])]


def fetch_all_datasets(marquez_url: str, namespace: str) -> list[dict]:
    url = f"{marquez_url}/api/v1/namespaces/{namespace}/datasets"
    limit = 100
    offset = 0
    out = []

    with httpx.Client(timeout=10.0) as client:
        while True:
            resp = client.get(url, params={"limit": limit, "offset": offset})
            resp.raise_for_status()
            page = resp.json().get("datasets", [])
            out.extend(page)
            if len(page) < limit:
                break
            offset += limit

    return out


def normalize_dataset_id(ds: dict) -> str:
    name = ds.get("name", "")
    return name.lower().replace(" ", "_")


def extract_lineage_info(mq: dict) -> dict:
    """Extract only stable OpenLineage lineage fields."""
    if not mq:
        return {}

    fields = {
        "namespace": mq.get("namespace"),
        "name": mq.get("name"),
        "sourceName": mq.get("sourceName"),
        "createdAt": mq.get("createdAt"),
        "updatedAt": mq.get("updatedAt") or mq.get("lastModifiedAt"),
        "lastLifecycleState": mq.get("lastLifecycleState"),
        "tags": mq.get("tags", []),
        "facets": mq.get("facets", {}),
    }
    return {k: v for k, v in fields.items() if v is not None}


def map_openlineage_to_catalogue(
    ds: dict, backend_type: str, expose: bool = False
) -> dict[str, Any]:
    """Convert Marquez/OpenLineage dataset into our YAML entry."""
    name = ds.get("name")
    physical = ds.get("physicalName")
    description = ds.get("description") or physical
    tags = ds.get("tags") or []

    lineage = extract_lineage_info(ds)

    facets = ds.get("facets") or {}
    gov = facets.get("governance") or {}

    # Remove OL metadata keys
    gov_data = (
        {k: v for k, v in gov.items() if not k.startswith("_")}
        if isinstance(gov, dict)
        else {}
    )

    entry: dict[str, Any] = {
        "title": name,
        "description": description,
        "backend_type": backend_type,
        "backend_config": {},
        "expose": expose,
        "ontology_path": None,
        "schema_override_path": None,
        "tags": {"keywords": tags},
        "lineage": lineage,
    }

    if backend_type == "postgres":
        entry["backend_config"] = {"table": physical}
    else:
        entry["backend_config"] = {
            "path": physical,
            "format": "application/octet-stream",
        }

    # ---------------- Governance mapping ----------------
    # license -> license_uri
    license_val = gov_data.get("license")
    if isinstance(license_val, str) and license_val:
        entry["license_uri"] = license_val

    # owners (list[str]) -> rights_holder_uri (first) and keywords
    owners = gov_data.get("owners") or []
    if isinstance(owners, list) and owners:
        # represent as URN, can be adjusted later
        first_owner = owners[0]
        entry["rights_holder_uri"] = f"urn:team:{first_owner}"
        # also add to keywords
        entry["tags"].setdefault("keywords", [])
        entry["tags"]["keywords"].extend([f"owner:{o}" for o in owners])

    # access level, access rights, classification
    access_level = gov_data.get("accessLevel")
    access_rights = gov_data.get("accessRights")
    classification = gov_data.get("classification")

    if access_level:
        entry["access_level"] = str(access_level)
        # DCAT accessRights goes under tags
        entry["tags"]["accessRights"] = str(access_level)

    if access_rights:
        entry["tags"]["accessRights"] = str(access_rights)

    # merge governance tags into keywords
    gov_tags = gov_data.get("tags") or []
    if gov_tags:
        kw = set(entry["tags"].get("keywords") or [])
        kw.update(gov_tags)
        if classification:
            kw.add(f"classification:{classification}")
        entry["tags"]["keywords"] = sorted(kw)
    elif classification:
        kw = set(entry["tags"].get("keywords") or [])
        kw.add(f"classification:{classification}")
        entry["tags"]["keywords"] = sorted(kw)

    # optionally keep the raw gov_data under lineage.facets for debugging
    if gov_data:
        lineage_facets = entry["lineage"].get("facets") or {}
        lineage_facets["governance"] = gov_data
        entry["lineage"]["facets"] = lineage_facets

    return entry


@export_app.command("openlineage")
def export_openlineage(
    ns: List[str] = typer.Option(
        ..., "--ns", help="Namespaces to include/exclude. Supports '*', +ns, -ns."
    ),
    out_dir: Path = typer.Option(..., "-o", "--output", help="Output base directory."),
    backend_type: str = typer.Option("postgres", help="Backend type for the datasets."),
    marquez_url: Optional[str] = typer.Option(None, "--marquez-url"),
    verbose: bool = typer.Option(False, "--verbose"),
    expose: bool = typer.Option(
        False, "--expose", help="Mark exported datasets as exposed."
    ),
):
    setup_cli_logging(verbose)

    base_url = str(marquez_url or settings.marquez_url).rstrip("/")
    if not base_url:
        logger.error("Marquez URL not configured.")
        raise typer.Exit(1)

    typer.echo(f"Querying Marquez at {base_url}")

    all_namespaces = fetch_namespaces(base_url)
    typer.echo(f"Found namespaces: {all_namespaces}")

    try:
        selected = resolve_namespaces(all_namespaces, ns)
    except ValueError as e:
        typer.echo(str(e), err=True)
        raise typer.Exit(1)

    typer.echo(f"Selected namespaces: {selected}")

    out_dir.mkdir(parents=True, exist_ok=True)

    for namespace in selected:
        datasets = fetch_all_datasets(base_url, namespace)

        mapped = {
            "datasets": {
                normalize_dataset_id(ds): map_openlineage_to_catalogue(
                    ds=ds, backend_type=backend_type, expose=expose
                )
                for ds in datasets
            }
        }

        if len(mapped["datasets"]) == 0:
            typer.echo(f"Skip empty namespace {namespace} ")
            continue

        outfile = out_dir / f"{namespace}.yaml"
        with outfile.open("w", encoding="utf-8") as f:
            yaml.safe_dump(mapped, f, sort_keys=False, allow_unicode=True)

        typer.echo(f"Wrote {len(mapped['datasets'])} datasets → {outfile}")
