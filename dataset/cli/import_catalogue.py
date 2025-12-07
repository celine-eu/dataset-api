# dataset/cli/import_catalogue.py
from __future__ import annotations

import logging
from pathlib import Path
from typing import List, Dict, Any
import typer
import httpx
import glob
from pydantic import ValidationError

from dataset.cli.utils import setup_cli_logging, load_yaml_file, resolve_namespaces
from dataset.catalogue.schema import CatalogueImportModel, DatasetEntryModel

logger = logging.getLogger(__name__)

import_app = typer.Typer(name="import", help="Import catalogue into Dataset API")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def validate_catalogue_payload(
    raw_entries: Dict[str, Dict[str, Any]], strict=False
) -> List[DatasetEntryModel]:
    """
    Convert merged YAML dicts into validated Pydantic models.
    """
    dataset_models: List[DatasetEntryModel] = []

    for ds_id, entry in raw_entries.items():
        entry_with_id = {"dataset_id": ds_id, **entry}
        try:
            dataset_models.append(DatasetEntryModel(**entry_with_id))
        except ValidationError as exc:
            typer.echo(f"Validation error in dataset '{ds_id}':\n{exc}", err=True)
            if strict:
                raise typer.Exit(code=1)
            else:
                typer.echo("Warning: skipping invalid dataset ...", err=True)
                continue

    return dataset_models


def expand_inputs(patterns: List[Path]) -> List[Path]:
    """Expand input paths where some may be globs."""
    out: List[Path] = []
    for p in patterns:
        s = str(p)
        if any(c in s for c in "*?[]"):
            matches = [Path(m) for m in glob.glob(s)]
            out.extend(matches)
        else:
            out.append(Path(p))
    return out


def extract_dataset_namespace(entry: Dict[str, Any]) -> str:
    """
    Return the dataset's namespace from the lineage block if present.
    Default is "default" if missing.
    """
    lineage = entry.get("lineage") or {}
    return lineage.get("namespace") or "default"


# ---------------------------------------------------------------------------
# CLI Command
# ---------------------------------------------------------------------------


@import_app.command("catalogue")
def import_catalogue(
    input_yaml: List[Path] = typer.Option(
        ...,
        "--input",
        "-i",
        help="YAML file or glob (can be passed multiple times).",
        exists=False,
    ),
    ns: List[str] = typer.Option(
        None,
        "--ns",
        help="Namespace filter (supports '*', +ns, -ns). Default: import ALL.",
    ),
    api_url: str = typer.Option(..., "--api-url", help="Dataset API base URL"),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
    strict: bool = typer.Option(
        False, "--strict", help="Fail on first validation error."
    ),
):
    """
    Import datasets into the Dataset API.
    Supports namespace filtering, and glob input files.
    """
    setup_cli_logging(verbose)

    # Expand file globs
    files = expand_inputs(input_yaml)
    if not files:
        typer.echo("No input YAML files found from provided patterns.", err=True)
        raise typer.Exit(code=1)

    typer.echo(f"Found {len(files)} YAML file(s).")

    # ----------------------------------------------------------------------
    # Load all datasets from input files
    # ----------------------------------------------------------------------
    collected: Dict[str, Dict[str, Any]] = {}

    for f in files:
        try:
            data = load_yaml_file(f)
        except Exception as exc:
            typer.echo(f"Failed to load YAML {f}: {exc}", err=True)
            raise typer.Exit(code=1)

        ds_block = data.get("datasets")
        if not ds_block:
            typer.echo(f"Warning: YAML {f} has no 'datasets' section.", err=True)
            continue

        for ds_id, entry in ds_block.items():
            if ds_id in collected:
                typer.echo(
                    f"Warning: duplicate dataset_id '{ds_id}' from {f}. Overwriting.",
                    err=True,
                )
            collected[ds_id] = entry

    if not collected:
        typer.echo("Combined input contains NO datasets.", err=True)
        raise typer.Exit(code=1)

    typer.echo(f"Loaded {len(collected)} total datasets before filtering.")

    # ----------------------------------------------------------------------
    # Namespace filtering
    # ----------------------------------------------------------------------
    # Discover all namespaces present in loaded YAML
    all_namespaces = sorted(
        {extract_dataset_namespace(entry) for entry in collected.values()}
    )

    typer.echo(f"Namespaces found in input: {all_namespaces}")

    # If no --ns argument was provided → import ALL namespaces
    if not ns:
        selected_namespaces = all_namespaces
    else:
        # Reuse the same namespace resolution logic as export_openlineage
        try:
            selected_namespaces = resolve_namespaces(all_namespaces, ns)
        except ValueError as e:
            typer.echo(str(e), err=True)
            raise typer.Exit(code=1)

    typer.echo(f"Selected namespaces: {selected_namespaces}")

    # Filter datasets to selected namespaces
    filtered = {
        ds_id: entry
        for ds_id, entry in collected.items()
        if extract_dataset_namespace(entry) in selected_namespaces
    }

    if not filtered:
        typer.echo("No datasets match the selected namespace filters.", err=True)
        raise typer.Exit(code=1)

    typer.echo(f"{len(filtered)} datasets remaining after filtering.")

    # ----------------------------------------------------------------------
    # Prepare payload and POST
    # ----------------------------------------------------------------------
    validated = validate_catalogue_payload(filtered, strict)
    payload = CatalogueImportModel(datasets=validated).model_dump()

    url = api_url.rstrip("/") + "/admin/catalogue"

    typer.echo(f"Importing {len(payload['datasets'])} datasets → {url}")

    try:
        with httpx.Client(timeout=15.0) as client:
            resp = client.post(url, json=payload)
            resp.raise_for_status()
    except Exception as exc:
        typer.echo(f"Error importing catalogue: {exc}", err=True)
        raise typer.Exit(code=1)

    typer.echo("Catalogue import complete.")
