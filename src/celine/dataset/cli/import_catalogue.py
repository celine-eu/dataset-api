# dataset/cli/import_catalogue.py
from __future__ import annotations

import fnmatch
import glob
import logging
from pathlib import Path
from typing import Any, Dict, List

import httpx
import typer
from pydantic import ValidationError

from celine.dataset.cli.utils import (
    load_yaml_file,
    resolve_namespaces,
    setup_cli_logging,
)
from celine.dataset.schemas.catalogue_import import (
    CatalogueImportModel,
    DatasetEntryModel,
)

logger = logging.getLogger(__name__)

import_app = typer.Typer(name="import", help="Import catalogue into Dataset API")


def resolve_dataset_id_filters(
    dataset_ids: List[str],
    filters: List[str] | None,
) -> List[str]:
    """
    Resolve dataset_id filters with +pattern / -pattern semantics.

    Patterns are glob-based.
    """
    if not filters:
        return dataset_ids

    include_patterns: list[str] = []
    exclude_patterns: list[str] = []

    for f in filters:
        if f.startswith("-"):
            exclude_patterns.append(f[1:])
        elif f.startswith("+"):
            include_patterns.append(f[1:])
        else:
            include_patterns.append(f)

    def matches_any(dataset_id: str, patterns: list[str]) -> bool:
        return any(fnmatch.fnmatchcase(dataset_id, p) for p in patterns)

    # Step 1: apply includes
    if include_patterns:
        selected = [
            ds_id for ds_id in dataset_ids if matches_any(ds_id, include_patterns)
        ]
    else:
        selected = list(dataset_ids)

    # Step 2: apply excludes
    if exclude_patterns:
        selected = [
            ds_id for ds_id in selected if not matches_any(ds_id, exclude_patterns)
        ]

    return selected


def validate_catalogue_payload(
    raw_entries: Dict[str, Dict[str, Any]], strict: bool = False
) -> List[DatasetEntryModel]:
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
    """Expand the `--input` patterns into a **stable** list of paths.

    `glob.glob` returns `readdir` order. That is not sorted, it is not the same
    on two machines, and it changes when a directory is rewritten — so an import
    that merges several files was, until this was sorted, letting the filesystem
    decide which of two declarations of one `dataset_id` reached the catalogue.

    Sorting does not make the *winner* right; `import_catalogue` refuses a
    disagreement rather than picking one. What sorting buys is that two runs over
    the same inputs read them in the same sequence, so the refusal, the messages
    and the `--allow-conflicts` fallback are all reproducible.

    Overlapping patterns are de-duplicated: the same path is read once.
    """
    out: List[Path] = []
    seen: set[str] = set()
    for p in patterns:
        s = str(p)
        matches = sorted(glob.glob(s)) if any(c in s for c in "*?[]") else [s]
        for m in matches:
            if m in seen:
                continue
            seen.add(m)
            out.append(Path(m))
    return out


def _differing_fields(first: Dict[str, Any], second: Dict[str, Any]) -> List[str]:
    """The keys on which two declarations of one dataset disagree."""
    return sorted(k for k in set(first) | set(second) if first.get(k) != second.get(k))


def extract_dataset_namespace(entry: Dict[str, Any]) -> str:
    lineage = entry.get("lineage") or {}
    return lineage.get("namespace") or "default"


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
    datasets_filter: List[str] = typer.Option(
        None,
        "--datasets",
        help=(
            "Dataset filter by dataset_id using glob patterns. "
            "Supports +pattern / -pattern (e.g. singer.*, -singer.tmp.*). "
            "Can be passed multiple times."
        ),
    ),
    api_url: str = typer.Option(..., "--api-url", help="Dataset API base URL"),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
    strict: bool = typer.Option(
        False, "--strict", help="Fail on first validation error."
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Print selected dataset IDs and exit without importing.",
    ),
    allow_conflicts: bool = typer.Option(
        False,
        "--allow-conflicts",
        help=(
            "Do not refuse a dataset_id declared differently in two inputs; "
            "keep the declaration from the last file in sorted order."
        ),
    ),
):
    setup_cli_logging(verbose)

    files = expand_inputs(input_yaml)
    if not files:
        typer.echo("No input YAML files found from provided patterns.", err=True)
        raise typer.Exit(code=1)

    typer.echo(f"Found {len(files)} YAML file(s).")

    # **A repeated `dataset_id` is a disagreement, and it is refused.**
    #
    # The inputs are independent exports merged into one flat catalogue, so a
    # `dataset_id` can only carry one description. Until this check existed the
    # loop kept the last file's body and printed `Overwriting.` — with the
    # filesystem choosing which body that was. Sorting the inputs makes the
    # outcome reproducible but not *correct*: alphabetical order has nothing to
    # do with which of two contradictory descriptions of a dataset is true, so
    # an ordered last-writer-wins would only have turned an unnoticed defect
    # into a stable one.
    #
    # The refusal is narrowed to a real disagreement. Re-declaring the same
    # dataset with an identical body is ordinary — an app naming an upstream it
    # consumes — and carries no information to lose, so it passes silently.
    # `--allow-conflicts` exists for the caller who knows the inputs disagree
    # and wants the import anyway; it is then a *deterministic* last-writer-wins,
    # because the file list is sorted.
    collected: Dict[str, Dict[str, Any]] = {}
    origin: Dict[str, Path] = {}
    conflicts: List[str] = []

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
                if entry == collected[ds_id]:
                    logger.debug(
                        "dataset_id '%s' re-declared identically in %s", ds_id, f
                    )
                    continue
                fields = _differing_fields(collected[ds_id], entry)
                conflicts.append(ds_id)
                typer.echo(
                    f"Conflict: dataset_id '{ds_id}' is declared differently in "
                    f"{origin[ds_id]} and {f} — they disagree on "
                    f"{', '.join(fields)}.",
                    err=True,
                )
            collected[ds_id] = entry
            origin[ds_id] = f

    if conflicts and not allow_conflicts:
        typer.echo(
            f"Refusing to import: {len(conflicts)} dataset_id(s) are declared "
            "differently by two inputs, and no ordering of the files can say "
            "which declaration is the right one. Resolve the disagreement at "
            "the source, drop the stale export, or pass --allow-conflicts to "
            "keep the last declaration in sorted-filename order.",
            err=True,
        )
        raise typer.Exit(code=1)

    if not collected:
        typer.echo("Combined input contains NO datasets.", err=True)
        raise typer.Exit(code=1)

    typer.echo(f"Loaded {len(collected)} total datasets before filtering.")

    all_namespaces = sorted(
        {extract_dataset_namespace(entry) for entry in collected.values()}
    )

    typer.echo(f"Namespaces found in input: {all_namespaces}")

    if not ns:
        selected_namespaces = all_namespaces
    else:
        try:
            selected_namespaces = resolve_namespaces(all_namespaces, ns)
        except ValueError as e:
            typer.echo(str(e), err=True)
            raise typer.Exit(code=1)

    typer.echo(f"Selected namespaces: {selected_namespaces}")

    # 1. Namespace filtering
    filtered = {
        ds_id: entry
        for ds_id, entry in collected.items()
        if extract_dataset_namespace(entry) in selected_namespaces
    }

    if not filtered:
        typer.echo("No datasets match the selected namespace filters.", err=True)
        raise typer.Exit(code=1)

    # 2. Dataset ID filtering (glob)
    resolved_ids = resolve_dataset_id_filters(
        list(filtered.keys()),
        datasets_filter,
    )

    filtered = {ds_id: filtered[ds_id] for ds_id in resolved_ids}

    if not filtered:
        typer.echo("No datasets match the dataset_id filters.", err=True)
        raise typer.Exit(code=1)

    if datasets_filter:
        typer.echo(f"Dataset ID filters: {datasets_filter}")

    typer.echo(f"{len(filtered)} datasets remaining after filtering.")

    if dry_run:
        typer.echo("Dry run: selected dataset IDs")
        for ds_id in sorted(filtered.keys()):
            typer.echo(ds_id)
        raise typer.Exit(code=0)

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
