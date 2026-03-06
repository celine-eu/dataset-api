# dataset/cli/row_filter.py
from __future__ import annotations

import glob
import logging
from pathlib import Path
from typing import List

import typer

from celine.dataset.cli.utils import load_yaml_file, write_yaml_file

logger = logging.getLogger(__name__)

row_filter_app = typer.Typer(
    name="row-filter", help="Add, remove or list row filters in exported YAML files"
)


def _expand_glob(pattern: str) -> List[Path]:
    matches = glob.glob(pattern, recursive=True)
    if not matches:
        return [Path(pattern)]
    return [Path(m) for m in sorted(matches)]


def _parse_args(args: List[str]) -> dict:
    result: dict = {}
    for item in args:
        if "=" not in item:
            typer.echo(f"Invalid --args value '{item}': expected key=value", err=True)
            raise typer.Exit(code=1)
        k, v = item.split("=", 1)
        result[k] = v
    return result


def _get_row_filters(dataset_entry: dict) -> List[dict]:
    gov = (
        dataset_entry.get("lineage", {})
        .get("facets", {})
        .get("governance", {})
    )
    rf = gov.get("rowFilters")
    if not isinstance(rf, list):
        return []
    return rf


def _set_row_filters(dataset_entry: dict, filters: List[dict]) -> None:
    lineage = dataset_entry.setdefault("lineage", {})
    facets = lineage.setdefault("facets", {})
    gov = facets.setdefault("governance", {})
    gov["rowFilters"] = filters


def _filters_match(existing: dict, handler: str, args: dict | None) -> bool:
    if existing.get("handler") != handler:
        return False
    if args is not None:
        return existing.get("args", {}) == args
    return True


@row_filter_app.command("add")
def row_filter_add(
    path: str = typer.Argument(..., help="YAML file or glob pattern"),
    dataset_name: str = typer.Argument(..., help="Dataset name/ID to update"),
    handler: str = typer.Option(..., "--handler", help="Row filter handler name"),
    args: List[str] = typer.Option(
        [],
        "--args",
        help="Handler arguments as key=value (can be repeated)",
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
):
    """Add a row filter to a dataset in one or more YAML files."""
    if verbose:
        logging.basicConfig(level=logging.DEBUG)

    parsed_args = _parse_args(args)
    files = _expand_glob(path)
    found = False

    for f in files:
        try:
            data = load_yaml_file(f)
        except FileNotFoundError:
            typer.echo(f"File not found: {f}", err=True)
            continue

        datasets = data.get("datasets", {})
        if dataset_name not in datasets:
            logger.debug("Dataset '%s' not in %s — skipping", dataset_name, f)
            continue

        found = True
        entry = datasets[dataset_name]
        current_filters = _get_row_filters(entry)

        # Check for duplicate
        new_filter = {"handler": handler}
        if parsed_args:
            new_filter["args"] = parsed_args

        if any(_filters_match(rf, handler, parsed_args) for rf in current_filters):
            typer.echo(
                f"{f}: '{dataset_name}' already has filter handler='{handler}' with matching args — skipped"
            )
            continue

        current_filters.append(new_filter)
        _set_row_filters(entry, current_filters)
        write_yaml_file(f, data)
        typer.echo(f"{f}: added filter handler='{handler}' to '{dataset_name}'")

    if not found:
        typer.echo(
            f"Dataset '{dataset_name}' not found in any matched file.", err=True
        )
        raise typer.Exit(code=1)


@row_filter_app.command("remove")
def row_filter_remove(
    path: str = typer.Argument(..., help="YAML file or glob pattern"),
    dataset_name: str = typer.Argument(..., help="Dataset name/ID to update"),
    handler: str = typer.Option(..., "--handler", help="Row filter handler name"),
    args: List[str] = typer.Option(
        [],
        "--args",
        help="Restrict removal to filters matching these key=value args (if omitted, all filters with the handler are removed)",
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
):
    """Remove a row filter from a dataset in one or more YAML files."""
    if verbose:
        logging.basicConfig(level=logging.DEBUG)

    parsed_args = _parse_args(args) if args else None
    files = _expand_glob(path)
    found = False

    for f in files:
        try:
            data = load_yaml_file(f)
        except FileNotFoundError:
            typer.echo(f"File not found: {f}", err=True)
            continue

        datasets = data.get("datasets", {})
        if dataset_name not in datasets:
            logger.debug("Dataset '%s' not in %s — skipping", dataset_name, f)
            continue

        found = True
        entry = datasets[dataset_name]
        current_filters = _get_row_filters(entry)

        before = len(current_filters)
        remaining = [
            rf for rf in current_filters
            if not _filters_match(rf, handler, parsed_args)
        ]
        removed = before - len(remaining)

        if removed == 0:
            typer.echo(
                f"{f}: no matching filter handler='{handler}' found in '{dataset_name}' — skipped"
            )
            continue

        _set_row_filters(entry, remaining)
        write_yaml_file(f, data)
        typer.echo(
            f"{f}: removed {removed} filter(s) handler='{handler}' from '{dataset_name}'"
        )

    if not found:
        typer.echo(
            f"Dataset '{dataset_name}' not found in any matched file.", err=True
        )
        raise typer.Exit(code=1)


@row_filter_app.command("list")
def row_filter_list(
    path: str = typer.Argument(..., help="YAML file or glob pattern"),
):
    """List all configured row filters across matched YAML files."""
    files = _expand_glob(path)
    any_filter_found = False

    for f in files:
        try:
            data = load_yaml_file(f)
        except FileNotFoundError:
            typer.echo(f"File not found: {f}", err=True)
            continue

        datasets = data.get("datasets", {})
        for dataset_name, entry in datasets.items():
            filters = _get_row_filters(entry)
            if not filters:
                continue
            any_filter_found = True
            typer.echo(f"{dataset_name}  [{f}]")
            for rf in filters:
                handler = rf.get("handler", "?")
                rf_args = rf.get("args", {})
                args_str = "  ".join(f"{k}={v}" for k, v in rf_args.items()) if rf_args else ""
                line = f"  handler={handler}"
                if args_str:
                    line += f"  {args_str}"
                typer.echo(line)

    if not any_filter_found:
        typer.echo("No row filters found.")
