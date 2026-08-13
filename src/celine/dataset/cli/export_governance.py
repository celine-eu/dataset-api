# dataset/cli/export_governance.py
"""
CLI command to export governance.yaml files to OpenLineage-compatible catalogue YAML.

Finds governance.yaml files via a glob pattern, resolves governance rules
(merging per-dataset overrides with defaults), and produces YAML ready for
`import catalogue` — without requiring Marquez.
"""
from __future__ import annotations

import glob as glob_module
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import typer
import yaml

from celine.dataset.cli.ontology_resolver import OntologyResolutionError, resolve_mapping
from celine.dataset.cli.utils import setup_cli_logging

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Governance grammar — parsed by `celine.governance`, not here
# ---------------------------------------------------------------------------
#
# This module used to carry its own copy of the models, the parser, the merge
# and the resolver — roughly 290 lines mirroring `celine-utils`. That copy
# existed for a good reason: `celine-utils` required dbt, Meltano, Prefect and
# Keycloak in order to parse a YAML file, and an API service cannot take an
# orchestration stack as a dependency.
#
# `celine.governance` is that grammar with a core of pydantic + pyyaml +
# jsonschema, so the copy is no longer buying anything — and the four copies
# that existed had already drifted apart on how a dataset overlays its file's
# defaults, which is what a governance file is *for*.
#
# The behavioural change that comes with adopting it: overlays now merge by
# `exclude_unset` rather than truthiness, so a dataset can override an inherited
# value with `false` or `null`. `dataspace.expose: false` over a file default of
# `true` previously did nothing at all.
#
# `OwnersRegistry` comes from the same place for the same reason. This repo's
# copy was byte-identical *except* that it lacked `aliases`, so the generic
# owner labels the open-source pipelines carry — `dso`, `rec` — resolved to
# nothing and fell through to a synthetic `urn:owner:<alias>` below.

from celine.governance import (  # noqa: E402
    DataspaceConfig,
    DcatConfig,
    GovernanceConfig,
    GovernanceOwner,
    GovernanceResolver,
    GovernanceRule,
    dataspace_expose,
    effective_expose,
    exposure_conflict,
    OntologyConfig,
    OwnersRegistry,
    TemporalCoverage,
    build_facet,
    load_owners_yaml,
    merge_configs,
    merge_rules,
    parse_rule,
)


def load_governance_yaml(path: Path) -> GovernanceConfig:
    """Load one governance file into a config (no deployer overlay)."""
    return GovernanceResolver.from_file(path).config


def load_governance_with_override(
    base_path: Path, app_name: Optional[str] = None
) -> GovernanceConfig:
    """Load governance.yaml and merge a deployer override if present.

    Looks for ``governance.<app_name>.yaml`` next to the base file; when
    ``app_name`` is not given it is inferred from the parent directory name,
    which is this repo's long-standing behaviour and why `infer_from_dir` is
    passed explicitly.
    """
    return GovernanceResolver.from_file_with_override(
        base_path, app_name, infer_from_dir=True
    ).config


def resolve_rule(config: GovernanceConfig, dataset_name: str) -> GovernanceRule:
    """Resolve a dataset against a config: exact match → longest glob → defaults."""
    return GovernanceResolver(config).resolve(dataset_name)



# ---------------------------------------------------------------------------
# Mapping
# ---------------------------------------------------------------------------


def _derive_physical_table(dataset_name: str) -> str:
    """
    Derive the physical schema.table reference from an OpenLineage-style name.

    "datasets.ds_dev_gold.foo"  -> "ds_dev_gold.foo"
    "singer.tap-test.foo"       -> "tap-test.foo"
    "schema.table"              -> "schema.table"  (already 2-part, kept as-is)
    """
    parts = dataset_name.split(".")
    if len(parts) >= 3:
        return ".".join(parts[1:])
    return dataset_name


def _normalize_dataset_id(dataset_name: str) -> str:
    return dataset_name.lower().replace("-", "_").replace(" ", "_")


def governance_rule_to_entry(
    dataset_name: str,
    rule: GovernanceRule,
    backend_type: str,
    owners: OwnersRegistry | None = None,
    base_dir: Path | None = None,
) -> dict[str, Any]:
    physical_table = _derive_physical_table(dataset_name)
    title = rule.title or dataset_name
    description = rule.description or physical_table

    # One projection of a rule onto the facet, shared with the lineage extractors.
    # This was a hand-built dict with camelCase keys typed by eye and the schema
    # URL hardcoded — matching a class in a package this repo did not depend on.
    gov_facet = build_facet(rule, producer="dataset-cli/export-governance")

    lineage: dict[str, Any] = {
        "name": dataset_name,
        "facets": {"governance": gov_facet},
    }

    # Build tags block
    keywords: set[str] = set(rule.tags)
    ownership = rule.ownership or []
    if ownership:
        keywords.update(f"owner:{o.name}" for o in ownership)
    if rule.classification:
        keywords.add(f"classification:{rule.classification}")

    tags: dict[str, Any] = {"keywords": sorted(keywords)}
    if rule.access_level:
        tags["accessRights"] = rule.access_level  # stored as string; DCAT converts to URI

    # Map dcat sub-fields into tags (catalogue_admin reads tags.themes etc.)
    dcat = rule.dcat
    if dcat:
        if dcat.themes:
            tags["themes"] = dcat.themes
        if dcat.accrual_periodicity:
            tags["accrualPeriodicity"] = dcat.accrual_periodicity
        if dcat.conforms_to:
            tags["conformsTo"] = dcat.conforms_to
        if dcat.temporal:
            tags["temporal"] = {
                k: v for k, v in dcat.temporal.model_dump().items() if v is not None
            }

    # Two gates, resolved by the library so every consumer agrees on what a file
    # means. `expose` falls back to `dataspace.expose` when unstated, which is
    # what keeps files written before the split behaving exactly as they did.
    catalogue_expose = effective_expose(rule)
    offered_to_dataspace = dataspace_expose(rule)

    # Resolve rights_holder and publisher URIs via owners registry when available.
    # Priority: DID > URL > urn:owner:<alias> fallback.
    def _owner_uri(alias: str) -> str:
        if owners_registry := owners:
            uri = owners_registry.canonical_uri(alias)
            if uri:
                return uri
        return f"urn:owner:{alias}"

    rights_holder_uri = _owner_uri(rule.ownership[0].name) if rule.ownership else None

    # publisher_uri: prefer explicit dcat.publisher_uri, then first owner
    publisher_uri = (dcat.publisher_uri if dcat else None) or (
        _owner_uri(rule.ownership[0].name) if rule.ownership else None
    )

    # Resolve the semantic-model binding. `base_dir` is the directory holding the
    # governance.yaml, because `ontology.spec_file` is relative to it; when the
    # caller has no file context a local spec cannot be resolved and saying so is
    # better than resolving it against the process's cwd.
    ontology_path, ontology_mapping = resolve_mapping(
        rule.ontology,
        base_dir if base_dir is not None else Path.cwd(),
        dataset_name,
    )

    entry: dict[str, Any] = {
        "title": title,
        "description": description,
        "backend_type": backend_type,
        "backend_config": {},
        "expose": catalogue_expose,
        "dataspace_expose": offered_to_dataspace,
        "ontology_path": ontology_path,
        "ontology_mapping": ontology_mapping,
        "schema_override_path": None,
        "tags": tags,
        "lineage": lineage,
        "access_level": rule.access_level,
        "license_uri": rule.license,
        "rights_holder_uri": rights_holder_uri,
        # DCAT-specific ORM fields — populated from dcat block or ownership
        "publisher_uri": publisher_uri,
        "language_uris": (dcat.language_uris or None) if dcat else None,
        "spatial_uris": (dcat.spatial_uris or None) if dcat else None,
        "landing_page": rule.documentation_url,
    }

    if backend_type == "postgres":
        entry["backend_config"] = {"table": physical_table}
    else:
        entry["backend_config"] = {"path": physical_table, "format": "application/octet-stream"}

    return entry


# ---------------------------------------------------------------------------
# CLI command
# ---------------------------------------------------------------------------


def export_governance_cmd(
    glob_pattern: str = typer.Argument(
        ...,
        help=(
            "Glob pattern to find governance.yaml files. "
            "Quote the pattern to prevent shell expansion, e.g. "
            '"/path/to/**/governance.yaml"'
        ),
    ),
    out_dir: Path = typer.Option(..., "-o", "--output", help="Output directory for YAML files."),
    backend_type: str = typer.Option(
        "postgres",
        "--backend-type",
        help="Backend type for the datasets (postgres, s3, fs).",
    ),
    owners_path: Optional[Path] = typer.Option(
        None,
        "--owners",
        help=(
            "Path to owners.yaml registry. When set, owner aliases in governance "
            "files are resolved to canonical URIs (DID or URL) for publisher_uri "
            "and rights_holder_uri fields. If not set, a ./owners.yaml alongside "
            "each governance.yaml is tried automatically."
        ),
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
):
    """
    Export governance.yaml files to OpenLineage-compatible catalogue YAML.

    Finds governance.yaml files via GLOB_PATTERN, parses governance rules,
    and produces catalogue YAML files ready for `import catalogue`.
    Does not require a database connection or Marquez.
    """
    setup_cli_logging(verbose)

    # Load owners registry — explicit flag > ./owners.yaml beside each governance file
    _global_owners: OwnersRegistry | None = None
    if owners_path is not None:
        try:
            _global_owners = load_owners_yaml(owners_path)
            typer.echo(f"Loaded {len(_global_owners)} owner(s) from {owners_path}")
        except Exception as exc:
            typer.echo(f"WARNING: could not load owners registry at {owners_path}: {exc}", err=True)

    matched = sorted(glob_module.glob(glob_pattern, recursive=True))
    if not matched:
        typer.echo(f"No files matched pattern: {glob_pattern}", err=True)
        raise typer.Exit(1)

    typer.echo(f"Found {len(matched)} governance.yaml file(s)")
    out_dir.mkdir(parents=True, exist_ok=True)

    total_datasets = 0
    ontology_errors: list[str] = []
    exposure_errors: list[str] = []

    for gov_path_str in matched:
        gov_path = Path(gov_path_str)
        logger.debug("Processing %s", gov_path)

        # Per-file owners registry: global flag > sibling owners.yaml > None
        file_owners = _global_owners
        if file_owners is None:
            sibling = gov_path.parent / "owners.yaml"
            if sibling.is_file():
                try:
                    file_owners = load_owners_yaml(sibling)
                    logger.debug("Using sibling owners.yaml at %s", sibling)
                except Exception as exc:
                    logger.warning("Could not load %s: %s", sibling, exc)

        try:
            config = load_governance_with_override(gov_path)
        except Exception as exc:
            typer.echo(f"  ERROR loading {gov_path}: {exc}", err=True)
            continue

        if not config.sources:
            typer.echo(f"  SKIP {gov_path} — no sources declared")
            continue

        datasets: dict[str, Any] = {}
        for dataset_name, _ in config.sources.items():
            rule = resolve_rule(config, dataset_name)
            dataset_id = _normalize_dataset_id(dataset_name)

            # The two gates are AND, and only one combination is incoherent:
            # offered into the dataspace but absent from the catalogue. A
            # consumer reaches a dataspace asset through the catalogue entry
            # describing it, so that pairing cannot be honoured — and resolving
            # it either way in silence is the wrong move in both directions.
            # Collected like the ontology errors so one run reports every
            # instance, and fatal at the end for the same reason.
            if (conflict := exposure_conflict(rule)) is not None:
                typer.echo(f"  ERROR {gov_path} [{dataset_name}]: {conflict}", err=True)
                exposure_errors.append(f"{gov_path} [{dataset_name}]: {conflict}")
                continue
            try:
                datasets[dataset_id] = governance_rule_to_entry(
                    dataset_name=dataset_name,
                    rule=rule,
                    backend_type=backend_type,
                    owners=file_owners,
                    base_dir=gov_path.parent,
                )
            except OntologyResolutionError as exc:
                # Collected rather than raised, so one run reports every broken
                # binding instead of the first. Still fatal at the end: a dataset
                # that declares a mapping and silently exports without one would
                # serve 404 from /vocabulary, which reads as "no model declared"
                # — the opposite of what the governance file says.
                typer.echo(f"  ERROR {gov_path}: {exc}", err=True)
                ontology_errors.append(f"{gov_path}: {exc}")

        # Name output file after the parent directory of the governance.yaml
        # e.g. apps/demo3/governance.yaml -> demo3.yaml
        stem = gov_path.parent.name or gov_path.stem
        out_file = out_dir / f"{stem}.yaml"

        # If file already exists (two apps share a parent dir name), append suffix
        if out_file.exists():
            stem = f"{gov_path.parent.parent.name}__{stem}"
            out_file = out_dir / f"{stem}.yaml"

        with out_file.open("w", encoding="utf-8") as f:
            yaml.safe_dump({"datasets": datasets}, f, sort_keys=False, allow_unicode=True)

        typer.echo(f"  {gov_path} → {out_file} ({len(datasets)} datasets)")
        total_datasets += len(datasets)

    typer.echo(f"Done. Exported {total_datasets} datasets across {len(matched)} file(s).")

    if exposure_errors:
        typer.echo(
            f"\n{len(exposure_errors)} dataset(s) are offered into the dataspace "
            f"but not listed in the catalogue, and were omitted from the export.",
            err=True,
        )

    if ontology_errors:
        typer.echo(
            f"\n{len(ontology_errors)} dataset(s) declared a semantic model that "
            f"could not be resolved and were omitted from the export.",
            err=True,
        )

    # One exit for both. Keeping it inside the ontology branch would have let an
    # export that dropped datasets for exposure conflicts still return 0, and
    # this command runs unattended in `task import:dataset-sync` — a zero exit
    # there means the catalogue was synced, not that some of it was skipped.
    if exposure_errors or ontology_errors:
        raise typer.Exit(1)
