# dataset/cli/export_governance.py
"""
CLI command to export governance.yaml files to OpenLineage-compatible catalogue YAML.

Finds governance.yaml files via a glob pattern, resolves governance rules
(merging per-dataset overrides with defaults), and produces YAML ready for
`import catalogue` — without requiring Marquez.
"""
from __future__ import annotations

import fnmatch
import glob as glob_module
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import typer
import yaml
from pydantic import BaseModel, Field

from celine.dataset.cli.utils import setup_cli_logging

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Inline governance models (mirrors celine-utils GovernanceRule/Config)
# ---------------------------------------------------------------------------


class GovernanceOwner(BaseModel):
    name: str
    type: str = "OWNER"


class GovernanceRule(BaseModel):
    title: Optional[str] = None
    description: Optional[str] = None
    license: Optional[str] = None
    attribution: Optional[str] = None
    ownership: List[GovernanceOwner] = Field(default_factory=list)
    access_level: Optional[str] = None
    access_requirements: Optional[str] = None
    classification: Optional[str] = None
    tags: List[str] = Field(default_factory=list)
    retention_days: Optional[int] = None
    documentation_url: Optional[str] = None
    source_system: Optional[str] = None
    user_filter_column: Optional[str] = None


class GovernanceConfig(BaseModel):
    defaults: GovernanceRule = Field(default_factory=GovernanceRule)
    sources: Dict[str, GovernanceRule] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------


def load_governance_yaml(path: Path) -> GovernanceConfig:
    with path.open(encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}

    defaults_raw = raw.get("defaults") or {}
    sources_raw = raw.get("sources") or {}

    defaults = GovernanceRule.model_validate(defaults_raw)

    sources: Dict[str, GovernanceRule] = {}
    for name, rule_raw in sources_raw.items():
        sources[name] = GovernanceRule.model_validate(rule_raw or {})

    return GovernanceConfig(defaults=defaults, sources=sources)


# ---------------------------------------------------------------------------
# Resolution (exact match → fnmatch glob → defaults)
# ---------------------------------------------------------------------------


def _merge_rule(base: GovernanceRule, override: GovernanceRule) -> GovernanceRule:
    """Overlay non-None/non-empty fields from override onto base."""
    data = base.model_dump()
    for field, value in override.model_dump().items():
        if value is None:
            continue
        if isinstance(value, list) and len(value) == 0:
            continue
        data[field] = value
    return GovernanceRule.model_validate(data)


def resolve_rule(config: GovernanceConfig, dataset_name: str) -> GovernanceRule:
    # 1. exact match
    if dataset_name in config.sources:
        return _merge_rule(config.defaults, config.sources[dataset_name])

    # 2. glob/fnmatch — prefer longest (most-specific) pattern
    best_key: Optional[str] = None
    for key in config.sources:
        if fnmatch.fnmatchcase(dataset_name, key):
            if best_key is None or len(key) > len(best_key):
                best_key = key

    if best_key is not None:
        return _merge_rule(config.defaults, config.sources[best_key])

    # 3. defaults only
    return config.defaults


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
    expose: bool,
) -> dict[str, Any]:
    physical_table = _derive_physical_table(dataset_name)
    title = rule.title or dataset_name
    description = rule.description or physical_table

    # Build governance facet (camelCase, matching GovernanceDatasetFacet)
    gov_facet: dict[str, Any] = {
        "_producer": "dataset-cli/export-governance",
        "_schemaURL": "https://celine-eu.github.io/schema/GovernanceDatasetFacet.schema.json",
    }
    if rule.title:
        gov_facet["title"] = rule.title
    if rule.description:
        gov_facet["description"] = rule.description
    if rule.license:
        gov_facet["license"] = rule.license
    if rule.attribution:
        gov_facet["attribution"] = rule.attribution
    if rule.ownership:
        gov_facet["owners"] = [o.name for o in rule.ownership]
    if rule.access_level:
        gov_facet["accessLevel"] = rule.access_level
    if rule.access_requirements:
        gov_facet["accessRequirements"] = rule.access_requirements
    if rule.classification:
        gov_facet["classification"] = rule.classification
    if rule.tags:
        gov_facet["tags"] = rule.tags
    if rule.retention_days is not None:
        gov_facet["retentionDays"] = rule.retention_days
    if rule.documentation_url:
        gov_facet["documentationUrl"] = rule.documentation_url
    if rule.source_system:
        gov_facet["sourceSystem"] = rule.source_system
    if rule.user_filter_column:
        gov_facet["userFilterColumn"] = rule.user_filter_column

    lineage: dict[str, Any] = {
        "name": dataset_name,
        "facets": {"governance": gov_facet},
    }

    # Build tags
    keywords: set[str] = set(rule.tags)
    owners = rule.ownership or []
    if owners:
        keywords.update(f"owner:{o.name}" for o in owners)
    if rule.classification:
        keywords.add(f"classification:{rule.classification}")

    tags: dict[str, Any] = {"keywords": sorted(keywords)}
    if rule.access_level:
        tags["accessRights"] = rule.access_level

    entry: dict[str, Any] = {
        "title": title,
        "description": description,
        "backend_type": backend_type,
        "backend_config": {},
        "expose": expose,
        "ontology_path": None,
        "schema_override_path": None,
        "tags": tags,
        "lineage": lineage,
        "access_level": rule.access_level,
        "license_uri": rule.license,
        "rights_holder_uri": f"urn:team:{owners[0].name}" if owners else None,
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
    expose: bool = typer.Option(
        False,
        "--expose",
        help="Mark exported datasets as exposed (visible in catalogue).",
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

    matched = sorted(glob_module.glob(glob_pattern, recursive=True))
    if not matched:
        typer.echo(f"No files matched pattern: {glob_pattern}", err=True)
        raise typer.Exit(1)

    typer.echo(f"Found {len(matched)} governance.yaml file(s)")
    out_dir.mkdir(parents=True, exist_ok=True)

    total_datasets = 0

    for gov_path_str in matched:
        gov_path = Path(gov_path_str)
        logger.debug("Processing %s", gov_path)

        try:
            config = load_governance_yaml(gov_path)
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
            datasets[dataset_id] = governance_rule_to_entry(
                dataset_name=dataset_name,
                rule=rule,
                backend_type=backend_type,
                expose=expose,
            )

        # Name output file after the parent directory of the governance.yaml
        # e.g. apps/demo3/governance.yaml -> demo3.yaml
        stem = gov_path.parent.name or gov_path.stem
        out_file = out_dir / f"{stem}.yaml"

        # If file already exists (two apps share a parent dir name), append suffix
        if out_file.exists():
            # Use grandparent.parent for disambiguation
            stem = f"{gov_path.parent.parent.name}__{stem}"
            out_file = out_dir / f"{stem}.yaml"

        with out_file.open("w", encoding="utf-8") as f:
            yaml.safe_dump({"datasets": datasets}, f, sort_keys=False, allow_unicode=True)

        typer.echo(f"  {gov_path} → {out_file} ({len(datasets)} datasets)")
        total_datasets += len(datasets)

    typer.echo(f"Done. Exported {total_datasets} datasets across {len(matched)} file(s).")
