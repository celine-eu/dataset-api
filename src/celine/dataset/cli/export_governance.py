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
from pydantic import BaseModel, ConfigDict, Field

from celine.dataset.cli.utils import setup_cli_logging
from celine.dataset.core.owners import OwnersRegistry, load_owners_yaml

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Inline governance models (self-contained; mirrors celine-utils GovernanceRule)
# ---------------------------------------------------------------------------


class GovernanceOwner(BaseModel):
    name: str
    type: str = "OWNER"


class TemporalCoverage(BaseModel):
    start: Optional[str] = None
    end: Optional[str] = None


class DcatConfig(BaseModel):
    """DCAT-AP metadata for catalogue exposition."""

    model_config = ConfigDict(extra="ignore")

    publisher_uri: Optional[str] = None
    themes: List[str] = Field(default_factory=list)
    language_uris: List[str] = Field(default_factory=list)
    spatial_uris: List[str] = Field(default_factory=list)
    accrual_periodicity: Optional[str] = None
    conforms_to: Optional[str] = None
    temporal: Optional[TemporalCoverage] = None


class DataspaceConfig(BaseModel):
    """Dataspace ODRL policy hints.

    Extra fields (e.g. EDC-specific asset/data_address/contract sub-objects written
    by the dataspaces connector) are silently ignored so that a single governance.yaml
    can be shared between dataset-api and the dataspaces connector.
    """

    model_config = ConfigDict(extra="ignore")

    medallion: Optional[str] = None
    contract_required: bool = False
    consent_required: bool = False
    odrl_action: str = "use"
    purpose: List[str] = Field(default_factory=list)
    expose: bool = False                     # catalogue visibility (set per-source)


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
    row_filters: List[dict] = Field(default_factory=list)
    dcat: Optional[DcatConfig] = None
    dataspace: Optional[DataspaceConfig] = None


class GovernanceConfig(BaseModel):
    defaults: GovernanceRule = Field(default_factory=GovernanceRule)
    sources: Dict[str, GovernanceRule] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

_KNOWN_KEYS = {
    "title", "description", "license", "attribution", "ownership",
    "access_level", "access_requirements", "classification", "tags",
    "retention_days", "documentation_url", "source_system",
    "row_filters", "dcat", "dataspace",
    # v2 keys from dataspaces connector — ignored here
    "policy",
}


def _parse_rule(data: dict[str, Any]) -> GovernanceRule:
    block: dict[str, Any] = (
        data.get("governance") if "governance" in data else data
    ) or {}

    owners_raw = block.get("ownership") or []
    owners = [
        GovernanceOwner(**o) if isinstance(o, dict) else GovernanceOwner(name=str(o))
        for o in owners_raw
    ]

    dcat_raw = block.get("dcat") or {}
    dataspace_raw = block.get("dataspace") or {}

    return GovernanceRule(
        title=block.get("title"),
        description=block.get("description"),
        license=block.get("license"),
        attribution=block.get("attribution"),
        ownership=owners,
        access_level=block.get("access_level"),
        access_requirements=block.get("access_requirements"),
        classification=block.get("classification"),
        tags=block.get("tags") or [],
        retention_days=block.get("retention_days"),
        documentation_url=block.get("documentation_url"),
        source_system=block.get("source_system"),
        row_filters=block.get("row_filters") or [],
        dcat=DcatConfig.model_validate(dcat_raw) if dcat_raw else None,
        dataspace=DataspaceConfig.model_validate(dataspace_raw) if dataspace_raw else None,
    )


def load_governance_yaml(path: Path) -> GovernanceConfig:
    with path.open(encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}

    defaults = _parse_rule(raw.get("defaults") or {})
    sources: Dict[str, GovernanceRule] = {
        name: _parse_rule(rule_raw or {})
        for name, rule_raw in (raw.get("sources") or {}).items()
    }
    return GovernanceConfig(defaults=defaults, sources=sources)


# ---------------------------------------------------------------------------
# Resolution (exact match → fnmatch glob → defaults)
# ---------------------------------------------------------------------------


def _merge_dataspace(
    base: Optional[DataspaceConfig], override: Optional[DataspaceConfig]
) -> Optional[DataspaceConfig]:
    if base is None:
        return override
    if override is None:
        return base
    return DataspaceConfig(
        medallion=override.medallion or base.medallion,
        contract_required=base.contract_required or override.contract_required,
        consent_required=base.consent_required or override.consent_required,
        odrl_action=override.odrl_action if override.odrl_action != "use" else base.odrl_action,
        purpose=sorted(set(base.purpose) | set(override.purpose)),
        expose=base.expose or override.expose,
    )


def _merge_rule(base: GovernanceRule, override: GovernanceRule) -> GovernanceRule:
    """Overlay non-None/non-empty fields from override onto base."""
    data = base.model_dump()
    for field, value in override.model_dump().items():
        if field == "dataspace":
            continue  # handled separately below
        if value is None:
            continue
        if isinstance(value, list) and len(value) == 0:
            continue
        data[field] = value
    merged = GovernanceRule.model_validate(data)
    merged.dataspace = _merge_dataspace(base.dataspace, override.dataspace)
    return merged


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
    owners: OwnersRegistry | None = None,
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
    if rule.row_filters:
        gov_facet["rowFilters"] = rule.row_filters

    # Dataspace hints go into the governance facet so the DCAT formatter can read them
    ds_cfg = rule.dataspace
    if ds_cfg:
        if ds_cfg.medallion:
            gov_facet["medallion"] = ds_cfg.medallion
        if ds_cfg.contract_required:
            gov_facet["contractRequired"] = True
        if ds_cfg.consent_required:
            gov_facet["consentRequired"] = True
        if ds_cfg.odrl_action != "use":
            gov_facet["odrlAction"] = ds_cfg.odrl_action
        if ds_cfg.purpose:
            gov_facet["purpose"] = ds_cfg.purpose

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

    effective_expose = rule.dataspace.expose if rule.dataspace else False

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

    entry: dict[str, Any] = {
        "title": title,
        "description": description,
        "backend_type": backend_type,
        "backend_config": {},
        "expose": effective_expose,
        "ontology_path": None,
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
                owners=file_owners,
            )

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
