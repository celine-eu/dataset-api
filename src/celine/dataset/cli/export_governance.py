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
from celine.dataset.core.datasets import derive_physical_table

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


#: Findings that leave an exported entry wrong. `row-policy` is not one: the
#: entry is right, and the serving default refuses the dataset (GS-10).
_FATAL_RULES = frozenset({"parse", "schema", "handler", "layer-verdict", "consent-coherence", "input"})


def _check_governance(paths: list[str]) -> list[str]:
    """Run `celine.governance.check` over the matched files; echo every finding.

    Returns the fatal ones. Handlers are this service's own — built-ins and the
    plugins it loads — so a plugin a deployment registered is known here too.
    """
    from celine.governance.check import ERROR, check_governance

    from celine.dataset.api.dataset_query.row_filters import get_row_filter_registry

    report = check_governance(
        paths, handlers=list(get_row_filter_registry().handlers)
    )
    fatal: list[str] = []
    for f in report.findings:
        where = f"{f.source} [{f.dataset}]" if f.dataset else f.source
        is_fatal = f.severity == ERROR and f.rule in _FATAL_RULES
        label = "ERROR" if is_fatal else f.severity.upper()
        typer.echo(f"  {label} {f.rule} {where}: {f.detail}", err=is_fatal or f.severity != "info")
        if is_fatal:
            fatal.append(f"{where}: {f.detail}")
    return fatal


def resolve_rule(config: GovernanceConfig, dataset_name: str) -> GovernanceRule:
    """Resolve a dataset against a config: exact match → longest glob → defaults."""
    return GovernanceResolver(config).resolve(dataset_name)



# ---------------------------------------------------------------------------
# Mapping
# ---------------------------------------------------------------------------


def _normalize_dataset_id(dataset_name: str) -> str:
    return dataset_name.lower().replace("-", "_").replace(" ", "_")


def governance_rule_to_entry(
    dataset_name: str,
    rule: GovernanceRule,
    backend_type: str,
    owners: OwnersRegistry | None = None,
    base_dir: Path | None = None,
) -> dict[str, Any]:
    physical_table = derive_physical_table(dataset_name)
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
        # Strict, and fatal: a registry that does not validate resolves fewer
        # owners and the export still "succeeds" (celine-policies `sync-orgs`
        # model, celine-utils REQ-0014).
        try:
            _global_owners = load_owners_yaml(owners_path, validate=True)
        except Exception as exc:
            typer.echo(f"ERROR: owners registry {owners_path}: {exc}", err=True)
            raise typer.Exit(1)
        typer.echo(f"Loaded {len(_global_owners)} owner(s) from {owners_path}")

    # Sorted for a stable report and stable output names; de-duplicated because a
    # pattern such as `a/**/**/governance.yaml` matches the same path more than
    # once, and processing a file twice used to make it collide with itself.
    matched = sorted(dict.fromkeys(glob_module.glob(glob_pattern, recursive=True)))
    if not matched:
        typer.echo(f"No files matched pattern: {glob_pattern}", err=True)
        raise typer.Exit(1)

    typer.echo(f"Found {len(matched)} governance.yaml file(s)")

    # The governance checks, on the same files merged with the same overlays,
    # before anything is written. What would make an exported entry wrong stops
    # the export; a missing row-policy layer is reported only — the serving
    # instance's `ROW_FILTERS_DEFAULT` refuses that one dataset at query time
    # (GS-10), and the strict gate is `celine-governance validate` in the
    # deployment's own task.
    governance_errors = _check_governance(matched)
    if governance_errors:
        typer.echo(
            f"\n{len(governance_errors)} governance error(s); nothing was exported.",
            err=True,
        )
        raise typer.Exit(1)

    out_dir.mkdir(parents=True, exist_ok=True)

    total_datasets = 0
    ontology_errors: list[str] = []
    exposure_errors: list[str] = []
    load_errors: list[str] = []
    #: Output stems this run has already written — see the naming block below.
    written_stems: set[str] = set()

    for gov_path_str in matched:
        gov_path = Path(gov_path_str)
        logger.debug("Processing %s", gov_path)

        # Per-file owners registry: global flag > sibling owners.yaml > None
        file_owners = _global_owners
        if file_owners is None:
            sibling = gov_path.parent / "owners.yaml"
            if sibling.is_file():
                try:
                    file_owners = load_owners_yaml(sibling, validate=True)
                    logger.debug("Using sibling owners.yaml at %s", sibling)
                except Exception as exc:
                    typer.echo(f"  ERROR owners registry {sibling}: {exc}", err=True)
                    load_errors.append(f"{sibling}: {exc}")
                    continue

        try:
            config = load_governance_with_override(gov_path)
        except Exception as exc:
            # Fatal at the end: a file that does not load used to be skipped
            # with the run exiting 0, and the catalogue lost its datasets.
            typer.echo(f"  ERROR loading {gov_path}: {exc}", err=True)
            load_errors.append(f"{gov_path}: {exc}")
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

        # **Collide against this run, not against the directory.**
        #
        # The suffix disambiguates two apps whose directories share a name, and
        # that is the only thing it should do. Keyed on `out_file.exists()` it
        # also fired on the output of a *previous* run — so a re-export never
        # replaced its own file, it wrote a second one beside it. Where the
        # caller stages the sources in `mktemp -d`, the grandparent is a new
        # `tmp.XXXXXXXX` each time, so every run minted a fresh generation of
        # names and the directory grew without bound. One deployment reached 100
        # files for 21 apps that way, and the importer then merged all five
        # generations into one catalogue.
        #
        # Re-exporting the same app must overwrite: the file is derived output,
        # and the new content is the current answer.
        if stem in written_stems:
            stem = f"{gov_path.parent.parent.name}__{stem}"
            out_file = out_dir / f"{stem}.yaml"
        written_stems.add(stem)

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

    if load_errors:
        typer.echo(
            f"\n{len(load_errors)} file(s) could not be loaded and were omitted from "
            "the export.",
            err=True,
        )

    # One exit for all. Keeping it inside the ontology branch would have let an
    # export that dropped datasets for exposure conflicts still return 0, and
    # this command runs unattended in `task import:dataset-sync` — a zero exit
    # there means the catalogue was synced, not that some of it was skipped.
    if exposure_errors or ontology_errors or load_errors:
        raise typer.Exit(1)
