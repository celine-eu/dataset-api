# dataset/cli/export_postgres.py
"""
CLI command to export PostgreSQL schema/tables to OpenLineage-compatible YAML.

This command introspects a PostgreSQL database and generates YAML files that are
compatible with the `import catalogue` command. Since governance metadata is not
available in the database schema, safe defaults are provided for manual curation.
"""
from __future__ import annotations

import fnmatch
import logging
import warnings
from pathlib import Path
from typing import Any, List, Optional

import typer
import yaml
from sqlalchemy import MetaData, create_engine, inspect, text
from sqlalchemy.engine import Engine

from celine.dataset.cli.utils import setup_cli_logging, write_yaml_file
from celine.dataset.core.config import settings

# Suppress SQLAlchemy warnings for unrecognized types (e.g., PostGIS geography/geometry)
warnings.filterwarnings(
    "ignore",
    message="Did not recognize type",
    category=Warning,
)

logger = logging.getLogger(__name__)

# Known PostGIS/geospatial types that we handle specially
GEOSPATIAL_TYPES = frozenset(
    {
        "geography",
        "geometry",
        "point",
        "linestring",
        "polygon",
        "multipoint",
        "multilinestring",
        "multipolygon",
        "geometrycollection",
        "raster",
    }
)

# Default values for governance fields that must be curated by hand
GOVERNANCE_DEFAULTS = {
    "access_level": "internal",  # Safe default: not open, requires auth
    "classification": "yellow",  # Safe default: requires review
    "license": None,  # Must be filled in
    "attribution": None,  # Must be filled in if required by license
    "access_requirements": "partner",  # Safe default
    "retention_days": 365,  # Common default
}


def _get_engine(database_url: str) -> Engine:
    """Create SQLAlchemy engine from connection URL."""
    return create_engine(database_url)


def _list_schemas(engine: Engine, include_system: bool = False) -> List[str]:
    """List available schemas in the database."""
    inspector = inspect(engine)
    schemas = inspector.get_schema_names()

    if not include_system:
        # Exclude common system schemas
        system_schemas = {"pg_catalog", "pg_toast", "information_schema"}
        schemas = [s for s in schemas if s not in system_schemas]

    return sorted(schemas)


def _list_tables(
    engine: Engine,
    schema: str,
    include_views: bool = True,
) -> List[dict]:
    """
    List tables and optionally views in a schema.

    Returns list of dicts with:
    - name: table/view name
    - type: 'table' or 'view'
    - schema: schema name
    """
    inspector = inspect(engine)

    tables = []

    # Get tables
    for table_name in inspector.get_table_names(schema=schema):
        tables.append(
            {
                "name": table_name,
                "type": "table",
                "schema": schema,
            }
        )

    # Get views
    if include_views:
        for view_name in inspector.get_view_names(schema=schema):
            tables.append(
                {
                    "name": view_name,
                    "type": "view",
                    "schema": schema,
                }
            )

    return sorted(tables, key=lambda x: x["name"])


def _get_table_columns(
    engine: Engine,
    schema: str,
    table_name: str,
) -> List[dict]:
    """
    Get column information for a table.

    Handles unknown types (e.g., PostGIS geography/geometry) by falling back
    to direct PostgreSQL catalog queries for accurate type names.
    """
    inspector = inspect(engine)

    # First, get raw column info from pg_catalog for accurate types
    # This handles PostGIS and other extension types that SQLAlchemy doesn't recognize
    raw_types = {}
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text(
                    """
                    SELECT 
                        a.attname AS column_name,
                        pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
                        NOT a.attnotnull AS is_nullable,
                        pg_get_expr(d.adbin, d.adrelid) AS column_default
                    FROM pg_catalog.pg_attribute a
                    LEFT JOIN pg_catalog.pg_attrdef d ON (a.attrelid = d.adrelid AND a.attnum = d.adnum)
                    WHERE a.attrelid = (
                        SELECT c.oid 
                        FROM pg_catalog.pg_class c
                        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                        WHERE n.nspname = :schema AND c.relname = :table
                    )
                    AND a.attnum > 0 
                    AND NOT a.attisdropped
                    ORDER BY a.attnum
                """
                ),
                {"schema": schema, "table": table_name},
            )
            for row in result:
                raw_types[row[0]] = {
                    "type": row[1],
                    "nullable": row[2],
                    "default": row[3],
                }
    except Exception as exc:
        logger.debug(
            "Could not query pg_catalog for %s.%s: %s", schema, table_name, exc
        )

    # Get columns via SQLAlchemy inspector
    try:
        columns = inspector.get_columns(table_name, schema=schema)
    except Exception as exc:
        logger.warning("Failed to get columns for %s.%s: %s", schema, table_name, exc)
        # Fall back to raw_types only if available
        if raw_types:
            return [
                {
                    "name": name,
                    "type": info["type"],
                    "nullable": info["nullable"],
                    "default": info["default"],
                    "is_geospatial": any(
                        geo in info["type"].lower() for geo in GEOSPATIAL_TYPES
                    ),
                }
                for name, info in raw_types.items()
            ]
        return []

    result = []
    for col in columns:
        col_name = col["name"]

        # Get type - prefer raw PostgreSQL type if SQLAlchemy returned NullType
        col_type = col.get("type")
        type_str = str(col_type) if col_type is not None else "UNKNOWN"

        # If SQLAlchemy returned NullType, use the raw type from pg_catalog
        if "NullType" in type_str or type_str == "NULL":
            type_str = raw_types.get(col_name, {}).get("type", type_str)

        # Check if this is a geospatial type
        type_lower = type_str.lower()
        is_geospatial = any(geo in type_lower for geo in GEOSPATIAL_TYPES)

        result.append(
            {
                "name": col_name,
                "type": type_str,
                "nullable": col.get("nullable", True),
                "default": str(col["default"]) if col.get("default") else None,
                "is_geospatial": is_geospatial,
            }
        )

    return result


def _get_table_comment(engine: Engine, schema: str, table_name: str) -> Optional[str]:
    """Get table comment/description if available."""
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text(
                    """
                    SELECT obj_description(
                        (quote_ident(:schema) || '.' || quote_ident(:table))::regclass,
                        'pg_class'
                    )
                """
                ),
                {"schema": schema, "table": table_name},
            )
            row = result.fetchone()
            return row[0] if row and row[0] else None
    except Exception as exc:
        logger.debug("Failed to get comment for %s.%s: %s", schema, table_name, exc)
        return None


def _get_primary_keys(engine: Engine, schema: str, table_name: str) -> List[str]:
    """Get primary key columns for a table."""
    inspector = inspect(engine)
    try:
        pk = inspector.get_pk_constraint(table_name, schema=schema)
        return pk.get("constrained_columns", []) if pk else []
    except Exception:
        return []


def _normalize_dataset_id(schema: str, table_name: str, namespace: str) -> str:
    """
    Generate a dataset_id from schema and table name.

    Format: {namespace}.{schema}.{table_name}

    If namespace equals schema, omits the duplicate to avoid IDs like:
    ds_dev_gold.ds_dev_gold.table -> ds_dev_gold.table
    """
    # Normalize to lowercase and replace special chars
    clean_schema = schema.lower().replace("-", "_").replace(" ", "_")
    clean_table = table_name.lower().replace("-", "_").replace(" ", "_")
    clean_namespace = namespace.lower().replace("-", "_").replace(" ", "_")

    # Avoid duplication when namespace == schema
    if clean_namespace == clean_schema:
        return f"{clean_namespace}.{clean_table}"

    return f"{clean_namespace}.{clean_schema}.{clean_table}"


def _build_dataset_entry(
    engine: Engine,
    schema: str,
    table_info: dict,
    namespace: str,
    expose: bool,
    backend_type: str = "postgres",
) -> dict[str, Any]:
    """
    Build a dataset entry for a single table/view.

    Includes safe governance defaults that should be curated before import.
    """
    table_name = table_info["name"]
    table_type = table_info["type"]

    # Get metadata from database
    comment = _get_table_comment(engine, schema, table_name)
    columns = _get_table_columns(engine, schema, table_name)
    primary_keys = _get_primary_keys(engine, schema, table_name)

    # Physical table reference (schema-qualified)
    physical_table = f"{schema}.{table_name}"

    # Build description
    if comment:
        description = comment
    else:
        description = f"{table_type} '{physical_table}'"

    # Build lineage info (mimics OpenLineage export structure)
    lineage = {
        "namespace": namespace,
        "name": physical_table,
        "sourceName": "postgres",
        "facets": {
            "schema": {
                "_producer": "dataset-cli/export-postgres",
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/SchemaDatasetFacet.json",
                "fields": [
                    {
                        "name": col["name"],
                        "type": col["type"],
                        "description": None,  # Would need column comments
                    }
                    for col in columns
                ],
            },
            # Governance facet with safe defaults (to be edited)
            "governance": {
                "_producer": "dataset-cli/export-postgres",
                "_schemaURL": "file:///GovernanceDatasetFacet.schema.json",
                "title": None,  # TODO: to be filled
                "description": None,  # TODO: to be filled
                "accessLevel": GOVERNANCE_DEFAULTS["access_level"],
                "classification": GOVERNANCE_DEFAULTS["classification"],
                "license": GOVERNANCE_DEFAULTS["license"],
                "attribution": GOVERNANCE_DEFAULTS["attribution"],
                "access_requirements": GOVERNANCE_DEFAULTS["access_requirements"],
                "retentionDays": GOVERNANCE_DEFAULTS["retention_days"],
                "sourceSystem": "postgres",
            },
        },
    }

    # Build tags
    tags = {
        "keywords": [
            f"schema:{schema}",
            f"type:{table_type}",
        ],
    }

    if primary_keys:
        tags["keywords"].append("has_pk")

    # Check for geospatial columns
    if any(col.get("is_geospatial") for col in columns):
        tags["keywords"].append("has_geospatial")

    # Add governance-related tags for filtering
    tags["keywords"].append(f"classification:{GOVERNANCE_DEFAULTS['classification']}")
    tags["accessRights"] = GOVERNANCE_DEFAULTS["access_level"]

    entry: dict[str, Any] = {
        "title": f"{physical_table}",
        "description": description,
        "backend_type": backend_type,
        "backend_config": {
            "table": physical_table,
        },
        "expose": expose,
        "ontology_path": None,
        "schema_override_path": None,
        "tags": tags,
        "lineage": lineage,
        "access_level": GOVERNANCE_DEFAULTS["access_level"],
        # DCAT fields - safe defaults
        "publisher_uri": None,  # TODO: to be filled
        "rights_holder_uri": None,  # TODO: to be filled
        "license_uri": GOVERNANCE_DEFAULTS["license"],
        "landing_page": None,
        "language_uris": None,
        "spatial_uris": None,
    }

    return entry


def _filter_tables(
    tables: List[dict],
    include_patterns: List[str],
    exclude_patterns: List[str],
) -> List[dict]:
    """
    Filter tables by include/exclude glob patterns.

    Patterns match against "{schema}.{table_name}".
    """
    if not include_patterns and not exclude_patterns:
        return tables

    result = []

    for table in tables:
        full_name = f"{table['schema']}.{table['name']}"

        # Check includes
        if include_patterns:
            included = any(fnmatch.fnmatchcase(full_name, p) for p in include_patterns)
            if not included:
                continue

        # Check excludes
        if exclude_patterns:
            excluded = any(fnmatch.fnmatchcase(full_name, p) for p in exclude_patterns)
            if excluded:
                continue

        result.append(table)

    return result


def _resolve_table_filters(filters: List[str]) -> tuple[List[str], List[str]]:
    """
    Parse filter patterns into include and exclude lists.

    Supports:
    - +pattern: include
    - -pattern: exclude
    - pattern: include (default)
    """
    includes = []
    excludes = []

    for f in filters:
        if f.startswith("-"):
            excludes.append(f[1:])
        elif f.startswith("+"):
            includes.append(f[1:])
        else:
            includes.append(f)

    return includes, excludes


# Register as subcommand under export_app
# This will be added to the export typer app


def export_postgres_cmd(
    out_dir: Path = typer.Option(
        ..., "-o", "--output", help="Output directory for YAML files."
    ),
    schemas: List[str] = typer.Option(
        None,
        "--schema",
        "-s",
        help=(
            "Schemas to export (can be passed multiple times). "
            "If not specified, exports all non-system schemas."
        ),
    ),
    tables_filter: List[str] = typer.Option(
        None,
        "--tables",
        "-t",
        help=(
            "Table filter using glob patterns (e.g., 'public.*', '-*.tmp_*'). "
            "Patterns match '{schema}.{table}'. Can be passed multiple times."
        ),
    ),
    namespace: str = typer.Option(
        "postgres",
        "--namespace",
        "-n",
        help="Namespace prefix for dataset IDs (e.g., 'gold', 'silver', 'raw').",
    ),
    database_url: Optional[str] = typer.Option(
        settings.database_url,
        "--database-url",
        envvar="DATABASE_URL",
        help="PostgreSQL connection URL. Defaults to settings.database_url.",
    ),
    include_views: bool = typer.Option(
        True, "--views/--no-views", help="Include views in export."
    ),
    expose: bool = typer.Option(
        False,
        "--expose",
        help="Mark exported datasets as exposed (visible in catalogue).",
    ),
    one_file_per_schema: bool = typer.Option(
        True,
        "--one-file-per-schema/--single-file",
        help="Create one YAML file per schema (default) or a single combined file.",
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Print what would be exported without writing files."
    ),
):
    """
    Export PostgreSQL tables/views to OpenLineage-compatible YAML.
    
    The output is compatible with `dataset-cli import catalogue` command.
    
    Since governance metadata is not available from the database schema,
    safe defaults are provided. The YAML should be edited before import
    to fill in:
    
    \b
    - title: Human-readable dataset title
    - description: Dataset description
    - access_level: open | internal | restricted
    - classification: pii | red | yellow | green
    - license_uri: License URL
    - rights_holder_uri: Data owner/team
    
    Examples:
    
    \b
    # Export all tables from default database
    dataset-cli export postgres -o ./catalogue
    
    \b
    # Export specific schemas
    dataset-cli export postgres -o ./catalogue --schema public --schema analytics
    
    \b
    # Export with filters
    dataset-cli export postgres -o ./catalogue \\
        --tables 'public.*' \\
        --tables '-public.tmp_*' \\
        --tables '-public.*_backup'
    
    \b
    # Export as 'gold' namespace
    dataset-cli export postgres -o ./catalogue --namespace gold --expose
    """
    setup_cli_logging(verbose)

    # Resolve database URL
    db_url = database_url or settings.database_url
    if not db_url:
        typer.echo("Error: No database URL configured.", err=True)
        raise typer.Exit(code=1)

    # Mask password in output
    safe_url = db_url
    if "@" in db_url:
        parts = db_url.split("@")
        safe_url = parts[0].rsplit(":", 1)[0] + ":***@" + parts[1]

    typer.echo(f"Connecting to database: {safe_url}")

    try:
        engine = _get_engine(db_url)
    except Exception as exc:
        typer.echo(f"Failed to connect to database: {exc}", err=True)
        raise typer.Exit(code=1)

    # Resolve schemas
    if schemas:
        selected_schemas = list(schemas)
    else:
        selected_schemas = _list_schemas(engine)

    typer.echo(f"Selected schemas: {selected_schemas}")

    # Parse table filters
    include_patterns, exclude_patterns = [], []
    if tables_filter:
        include_patterns, exclude_patterns = _resolve_table_filters(tables_filter)
        if include_patterns:
            typer.echo(f"Include patterns: {include_patterns}")
        if exclude_patterns:
            typer.echo(f"Exclude patterns: {exclude_patterns}")

    # Collect all tables
    all_tables: dict[str, List[dict]] = {}

    for schema in selected_schemas:
        tables = _list_tables(engine, schema, include_views=include_views)

        # Apply filters
        tables = _filter_tables(tables, include_patterns, exclude_patterns)

        if tables:
            all_tables[schema] = tables
            typer.echo(f"  {schema}: {len(tables)} tables/views")

    if not all_tables:
        typer.echo("No tables found matching the filters.", err=True)
        raise typer.Exit(code=1)

    total_count = sum(len(t) for t in all_tables.values())
    typer.echo(f"Total: {total_count} tables/views to export")

    if dry_run:
        typer.echo("\nDry run - tables that would be exported:")
        for schema, tables in sorted(all_tables.items()):
            for table in tables:
                dataset_id = _normalize_dataset_id(schema, table["name"], namespace)
                typer.echo(f"  {dataset_id} ({table['type']})")
        raise typer.Exit(code=0)

    # Build dataset entries
    out_dir.mkdir(parents=True, exist_ok=True)

    if one_file_per_schema:
        # One file per schema
        for schema, tables in all_tables.items():
            datasets = {}

            for table in tables:
                dataset_id = _normalize_dataset_id(schema, table["name"], namespace)
                entry = _build_dataset_entry(
                    engine=engine,
                    schema=schema,
                    table_info=table,
                    namespace=namespace,
                    expose=expose,
                )
                datasets[dataset_id] = entry

            output = {"datasets": datasets}
            outfile = out_dir / f"{namespace}.{schema}.yaml"
            write_yaml_file(outfile, output)
            typer.echo(f"Wrote {len(datasets)} datasets → {outfile}")
    else:
        # Single combined file
        datasets = {}

        for schema, tables in all_tables.items():
            for table in tables:
                dataset_id = _normalize_dataset_id(schema, table["name"], namespace)
                entry = _build_dataset_entry(
                    engine=engine,
                    schema=schema,
                    table_info=table,
                    namespace=namespace,
                    expose=expose,
                )
                datasets[dataset_id] = entry

        output = {"datasets": datasets}
        outfile = out_dir / f"{namespace}.yaml"
        write_yaml_file(outfile, output)
        typer.echo(f"Wrote {len(datasets)} datasets → {outfile}")

    typer.echo("\n⚠️  IMPORTANT: The exported YAML contains governance defaults.")
    typer.echo("   Please review and edit the following fields before import:")
    typer.echo("   - title")
    typer.echo("   - description")
    typer.echo("   - access_level (currently: internal)")
    typer.echo("   - classification (currently: yellow)")
    typer.echo("   - license_uri")
    typer.echo("   - rights_holder_uri")


# For integration with the existing CLI
if __name__ == "__main__":
    # Standalone testing
    app = typer.Typer()
    app.command("postgres")(export_postgres_cmd)
    app()
