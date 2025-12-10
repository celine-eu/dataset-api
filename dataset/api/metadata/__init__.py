# dataset/api/metadata/builder.py
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import Table

from dataset.db.models.dataset_entry import DatasetEntry
from dataset.schemas.dataset_metadata import (
    ColumnSchema,
    DatasetMetadata,
    GovernanceInfo,
    TableSchemaSummary,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Type mapping helpers
# ---------------------------------------------------------------------------


def _sqlalchemy_type_to_json_type(col_type: Any) -> Tuple[str, Optional[str]]:
    """
    Very small, pragmatic mapping from SQLAlchemy column types to JSON Schema
    'type' + 'format'.

    Returns: (json_type, json_format)
    """
    type_name = col_type.__class__.__name__.lower()

    # Integers
    if "integer" in type_name or type_name in {"smallint", "bigint"}:
        return "integer", None

    # Numeric / float
    if "numeric" in type_name or "float" in type_name or "double" in type_name:
        return "number", None

    # Boolean
    if "bool" in type_name:
        return "boolean", None

    # Date / time
    if "datetime" in type_name or "timestamp" in type_name:
        return "string", "date-time"
    if "date" in type_name:
        return "string", "date"
    if "time" in type_name:
        return "string", "time"

    # JSON / dict-like
    if "json" in type_name:
        return "object", None

    # Geometry (GeoAlchemy2)
    if "geometry" in type_name:
        return "object", "geojson"

    # Default: string
    return "string", None


# ---------------------------------------------------------------------------
# Governance
# ---------------------------------------------------------------------------


def _build_governance(entry: DatasetEntry) -> Optional[GovernanceInfo]:
    tags_dict: Dict[str, Any] = entry.tags or {}
    keywords: List[str] = list(tags_dict.get("keywords") or [])
    access_rights: Optional[str] = tags_dict.get("accessRights")

    lineage = entry.lineage or {}
    facets = lineage.get("facets") or {}
    raw_gov = facets.get("governance")

    owners: Optional[List[str]] = None
    classification: Optional[str] = None
    facet_tags: Optional[List[str]] = None

    if isinstance(raw_gov, dict):
        owners = raw_gov.get("owners")
        classification = raw_gov.get("classification")
        facet_tags = raw_gov.get("tags")

    # Merge facet tags into keywords, but keep keywords as primary
    all_tags: Optional[List[str]] = None
    if keywords or facet_tags:
        merged = list(dict.fromkeys((keywords or []) + (facet_tags or [])))
        all_tags = merged

    # If nothing meaningful is present, we can still return a minimal object,
    # but this guard avoids completely empty payloads.
    if (
        entry.license_uri is None
        and entry.rights_holder_uri is None
        and entry.access_level is None
        and access_rights is None
        and not all_tags
        and not owners
        and not classification
        and not isinstance(raw_gov, dict)
    ):
        return None

    return GovernanceInfo(
        license_uri=entry.license_uri,
        rights_holder_uri=entry.rights_holder_uri,
        access_level=entry.access_level,
        access_rights=access_rights,
        tags=all_tags,
        owners=owners,
        classification=classification,
        raw_facet=raw_gov if isinstance(raw_gov, dict) else None,
    )


# ---------------------------------------------------------------------------
# JSON Schema builder
# ---------------------------------------------------------------------------


def _build_json_schema_for_table(
    entry: DatasetEntry,
    table: Optional[Table],
) -> Tuple[Dict[str, Any], TableSchemaSummary]:
    """
    Build a JSON Schema–like document and a flat summary from a reflected table.

    If table is None (e.g. non-postgres backend), an empty schema is returned.
    """
    properties: Dict[str, Any] = {}
    required: List[str] = []
    column_summaries: List[ColumnSchema] = []

    if table is not None:
        for col in table.columns:
            json_type, json_format = _sqlalchemy_type_to_json_type(col.type)
            col_schema: Dict[str, Any] = {
                "type": json_type,
                "nullable": col.nullable,
                "x-db-type": str(col.type),
                "x-primary-key": bool(col.primary_key),
            }
            if json_format:
                col_schema["format"] = json_format

            properties[col.name] = col_schema

            if not col.nullable:
                required.append(col.name)

            column_summaries.append(
                ColumnSchema(
                    name=col.name,
                    type=json_type,
                    nullable=col.nullable,
                    primary_key=bool(col.primary_key),
                    db_type=str(col.type),
                    description=None,  # can be extended later from DB comments
                )
            )
    else:
        logger.info(
            "No physical table available for dataset '%s', "
            "returning empty properties in JSON Schema.",
            entry.dataset_id,
        )

    json_schema: Dict[str, Any] = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": entry.dataset_id,
        "description": entry.description,
        "type": "object",
        "properties": properties,
        "required": required,
        "additionalProperties": False,
    }

    schema_summary = TableSchemaSummary(columns=column_summaries)
    return json_schema, schema_summary


# ---------------------------------------------------------------------------
# Public builder
# ---------------------------------------------------------------------------


def build_metadata_document(
    entry: DatasetEntry,
    table: Optional[Table],
) -> DatasetMetadata:
    """
    Compose full DatasetMetadata instance from a DatasetEntry and an optional
    reflected Table.
    """
    governance = _build_governance(entry)
    json_schema, schema_summary = _build_json_schema_for_table(entry, table)

    backend_table: Optional[str] = None
    if isinstance(entry.backend_config, dict):
        backend_table = entry.backend_config.get("table")

    return DatasetMetadata(
        dataset_id=entry.dataset_id,
        title=entry.title,
        description=entry.description,
        backend_type=entry.backend_type,
        backend_table=backend_table,
        governance=governance,
        tags=entry.tags or {},
        lineage=entry.lineage or {},
        json_schema=json_schema,
        schema_summary=schema_summary,
    )
