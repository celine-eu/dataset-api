# dataset/schemas/dataset_metadata.py
from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class ColumnSchema(BaseModel):
    """Summary of a single column, aligned with JSON Schema concepts."""

    name: str
    type: str
    nullable: bool
    primary_key: bool = False
    db_type: str = Field(..., description="Database type as reported by SQLAlchemy")
    description: Optional[str] = None


class TableSchemaSummary(BaseModel):
    """Flat summary of the table columns."""

    columns: List[ColumnSchema] = []


class GovernanceInfo(BaseModel):
    """Flattened governance view combining top-level fields and raw facet."""

    license_uri: Optional[str] = None
    rights_holder_uri: Optional[str] = None
    access_level: Optional[str] = None
    access_rights: Optional[str] = None
    tags: Optional[List[str]] = None
    owners: Optional[List[str]] = None
    classification: Optional[str] = None
    raw_facet: Optional[Dict[str, Any]] = None


class DatasetMetadata(BaseModel):
    """
    Full dataset metadata including:
    - basic catalogue info
    - governance fields
    - lineage
    - JSON Schema–style table definition
    """

    dataset_id: str
    title: str
    description: Optional[str] = None

    backend_type: str
    backend_table: Optional[str] = None

    governance: Optional[GovernanceInfo] = None
    tags: Optional[Dict[str, Any]] = None
    lineage: Optional[Dict[str, Any]] = None

    # JSON Schema–style description of the physical table
    json_schema: Dict[str, Any]

    # Easier-to-consume flat summary of columns
    schema_summary: TableSchemaSummary
