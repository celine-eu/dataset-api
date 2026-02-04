# dataset/schemas/catalogue_import.py
from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator


class BackendConfig(BaseModel):
    """Backend configuration block for dataset storage."""

    table: Optional[str] = None
    path: Optional[str] = None
    format: Optional[str] = None
    public_url: Optional[str] = None
    size_bytes: Optional[int] = None


class ContactPoint(BaseModel):
    fn: Optional[str] = None
    email: Optional[str] = None


class TemporalCoverage(BaseModel):
    start: Optional[str] = None
    end: Optional[str] = None


class Tags(BaseModel):
    keywords: Optional[List[str]] = None
    themes: Optional[List[str]] = None
    accrualPeriodicity: Optional[str] = None
    accessRights: Optional[str] = None
    temporal: Optional[TemporalCoverage] = None
    contactPoint: Optional[ContactPoint] = None
    identifier: Optional[str] = None
    conformsTo: Optional[str] = None


class Lineage(BaseModel):
    title: Optional[str] = None
    description: Optional[str] = None
    namespace: Optional[str] = None
    name: Optional[str] = None
    sourceName: Optional[str] = None
    createdAt: Optional[str] = None
    updatedAt: Optional[str] = None
    lastModifiedAt: Optional[str] = None
    lastLifecycleState: Optional[str] = None
    tags: Optional[List[str]] = None
    facets: Optional[Dict[str, Any]] = None

    @field_validator("namespace")
    def namespace_not_empty(cls, v):
        return v or "default"

    model_config = ConfigDict(extra="allow")


class DatasetEntryModel(BaseModel):
    dataset_id: str
    title: str
    description: Optional[str] = None

    backend_type: str
    backend_config: BackendConfig = Field(default_factory=BackendConfig)

    expose: bool = False
    ontology_path: Optional[str] = None
    schema_override_path: Optional[str] = None

    tags: Optional[Tags] = None
    lineage: Optional[Lineage] = None

    publisher_uri: Optional[str] = None
    rights_holder_uri: Optional[str] = None
    license_uri: Optional[str] = None

    landing_page: Optional[str] = None
    language_uris: Optional[List[str]] = None
    spatial_uris: Optional[List[str]] = None

    access_level: Optional[str] = None

    @field_validator("backend_type")
    def check_backend_type(cls, v):
        allowed = {"postgres", "s3", "fs"}
        if v not in allowed:
            raise ValueError(f"backend_type must be one of {allowed}, got '{v}'")
        return v


class CatalogueImportModel(BaseModel):
    """Full catalogue import: list of datasets."""

    datasets: List[DatasetEntryModel]
