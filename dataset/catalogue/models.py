# dataset/catalogue/models.py
from __future__ import annotations

from typing import Optional, Dict, Any

from sqlalchemy import Boolean, Integer, JSON, String, Text
from sqlalchemy.orm import declarative_base, Mapped, mapped_column

from dataset.core.config import settings


Base = declarative_base()


class DatasetEntry(Base):
    __tablename__ = "datasets_entries"
    __table_args__ = {"schema": settings.catalogue_schema}

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    dataset_id: Mapped[str] = mapped_column(String(255), unique=True, index=True)

    title: Mapped[str] = mapped_column(String(512))
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    backend_type: Mapped[str] = mapped_column(String(64))
    backend_config: Mapped[Optional[Dict[str, Any]]] = mapped_column(
        JSON, nullable=True
    )

    ontology_path: Mapped[Optional[str]] = mapped_column(String(1024), nullable=True)
    schema_override_path: Mapped[Optional[str]] = mapped_column(
        String(1024), nullable=True
    )

    expose: Mapped[bool] = mapped_column(Boolean, default=False)
    tags: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)

    # Optional extra fields for DCAT-AP / provenance
    publisher_uri: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)
    rights_holder_uri: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)
    license_uri: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)

    landing_page: Mapped[Optional[str]] = mapped_column(String(1024), nullable=True)
    language_uris: Mapped[Optional[list[str]]] = mapped_column(JSON, nullable=True)
    spatial_uris: Mapped[Optional[list[str]]] = mapped_column(JSON, nullable=True)

    lineage: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)

    # NEW: coarse dataset access level (used together with OPA)
    # Suggested values: "open", "restricted", "internal"
    access_level: Mapped[Optional[str]] = mapped_column(
        String(32), nullable=True, default="open"
    )
