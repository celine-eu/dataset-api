# dataset/db/models.py
from __future__ import annotations

from typing import Any, Dict, Optional

from sqlalchemy import Boolean, Integer, JSON, String, Text, text
from sqlalchemy.orm import Mapped, declarative_base, mapped_column

from celine.dataset.core.config import get_settings

Base = declarative_base()


class DatasetEntry(Base):
    __tablename__ = "datasets_entries"
    __table_args__ = {"schema": get_settings().catalogue_schema}

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    dataset_id: Mapped[str] = mapped_column(String(255), unique=True, index=True)

    title: Mapped[str] = mapped_column(String(512))
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    backend_type: Mapped[str] = mapped_column(String(64))
    backend_config: Mapped[Optional[Dict[str, Any]]] = mapped_column(
        JSON, nullable=True
    )

    # What was declared in governance.yaml — a shared spec name, or a path
    # relative to that file. Kept for provenance: it traces the binding back to
    # the governance file that made it.
    ontology_path: Mapped[Optional[str]] = mapped_column(String(1024), nullable=True)
    # The resolved mapping spec itself, materialized at import. A `spec_file`
    # lives in the pipelines checkout, which the API does not have at request
    # time — so the content travels with the entry rather than the reference.
    ontology_mapping: Mapped[Optional[Dict[str, Any]]] = mapped_column(
        JSON, nullable=True
    )
    schema_override_path: Mapped[Optional[str]] = mapped_column(
        String(1024), nullable=True
    )

    # Listed in the catalogue and served by the API: gates /catalogue* and /query.
    expose: Mapped[bool] = mapped_column(Boolean, default=False)

    # Offered into the dataspace: gates requests arriving with EDR context.
    #
    # Separate from `expose` because they answer different questions, and one
    # boolean used to answer both — the exporter copied `dataspace.expose`
    # straight onto `expose`, so a dataset that had to appear in the catalogue
    # was thereby offered to the dataspace, and one withheld from the dataspace
    # was also unqueryable through the API.
    #
    # Defaults to False, including for every row that predates this column. That
    # is deliberate: the values it would otherwise inherit were written when the
    # flag only controlled the catalogue, so treating them as dataspace consent
    # would offer 60 datasets — 13 of them NonCommercial-licensed and 33 with no
    # declared licence — on the strength of a statement that meant something else.
    dataspace_expose: Mapped[bool] = mapped_column(
        Boolean, default=False, server_default=text("false"), nullable=False
    )

    tags: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)

    # Optional extra fields for DCAT-AP / provenance
    publisher_uri: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)
    rights_holder_uri: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)
    license_uri: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)

    landing_page: Mapped[Optional[str]] = mapped_column(String(1024), nullable=True)
    language_uris: Mapped[Optional[list[str]]] = mapped_column(JSON, nullable=True)
    spatial_uris: Mapped[Optional[list[str]]] = mapped_column(JSON, nullable=True)

    lineage: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)

    # Coarse dataset access level (used together with OPA)
    # Suggested values: "open", "restricted", "internal"
    access_level: Mapped[Optional[str]] = mapped_column(
        String(32), nullable=True, default="internal"
    )
