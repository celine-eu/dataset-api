# dataset/core/config.py
from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Literal, Optional

from pydantic import AnyUrl, HttpUrl, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from celine.sdk.settings.models import OidcSettings


class Settings(BaseSettings):

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    app_name: str = "Dataset API"
    env: Literal["dev", "prod", "test"] = "dev"

    api_base_url: HttpUrl = HttpUrl("http://api.celine.localhost/datasets")
    catalog_uri: AnyUrl = HttpUrl("http://api.celine.localhost/datasets/catalog")
    dataset_base_uri: AnyUrl = HttpUrl("http://api.celine.localhost/datasets/dataset")

    database_url: str = (
        "postgresql+psycopg://postgres:securepassword123@172.17.0.1:15432/datasets"
    )
    catalogue_schema: str = "dataset_api"

    marquez_url: Optional[AnyUrl] = None
    marquez_namespace: Optional[str] = None

    api_url: str = Field(
        default="http://api.celine.localhost", description="CELINE API endpoint"
    )

    log_level: str = "INFO"

    oidc: OidcSettings = OidcSettings(audience="svc-dataset-api")

    # Policy Settings
    policies_check_enabled: bool = Field(
        default=True, description="Enable policy-based authorization"
    )

    # Policy engine settings (replaces api_url + "/policies")
    policies_dir: Path = Field(
        default=Path("./policies"),
        description="Directory containing .rego policy files",
    )
    policies_data_dir: Path | None = Field(
        default=None, description="Optional directory containing policy data JSON files"
    )

    # Policy package to evaluate
    policies_package: str = Field(
        default="celine.dataset",
        description="Policy package to evaluate for dataset authorization",
    )

    # Cache settings
    policies_cache_enabled: bool = Field(
        default=True, description="Enable in-memory decision caching"
    )
    policies_cache_ttl: int = Field(default=300, description="Cache TTL in seconds")
    policies_cache_maxsize: int = Field(
        default=10000, description="Maximum cache entries"
    )

    # =============================================================================
    # Row filter handlers
    # =============================================================================

    row_filters_modules: list[str] = Field(
        default_factory=list,
        description="Optional list of python modules to import to register row filter handlers",
    )
    row_filters_cache_ttl: int = Field(
        default=300, description="Row filter resolution cache TTL upper bound (seconds)"
    )
    row_filters_cache_maxsize: int = Field(
        default=10000, description="Row filter resolution cache max entries"
    )

    rec_registry_url: str = Field(
        default="http://api.celine.localhost/rec-registry",
        description="REC Registry URL",
    )


settings = Settings()
