# dataset/core/config.py
from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Literal, Optional

import yaml
from pydantic import AnyUrl, HttpUrl, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from celine.sdk.settings.models import OidcSettings

logger = logging.getLogger(__name__)


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
        "postgresql+psycopg://postgres:securepassword123@host.docker.internal:15432/datasets"
    )
    datasets_database_url: Optional[str] = (
        "postgresql+psycopg://postgres:securepassword123@host.docker.internal:15432/datasets"
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

    # ---------------------------------------------------------------------------
    # Dataspace EDR PEP
    # ---------------------------------------------------------------------------

    owners_yaml_path: Path = Field(
        default=Path("./owners.yaml"),
        description=(
            "Path to owners.yaml registry. When found, owner aliases are resolved "
            "to canonical URIs and foaf:Agent nodes are inlined in DCAT-AP output. "
            "If the file does not exist a warning is logged and enrichment is skipped."
        ),
    )

    query_statement_timeout_ms: int = Field(
        default=5000,
        description="PostgreSQL statement_timeout for dataset queries (milliseconds)",
    )

    edr_enabled: bool = Field(
        default=False,
        description=(
            "Enable EDR token enforcement. When True, requests carrying the "
            "Edc-Contract-Agreement-Id header are treated as EDC data-plane "
            "proxy requests and validated against ds-connector before serving data."
        ),
    )
    connector_internal_url: Optional[str] = Field(
        default=None,
        description=(
            "Base URL of the ds-connector internal API "
            "(e.g. http://ds-connector:30001). Required when edr_enabled=True."
        ),
    )


# ---------------------------------------------------------------------------
# Lazy settings management
# ---------------------------------------------------------------------------

_settings_override: Settings | None = None
_settings_instance: Settings | None = None


def _load_yaml_config() -> dict:
    config_path = os.environ.get("DATASET_CONFIG")
    if config_path is not None:
        path = Path(config_path)
        if not path.exists():
            raise FileNotFoundError(
                f"DATASET_CONFIG points to missing file: {config_path}"
            )
    else:
        path = Path("./config.yaml")
        if not path.exists():
            return {}

    with open(path) as f:
        data = yaml.safe_load(f) or {}

    if not isinstance(data, dict):
        raise ValueError(f"Config file must be a YAML mapping, got {type(data).__name__}")

    logger.info("Loaded config from %s", path)
    return data


def get_settings() -> Settings:
    global _settings_instance
    if _settings_override is not None:
        return _settings_override
    if _settings_instance is not None:
        return _settings_instance
    yaml_data = _load_yaml_config()
    if yaml_data:
        # env vars take precedence over YAML values
        filtered = {
            k: v for k, v in yaml_data.items() if k.upper() not in os.environ
        }
        _settings_instance = Settings(**filtered)
    else:
        _settings_instance = Settings()
    return _settings_instance


def configure(settings_override: Settings) -> None:
    global _settings_override
    _settings_override = settings_override


def reset_settings() -> None:
    global _settings_override, _settings_instance
    _settings_override = None
    _settings_instance = None
