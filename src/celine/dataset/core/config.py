# dataset/core/config.py
from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Literal, Optional

from pydantic import AnyUrl, HttpUrl, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "Dataset API"
    env: Literal["dev", "prod", "test"] = "dev"

    api_base_url: HttpUrl = HttpUrl("http://localhost:8000")
    catalog_uri: AnyUrl = HttpUrl("https://example.org/catalog")
    dataset_base_uri: AnyUrl = HttpUrl("https://example.org/dataset")

    database_url: str = (
        "postgresql+psycopg://postgres:securepassword123@172.17.0.1:5432/datasets"
    )
    catalogue_schema: str = "dataset_api"

    marquez_url: Optional[AnyUrl] = None
    marquez_namespace: Optional[str] = None

    api_url: str = Field(
        default="http://api.celine.localhost", description="CELINE API endpoint"
    )

    log_level: str = "INFO"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # =============================================================================
    # OIDC/JWT Settings - UPDATED for celine.sdk.auth
    # =============================================================================

    # OIDC issuer (base URL of your auth server)
    oidc_issuer: str = Field(
        default="http://keycloak.celine.localhost/realms/celine",
        description="OIDC issuer URL (e.g., https://auth.example.com/realms/celine)",
    )

    # JWKS URI for JWT signature verification
    # NEW: celine.sdk.auth.JwtUser uses this directly
    oidc_jwks_uri: str = Field(
        default="http://keycloak.celine.localhost/realms/celine/protocol/openid-connect/certs",
        description="JWKS URI for JWT verification (e.g., https://auth.example.com/realms/celine/protocol/openid-connect/certs)",
    )

    # Expected audience (optional - can validate multiple)
    oidc_audience: str | None = Field(
        default=None, description="Expected JWT audience (optional)"
    )

    # Client ID (for client-specific roles and audience validation)
    oidc_client_id: str | None = Field(default=None, description="OIDC client ID")

    # =============================================================================
    # Policy Settings - UPDATED for in-process evaluation
    # =============================================================================

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


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
