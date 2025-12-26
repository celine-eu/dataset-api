# dataset/core/config.py
from __future__ import annotations

from functools import lru_cache
from typing import Literal, Optional

from pydantic import AnyUrl, HttpUrl, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "Dataset API"
    env: Literal["dev", "prod", "test"] = "dev"

    api_base_url: HttpUrl = HttpUrl("http://localhost:8000")
    catalog_uri: AnyUrl = HttpUrl("https://example.org/catalog")
    dataset_base_uri: AnyUrl = HttpUrl("https://example.org/dataset")

    database_url: str = "postgresql+psycopg://postgres:postgres@localhost:5432/datasets"
    catalogue_schema: str = "dataset_api"

    marquez_url: Optional[AnyUrl] = None
    marquez_namespace: Optional[str] = None

    oidc_issuer: str = Field(
        default="http://keycloak.celine.localhost/realms/celine",
        description="OIDC url",
    )
    oidc_client_id: str = Field(
        default="celine-cli",
        description="OIDC client_id",
    )
    oidc_client_secret: str = Field(
        default="celine-cli",
        description="OIDC  client_secret",
    )
    oidc_audience: Optional[str] = Field(
        default="",
        description="OIDC token audience",
    )

    opa_enabled: bool = False
    opa_url: str = "http://localhost:8181"
    opa_policy_path: str = "celine/dataset/access"

    log_level: str = "INFO"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
