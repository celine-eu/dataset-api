# dataset/core/config.py
from __future__ import annotations

from functools import lru_cache
from typing import Literal, Optional

from pydantic import AnyUrl, HttpUrl
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

    keycloak_issuer: Optional[AnyUrl] = None
    keycloak_audience: Optional[str] = None
    keycloak_client_id: Optional[str] = None
    keycloak_client_secret: Optional[str] = None
    keycloak_callback_uri: Optional[HttpUrl] = HttpUrl("http://localhost/callback")
    keycloak_admin_client_secret: Optional[str] = None

    opa_url: Optional[AnyUrl] = None
    opa_dataset_policy_path: str = "dataset/access"

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
