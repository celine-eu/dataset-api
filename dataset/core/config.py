# datasets/core/config.py
from __future__ import annotations

from functools import lru_cache
from typing import Literal, Optional

from pydantic import HttpUrl, AnyUrl
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "Dataset API"
    env: Literal["dev", "prod", "test"] = "dev"

    # Provide safe defaults so Pylance stops complaining,
    # and still allow .env to override everything.
    api_base_url: HttpUrl = HttpUrl("http://localhost:8000")
    catalog_uri: AnyUrl = HttpUrl("https://example.org/catalog")
    dataset_base_uri: AnyUrl = HttpUrl("https://example.org/dataset")

    database_url: str = "postgresql+psycopg://postgres:postgres@localhost:5432/datasets"
    catalogue_schema: str = "dataset_api"

    marquez_url: Optional[AnyUrl] = None
    marquez_namespace: Optional[str] = None

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
