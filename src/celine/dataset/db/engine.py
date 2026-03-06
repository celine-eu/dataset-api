# dataset/db/engine.py
from __future__ import annotations

from typing import AsyncGenerator, Optional

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from celine.dataset.core.config import settings


def _to_asyncpg_url(url: str) -> str:
    return url.replace("postgresql+psycopg", "postgresql+asyncpg")


ASYNC_DATABASE_URL = _to_asyncpg_url(settings.database_url)
ASYNC_DATASETS_DATABASE_URL = _to_asyncpg_url(
    settings.datasets_database_url or settings.database_url
)

_engine: Optional[AsyncEngine] = None
_sessionmaker: Optional[async_sessionmaker[AsyncSession]] = None

_datasets_engine: Optional[AsyncEngine] = None
_datasets_sessionmaker: Optional[async_sessionmaker[AsyncSession]] = None


def get_engine() -> AsyncEngine:
    global _engine, _sessionmaker
    if _engine is None:
        _engine = create_async_engine(
            ASYNC_DATABASE_URL,
            future=True,
            # echo=settings.env == "dev",
        )
        _sessionmaker = async_sessionmaker(
            bind=_engine,
            expire_on_commit=False,
            autoflush=False,
            class_=AsyncSession,
        )
    return _engine


def get_sessionmaker() -> async_sessionmaker[AsyncSession]:
    global _sessionmaker
    if _sessionmaker is None:
        get_engine()
    if _sessionmaker is None:  # pragma: no cover - defensive
        raise Exception("Failed to create sessionmaker")
    return _sessionmaker


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """Catalogue DB session (DatasetEntry records, alembic schema)."""
    SessionLocal = get_sessionmaker()
    async with SessionLocal() as session:
        yield session


def get_datasets_engine() -> AsyncEngine:
    global _datasets_engine, _datasets_sessionmaker
    if _datasets_engine is None:
        _datasets_engine = create_async_engine(ASYNC_DATASETS_DATABASE_URL, future=True)
        _datasets_sessionmaker = async_sessionmaker(
            bind=_datasets_engine,
            expire_on_commit=False,
            autoflush=False,
            class_=AsyncSession,
        )
    return _datasets_engine


def get_datasets_sessionmaker() -> async_sessionmaker[AsyncSession]:
    global _datasets_sessionmaker
    if _datasets_sessionmaker is None:
        get_datasets_engine()
    if _datasets_sessionmaker is None:  # pragma: no cover - defensive
        raise Exception("Failed to create datasets sessionmaker")
    return _datasets_sessionmaker


async def get_datasets_session() -> AsyncGenerator[AsyncSession, None]:
    """Datasets DB session (actual data tables exposed via the API)."""
    SessionLocal = get_datasets_sessionmaker()
    async with SessionLocal() as session:
        yield session
