# dataset/db/engine.py
from __future__ import annotations

from typing import AsyncGenerator, Optional

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from celine.dataset.core.config import get_settings


def _to_asyncpg_url(url: str) -> str:
    return url.replace("postgresql+psycopg", "postgresql+asyncpg")


def _pool_options() -> dict:
    """Why every engine here checks its connections.

    Staging, 2026-09: roughly one dataset query in five failed inside 3 ms with a
    driver error PostgreSQL never saw, and each pod held fewer sockets to PostgreSQL
    than PostgreSQL held backends for it — pooled connections were dying silently and
    failing on first use. `pool_pre_ping` verifies a connection before handing it out
    and reconnects transparently; `pool_recycle` retires idle ones before whatever
    drops them gets the chance.
    """
    return {
        "pool_pre_ping": True,
        "pool_recycle": get_settings().db_pool_recycle_seconds,
    }


_engine: Optional[AsyncEngine] = None
_sessionmaker: Optional[async_sessionmaker[AsyncSession]] = None

_datasets_engine: Optional[AsyncEngine] = None
_datasets_sessionmaker: Optional[async_sessionmaker[AsyncSession]] = None


def get_engine() -> AsyncEngine:
    global _engine, _sessionmaker
    if _engine is None:
        s = get_settings()
        url = _to_asyncpg_url(s.database_url)
        _engine = create_async_engine(url, future=True, **_pool_options())
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
        s = get_settings()
        url = _to_asyncpg_url(s.datasets_database_url or s.database_url)
        _datasets_engine = create_async_engine(url, future=True, **_pool_options())
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
