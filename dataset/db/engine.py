# dataset/db/engine.py
from __future__ import annotations

from typing import AsyncGenerator, Optional

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from dataset.core.config import settings

ASYNC_DATABASE_URL = settings.database_url.replace(
    "postgresql+psycopg", "postgresql+asyncpg"
)

_engine: Optional[AsyncEngine] = None
_sessionmaker: Optional[async_sessionmaker[AsyncSession]] = None


def get_engine() -> AsyncEngine:
    global _engine, _sessionmaker
    if _engine is None:
        _engine = create_async_engine(
            ASYNC_DATABASE_URL,
            future=True,
            echo=settings.env == "dev",
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
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        yield session
