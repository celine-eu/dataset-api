# dataset_api/catalogue/db.py
from __future__ import annotations

from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dataset.core.config import settings

# For async SQLAlchemy we need an async driver (e.g. postgresql+asyncpg)
# If DATABASE_URL is sync, you can adapt here or keep a dedicated ASYNC_DATABASE_URL
ASYNC_DATABASE_URL = settings.database_url.replace(
    "postgresql+psycopg", "postgresql+asyncpg"
)

engine = create_async_engine(
    ASYNC_DATABASE_URL,
    future=True,
    echo=settings.env == "dev",
)

AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    expire_on_commit=False,
    autoflush=False,
)


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()
