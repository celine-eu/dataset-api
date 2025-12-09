# tests/conftest.py
import pytest
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy import text

from dataset.main import create_app
from dataset.catalogue.models import Base
from dataset.catalogue.db import get_session

from httpx import AsyncClient, ASGITransport


@pytest.fixture()
async def test_engine():
    from dataset.core.config import settings

    url = settings.database_url.replace("postgresql+psycopg", "postgresql+asyncpg")

    engine = create_async_engine(url, future=True)

    async with engine.begin() as conn:

        # --- HARD RESET ----------------------------------------------------
        # Completely remove the schema if it exists.
        await conn.execute(
            text(f"DROP SCHEMA IF EXISTS {settings.catalogue_schema} CASCADE")
        )
        # Recreate it empty.
        await conn.execute(text(f"CREATE SCHEMA {settings.catalogue_schema}"))
        # ---------------------------------------------------------------

        # Recreate SQLAlchemy models inside this schema
        await conn.run_sync(Base.metadata.create_all)

    yield engine

    # Cleanup after the entire test session
    async with engine.begin() as conn:
        await conn.execute(
            text(f"DROP SCHEMA IF EXISTS {settings.catalogue_schema} CASCADE")
        )

    await engine.dispose()


@pytest.fixture
async def test_session(test_engine):
    async_session_factory = async_sessionmaker(
        bind=test_engine,
        expire_on_commit=False,
        class_=AsyncSession,
    )
    async with async_session_factory() as session:
        yield session


@pytest.fixture
async def client(test_session):
    async def override_get_session():
        yield test_session

    app = create_app(use_lifespan=False)
    app.dependency_overrides[get_session] = override_get_session

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as c:
        yield c

    app.dependency_overrides.clear()
