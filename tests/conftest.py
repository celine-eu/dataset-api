# tests/conftest.py
import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from celine.dataset.core.config import Settings, settings
from celine.dataset.db.engine import get_session
from celine.dataset.db.models.dataset_entry import Base
from celine.dataset.main import create_app


@pytest.fixture()
async def test_engine():
    url = settings.database_url.replace("postgresql+psycopg", "postgresql+asyncpg")

    engine = create_async_engine(url, future=True)

    async with engine.begin() as conn:
        await conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis"))
        await conn.execute(
            text(f"DROP SCHEMA IF EXISTS {settings.catalogue_schema} CASCADE")
        )
        await conn.execute(text(f"CREATE SCHEMA {settings.catalogue_schema}"))
        await conn.run_sync(Base.metadata.create_all)

    yield engine

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
        try:
            yield test_session
        finally:
            if test_session.in_transaction():
                await test_session.rollback()

    app = create_app(use_lifespan=False)
    app.dependency_overrides[get_session] = override_get_session

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as c:
        yield c

    app.dependency_overrides.clear()
