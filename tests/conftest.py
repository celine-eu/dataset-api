# tests/conftest.py
import pytest
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker

from dataset.main import create_app
from dataset.catalogue.models import Base
from dataset.catalogue.db import get_session  # <-- IMPORTANT

from httpx import AsyncClient, ASGITransport


# ------------------------------------------------------
# Async SQLAlchemy test engine
# ------------------------------------------------------
@pytest.fixture()
async def test_engine():
    # Build a test DB URL by replacing sync driver with async
    from dataset.core.config import settings

    url = settings.database_url.replace("postgresql+psycopg", "postgresql+asyncpg")

    url = settings.database_url.replace("postgresql+psycopg", "postgresql+asyncpg")
    engine = create_async_engine(url, future=True)

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)

    yield engine
    await engine.dispose()


# ------------------------------------------------------
# Provide a fresh AsyncSession per test
# ------------------------------------------------------
@pytest.fixture
async def test_session(test_engine):
    async_session_factory = async_sessionmaker(
        bind=test_engine,
        expire_on_commit=False,
        class_=AsyncSession,
    )

    async with async_session_factory() as session:
        yield session


# ------------------------------------------------------
# Override FastAPI dependency: get_session → test_session
# ------------------------------------------------------
@pytest.fixture
async def client(test_session):

    # override FastAPI dependency
    async def override_get_session():
        yield test_session

    app = create_app(use_lifespan=False)
    app.dependency_overrides[get_session] = override_get_session

    # IMPORTANT: always create the client inside same event loop
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as c:

        # yield the client object properly
        yield c

    # cleanup overrides after client closes
    app.dependency_overrides.clear()
