# tests/conftest.py
import os

from celine.dataset.core.config import get_settings, reset_settings

from .testdb import assert_disposable, derive_test_url, ensure_database

# -- the suite's own database ------------------------------------------------
#
# Resolved and installed before the application is imported, so that nothing
# the suite does can reach the database the service runs against. See
# `tests/testdb.py`.

reset_settings()
#: The developer's own databases, as the service would resolve them. Never wiped.
PROTECTED_DATABASE_URLS = (
    get_settings().database_url,
    get_settings().datasets_database_url,
)
TEST_DATABASE_URL = os.environ.get("TEST_DATABASE_URL") or derive_test_url(
    PROTECTED_DATABASE_URLS[0]
)
assert_disposable(TEST_DATABASE_URL, protected=PROTECTED_DATABASE_URLS)
os.environ["DATABASE_URL"] = TEST_DATABASE_URL
os.environ["DATASETS_DATABASE_URL"] = TEST_DATABASE_URL
reset_settings()

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from celine.dataset.db.engine import get_datasets_session, get_session
from celine.dataset.db.models.dataset_entry import Base
from celine.dataset.main import create_app


@pytest.fixture(scope="session")
def test_database_url() -> str:
    """The suite's own database, created on first use. Needs PostgreSQL; parser tests do not."""
    ensure_database(TEST_DATABASE_URL, protected=PROTECTED_DATABASE_URLS)
    return TEST_DATABASE_URL


def _disposable_async_url() -> str:
    """The URL the catalogue fixtures are about to wipe, checked at the moment of use.

    Read from settings again rather than trusted from import time: a test may have
    rebuilt settings from a different environment.
    """
    url = get_settings().database_url
    assert_disposable(url, protected=PROTECTED_DATABASE_URLS)
    return url.replace("postgresql+psycopg", "postgresql+asyncpg")


@pytest.fixture()
async def test_engine(test_database_url):
    engine = create_async_engine(_disposable_async_url(), future=True)

    async with engine.begin() as conn:
        await conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis"))
        await conn.execute(
            text(f"DROP SCHEMA IF EXISTS {get_settings().catalogue_schema} CASCADE")
        )
        await conn.execute(text(f"CREATE SCHEMA {get_settings().catalogue_schema}"))
        await conn.run_sync(Base.metadata.create_all)

    yield engine

    assert_disposable(engine.url, protected=PROTECTED_DATABASE_URLS)
    async with engine.begin() as conn:
        await conn.execute(
            text(f"DROP SCHEMA IF EXISTS {get_settings().catalogue_schema} CASCADE")
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
    # Tests create their data tables inside the catalogue schema via `test_session`,
    # so the datasets DB must resolve to that same session. Without this the query
    # path falls through to the real datasets engine and the tables are not found.
    app.dependency_overrides[get_datasets_session] = override_get_session

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as c:
        yield c

    app.dependency_overrides.clear()
