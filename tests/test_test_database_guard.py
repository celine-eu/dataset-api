"""The suite never runs destructive setup against a database it does not own.

`tests/conftest.py` drops the catalogue schema before and after every test. Pointed
at the developer's own `DATABASE_URL`, that wipes the catalogue the running service
serves, so the suite runs against `<database>_test` and refuses anything else. See
`tests/testdb.py`.

None of these tests needs PostgreSQL. The URLs point at a port nothing listens on,
so a refusal can only come from the guard: without it, the same calls reach the
network and fail with a connection error instead.
"""
from __future__ import annotations

import pytest

from celine.dataset.core.config import get_settings

from . import conftest
from .testdb import (
    NotATestDatabase,
    assert_disposable,
    derive_test_url,
    ensure_database,
)

#: A developer's catalogue database, on a port nothing listens on.
DEVELOPER = "postgresql+psycopg://postgres:pw@127.0.0.1:9/datasets"
DERIVED = "postgresql+psycopg://postgres:pw@127.0.0.1:9/datasets_test"


def test_the_test_database_is_derived_by_suffix() -> None:
    assert derive_test_url(DEVELOPER) == DERIVED
    # Always a new name, even for one that already looks like a test database.
    assert derive_test_url(DERIVED).endswith("/datasets_test_test")


def test_the_developer_database_is_refused() -> None:
    with pytest.raises(NotATestDatabase, match="does not end in '_test'"):
        assert_disposable(DEVELOPER, protected=[DEVELOPER])


def test_a_test_named_database_that_is_the_developers_own_is_refused() -> None:
    """`DATABASE_URL=…/x_test` and `TEST_DATABASE_URL=…/x_test`: still refused."""
    with pytest.raises(NotATestDatabase, match="DATABASE_URL"):
        assert_disposable(DERIVED, protected=[DEVELOPER, DERIVED])


def test_the_derived_database_is_admitted() -> None:
    assert_disposable(DERIVED, protected=[DEVELOPER, None])


def test_creating_the_test_database_refuses_before_connecting() -> None:
    with pytest.raises(NotATestDatabase):
        ensure_database(DEVELOPER, protected=[DEVELOPER])


def test_the_catalogue_fixture_refuses_the_developer_database(monkeypatch) -> None:
    """The check the drop itself relies on, run against the developer's database.

    `test_engine` reads the URL from settings at the moment it is about to wipe the
    schema, so a test that rebuilt settings from another environment cannot steer the
    drop back onto a shared database.
    """
    monkeypatch.setattr(conftest, "PROTECTED_DATABASE_URLS", (DEVELOPER, DEVELOPER))
    monkeypatch.setattr(
        conftest, "get_settings", lambda: type("S", (), {"database_url": DEVELOPER})()
    )
    with pytest.raises(NotATestDatabase):
        conftest._disposable_async_url()


def test_the_suite_does_not_run_against_the_developer_database() -> None:
    """What the application resolves under the suite is the test database, not `DATABASE_URL`."""
    url = get_settings().database_url
    assert url == conftest.TEST_DATABASE_URL
    assert get_settings().datasets_database_url == conftest.TEST_DATABASE_URL
    assert_disposable(url, protected=conftest.PROTECTED_DATABASE_URLS)
