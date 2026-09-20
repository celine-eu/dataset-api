"""The suite's own PostgreSQL database, and the guard that keeps it off anyone else's.

The fixtures drop and recreate the catalogue schema on every test. They must never do
that to the database the service itself runs against, so the suite never uses
`DATABASE_URL` directly:

- `TEST_DATABASE_URL`, when set, names the test database;
- otherwise it is `DATABASE_URL` with `_test` appended to the database name
  (`.../datasets` -> `.../datasets_test`), on the same server, created on first use.

`tests/conftest.py` installs that URL as `DATABASE_URL` and `DATASETS_DATABASE_URL`
before the application is imported, so settings rebuilt by a test and subprocesses
started by one both resolve to it.

Every destructive step goes through `assert_disposable` first. It fails closed: a
database whose name does not end in `_test`, or which is the developer's own catalogue
or datasets database, is refused before any connection is opened.

This module imports nothing from the application, so it can be loaded before it.
"""
from __future__ import annotations

from collections.abc import Iterable

from sqlalchemy.engine import URL, make_url

TEST_SUFFIX = "_test"


class NotATestDatabase(RuntimeError):
    """Raised instead of running destructive setup against a database the suite does not own."""


def _url(url: str | URL) -> URL:
    return url if isinstance(url, URL) else make_url(url)


def _identity(url: str | URL) -> tuple[str, int, str]:
    u = _url(url)
    return (u.host or "localhost", u.port or 5432, u.database or "")


def derive_test_url(database_url: str) -> str:
    """`DATABASE_URL` with `_test` appended to the database name, always.

    Always, including for a name that already ends in `_test`: the result must never
    be the database `DATABASE_URL` itself names.
    """
    u = _url(database_url)
    if not u.database:
        raise NotATestDatabase(f"DATABASE_URL names no database: {u!r}")
    return u.set(database=u.database + TEST_SUFFIX).render_as_string(hide_password=False)


def assert_disposable(url: str | URL, *, protected: Iterable[str | URL | None]) -> None:
    """Refuse unless `url` is clearly a database this suite may wipe.

    Clearly means: its name ends in `_test`, and it is not any of `protected` (the
    developer's own `DATABASE_URL` and `DATASETS_DATABASE_URL`, compared by host,
    port and database name).
    """
    host, port, name = _identity(url)
    if not name.endswith(TEST_SUFFIX):
        raise NotATestDatabase(
            f"refusing destructive test setup on database {name!r} at {host}:{port}: "
            f"its name does not end in {TEST_SUFFIX!r}. Set TEST_DATABASE_URL to a "
            f"database the suite owns, or leave it unset to use DATABASE_URL + '{TEST_SUFFIX}'."
        )
    for other in protected:
        if other and _identity(other) == (host, port, name):
            raise NotATestDatabase(
                f"refusing destructive test setup on database {name!r} at {host}:{port}: "
                "it is the database DATABASE_URL or DATASETS_DATABASE_URL points at."
            )


def ensure_database(url: str, *, protected: Iterable[str | URL | None]) -> None:
    """Create the test database if it does not exist yet, on the same server."""
    import sqlalchemy

    assert_disposable(url, protected=protected)
    target = _url(url)
    admin_url = target.set(drivername="postgresql+psycopg", database="postgres")
    admin = sqlalchemy.create_engine(admin_url, isolation_level="AUTOCOMMIT")
    try:
        with admin.connect() as conn:
            exists = conn.execute(
                sqlalchemy.text("SELECT 1 FROM pg_database WHERE datname = :n"),
                {"n": target.database},
            ).scalar()
            if not exists:
                conn.execute(sqlalchemy.text(f'CREATE DATABASE "{target.database}"'))
    finally:
        admin.dispose()
