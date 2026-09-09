"""What happens to a query when its pooled connection is already dead.

Staging, 2026-09 (`docs/reports/2026-09-09-celine-staging-error-analysis.md` in
infra-observability, §4 and §9): about one dataset query in five from the digital twin
and the webapp answered `400 {"detail": "Database query failed"}` within 3 ms of the
governance check, PostgreSQL logged no error for any of them, and each dataset-api pod
held fewer open sockets to PostgreSQL than PostgreSQL held backends for it. A pooled
connection that has silently died fails on first use, nothing in the pool checked it
first, and the exception was logged at DEBUG — invisible at the INFO level the service
runs at.

Two things follow. The pool must verify a connection before handing it out and retire
connections that have idled too long (`pool_pre_ping`, `pool_recycle`). And a driver
failure must be logged where it can be read, and a lost connection reported as what it
is — a transient 503 the caller may retry — rather than as the caller's bad request.
"""

from __future__ import annotations

import logging

import pytest
from fastapi import HTTPException
from sqlalchemy.exc import DBAPIError

import celine.dataset.db.engine as engine_module
from celine.dataset.api.dataset_query.executor import _execute_sql_with_timeout
from celine.dataset.core.config import get_settings

# ─── The pool ────────────────────────────────────────────────────────────────


def test_both_engines_pre_ping_and_recycle_pooled_connections(monkeypatch):
    created: list[dict] = []

    def _record_engine(url, **kwargs):
        created.append(kwargs)
        return object()

    monkeypatch.setattr(engine_module, "create_async_engine", _record_engine)
    monkeypatch.setattr(engine_module, "async_sessionmaker", lambda **kwargs: object())
    for name in ("_engine", "_sessionmaker", "_datasets_engine", "_datasets_sessionmaker"):
        monkeypatch.setattr(engine_module, name, None)

    engine_module.get_engine()
    engine_module.get_datasets_engine()

    assert len(created) == 2, "the catalogue engine and the datasets engine"
    for kwargs in created:
        assert kwargs.get("pool_pre_ping") is True
        assert kwargs.get("pool_recycle") == get_settings().db_pool_recycle_seconds
        assert kwargs["pool_recycle"] > 0


def test_recycle_defaults_to_minutes_not_never():
    """SQLAlchemy's own default is -1 — keep for ever — which is what let a connection
    outlive whatever dropped it in the cluster network."""
    assert 0 < get_settings().db_pool_recycle_seconds <= 3600


# ─── The failure, once it happens anyway ─────────────────────────────────────


class _FailingSession:
    def __init__(self, exc: Exception) -> None:
        self._exc = exc

    async def execute(self, *_args, **_kwargs):
        raise self._exc


def _driver_error(message: str, *, disconnect: bool) -> DBAPIError:
    return DBAPIError(
        "SET LOCAL statement_timeout = 5000",
        {},
        ConnectionResetError(message) if disconnect else Exception(message),
        connection_invalidated=disconnect,
    )


@pytest.mark.asyncio
async def test_a_lost_connection_is_a_503_and_is_logged_at_warning(caplog):
    exc = _driver_error("connection was closed in the middle of operation", disconnect=True)

    with caplog.at_level(logging.WARNING), pytest.raises(HTTPException) as raised:
        await _execute_sql_with_timeout(_FailingSession(exc), "SELECT 1")

    assert raised.value.status_code == 503
    assert "connection" in raised.value.detail.lower()

    logged = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert logged, "a driver failure must be visible at the level the service runs at"
    assert "ConnectionResetError" in logged[0].getMessage()
    assert "connection was closed in the middle of operation" in logged[0].getMessage()


@pytest.mark.asyncio
async def test_a_failed_statement_is_still_a_400_but_is_logged_at_warning(caplog):
    exc = _driver_error('relation "ds_dev_gold.nope" does not exist', disconnect=False)

    with caplog.at_level(logging.WARNING), pytest.raises(HTTPException) as raised:
        await _execute_sql_with_timeout(_FailingSession(exc), "SELECT 1")

    assert raised.value.status_code == 400
    assert raised.value.detail == "Database query failed"

    logged = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert logged
    assert 'relation "ds_dev_gold.nope" does not exist' in logged[0].getMessage()


@pytest.mark.asyncio
async def test_a_statement_timeout_keeps_its_own_answer():
    exc = _driver_error("canceling statement due to statement timeout", disconnect=False)

    with pytest.raises(HTTPException) as raised:
        await _execute_sql_with_timeout(_FailingSession(exc), "SELECT 1")

    assert raised.value.status_code == 400
    assert raised.value.detail == "Query exceeded time limit"
