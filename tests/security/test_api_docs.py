"""GS-04 — the API documentation is served under `CELINE_ENV=dev` only, or on opt-in.

Swagger UI, ReDoc and `openapi.json` describe every route and model. Outside dev
they answer `404` unless `CELINE_PUBLIC_DOCS` is true.
"""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from celine.dataset.core.config import Settings, reset_settings
from celine.dataset.main import create_app

DOC_PATHS = ("/docs", "/redoc", "/openapi.json")


@pytest.fixture(autouse=True)
def _reset(monkeypatch):
    monkeypatch.delenv("CELINE_PUBLIC_DOCS", raising=False)
    reset_settings()
    yield
    reset_settings()


def _statuses(env: str) -> dict[str, int]:
    app = create_app(use_lifespan=False, settings_override=Settings(env=env))
    with TestClient(app) as client:
        return {path: client.get(path).status_code for path in DOC_PATHS}


# @verifies GS-04
def test_dev_serves_the_docs() -> None:
    assert _statuses("dev") == {path: 200 for path in DOC_PATHS}


# @verifies GS-04
@pytest.mark.parametrize("env", ["", "prod", "staging", "deev"])
def test_outside_dev_the_docs_are_not_served(env) -> None:
    assert _statuses(env) == {path: 404 for path in DOC_PATHS}


# @verifies GS-04
@pytest.mark.parametrize("value", ["true", "1", "yes", "on"])
def test_outside_dev_an_opt_in_serves_them(monkeypatch, value) -> None:
    monkeypatch.setenv("CELINE_PUBLIC_DOCS", value)
    assert _statuses("prod") == {path: 200 for path in DOC_PATHS}


# @verifies GS-04
@pytest.mark.parametrize("value", ["false", "0", "", "maybe"])
def test_anything_else_keeps_them_off(monkeypatch, value) -> None:
    monkeypatch.setenv("CELINE_PUBLIC_DOCS", value)
    assert _statuses("prod") == {path: 404 for path in DOC_PATHS}


# @verifies GS-04
def test_the_schema_is_still_built_in_process_outside_dev() -> None:
    """Tooling that reads `app.openapi()` keeps working; only the route is gone."""
    app = create_app(use_lifespan=False, settings_override=Settings(env="prod"))
    assert "/query" in app.openapi()["paths"]
