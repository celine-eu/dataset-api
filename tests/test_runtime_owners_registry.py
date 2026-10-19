"""GS-11 — the runtime owners registry is loaded validated, and an invalid one is not fatal.

The registry only enriches DCAT publisher metadata, so the service starts without it:
an invalid file is logged at `ERROR` (not read as a smaller registry) and enrichment
is off. No database: the start-up health check and posture guard are stubbed.
"""
from __future__ import annotations

import logging
from pathlib import Path

import pytest
import yaml
from fastapi.testclient import TestClient

from celine.dataset import main as main_mod
from celine.dataset.core.config import Settings, reset_settings
from celine.dataset.routes import health as health_mod


@pytest.fixture(autouse=True)
def _reset():
    reset_settings()
    yield
    reset_settings()


@pytest.fixture()
def no_database(monkeypatch):
    async def _healthy():
        return []

    monkeypatch.setattr(main_mod, "is_healthly", _healthy)
    monkeypatch.setattr(health_mod, "is_healthly", _healthy)
    monkeypatch.setattr(main_mod, "enforce_posture", lambda settings: None)


def _start(owners_path: Path):
    app = main_mod.create_app(settings_override=Settings(owners_yaml_path=owners_path))
    return app, TestClient(app)


# @verifies GS-11
def test_an_invalid_owners_registry_is_logged_and_enrichment_is_off(
    tmp_path: Path, no_database, caplog
) -> None:
    owners = tmp_path / "owners.yaml"
    # An unknown key on an owner: the strict schema refuses it.
    owners.write_text(
        yaml.safe_dump(
            {"owners": [{"id": "ex-00001", "name": "Example", "type": "schema:NGO", "organisation": {}}]}
        )
    )
    caplog.set_level(logging.ERROR, logger=main_mod.logger.name)

    app, client = _start(owners)
    with client:
        assert app.state.owners is None
        assert client.get("/health").status_code == 200

    assert any(
        r.levelno == logging.ERROR and "Owners registry invalid" in r.getMessage()
        for r in caplog.records
    )


# @verifies GS-11
def test_a_valid_owners_registry_is_loaded(tmp_path: Path, no_database) -> None:
    owners = tmp_path / "owners.yaml"
    owners.write_text(
        yaml.safe_dump({"owners": [{"id": "ex-00001", "name": "Example", "type": "schema:NGO", "url": "https://rec.example.org"}]})
    )
    app, client = _start(owners)
    with client:
        assert app.state.owners is not None
        assert len(app.state.owners) == 1
