"""DPS-01 — the routes exist only when the mode is enabled."""
from __future__ import annotations

import importlib

import pytest

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec

from celine.dataset.dps import api as api_mod
from celine.dataset.dps import settings as dps_settings
from celine.dataset.main import create_app


def _pem() -> str:
    return ec.generate_private_key(ec.SECP256R1()).private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
    ).decode()


def _paths(app) -> set[str]:
    return set(app.openapi()["paths"])


@pytest.fixture()
def reload_routes(monkeypatch):
    import celine.dataset.routes.dps as routes_dps

    def _reload(enabled: bool, key: bool = True):
        monkeypatch.setenv("DPS_ENABLED", "true" if enabled else "false")
        if key:
            monkeypatch.setenv("DPS_TOKEN_SIGNING_KEY", _pem())
        else:
            monkeypatch.delenv("DPS_TOKEN_SIGNING_KEY", raising=False)
        api_mod._dataplane = None
        dps_settings.get_dps_settings.cache_clear()
        return importlib.reload(routes_dps)

    yield _reload
    monkeypatch.delenv("DPS_ENABLED", raising=False)
    monkeypatch.delenv("DPS_TOKEN_SIGNING_KEY", raising=False)
    dps_settings.get_dps_settings.cache_clear()
    api_mod._dataplane = None
    importlib.reload(routes_dps)


def test_off_by_default(reload_routes) -> None:
    reload_routes(False)
    paths = _paths(create_app(use_lifespan=False))
    assert not any(p.startswith("/dps") for p in paths)
    assert "/query" in paths  # the legacy path is unaffected


def test_mounted_when_enabled(reload_routes) -> None:
    reload_routes(True)
    paths = _paths(create_app(use_lifespan=False))
    assert "/dps/v1/dataflows/start" in paths
    assert "/dps/public/query" in paths
    assert "/query" in paths


def test_enabled_without_a_signing_key_fails_at_startup(reload_routes) -> None:
    with pytest.raises(ValueError, match="DPS_TOKEN_SIGNING_KEY"):
        reload_routes(True, key=False)


def test_the_mounted_data_plane_keeps_flows_in_the_catalogue_database(reload_routes) -> None:
    from celine.dataset.dps.store import SqlDataFlowRepository

    reload_routes(True)
    assert isinstance(api_mod.get_dataplane().store, SqlDataFlowRepository)
