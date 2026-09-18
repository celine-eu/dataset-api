"""DPS-02 — only admitted control planes may signal (the real dependency)."""
from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from celine.dataset.dps import auth as auth_mod
from celine.dataset.dps.api import get_dataplane, signaling_router
from celine.dataset.dps.settings import DpsSettings

from .conftest import edc_start_message


@pytest.fixture()
def client(dataplane, monkeypatch) -> TestClient:
    monkeypatch.setattr(
        auth_mod, "get_dps_settings", lambda: DpsSettings(control_plane_clients=["svc-edc"])
    )

    def _validate(token: str) -> dict:
        tokens = {
            "edc": {"azp": "svc-edc", "sub": "u1"},
            "rfc9068": {"client_id": "svc-edc", "sub": "u2"},
            "stranger": {"azp": "svc-other", "sub": "u3"},
            "anonymous": {"sub": "u4"},
        }
        if token not in tokens:
            raise ValueError("signature")
        return tokens[token]

    monkeypatch.setattr(auth_mod, "_validated_claims", _validate)
    app = FastAPI()
    app.include_router(signaling_router)
    app.dependency_overrides[get_dataplane] = lambda: dataplane
    return TestClient(app)


def _start(client: TestClient, headers: dict, flow_id: str = "tp-a") -> int:
    return client.post(
        "/dps/v1/dataflows/start", json=edc_start_message(flow_id), headers=headers
    ).status_code


def test_an_admitted_client_may_signal(client) -> None:
    assert _start(client, {"Authorization": "Bearer edc"}) == 200


def test_client_id_is_read_when_azp_is_absent(client) -> None:
    assert _start(client, {"Authorization": "Bearer rfc9068"}, "tp-b") == 200


@pytest.mark.parametrize(
    ("headers", "status"),
    [
        ({}, 401),
        ({"Authorization": "Bearer "}, 401),
        ({"Authorization": "Bearer forged"}, 401),
        ({"Authorization": "Bearer stranger"}, 403),
        ({"Authorization": "Bearer anonymous"}, 403),
    ],
)
def test_everyone_else_is_refused(client, headers, status) -> None:
    assert _start(client, headers) == status
    assert client.get("/dps/registration", headers=headers).status_code == status


def test_an_empty_allowlist_admits_nobody(client, monkeypatch) -> None:
    monkeypatch.setattr(auth_mod, "get_dps_settings", lambda: DpsSettings())
    assert _start(client, {"Authorization": "Bearer edc"}) == 403


def test_the_validator_is_the_services_oidc_check(monkeypatch) -> None:
    """The token goes through `celine.sdk`'s JwtUser with this service's OIDC settings."""
    seen = {}

    class _User:
        claims = {"azp": "svc-edc"}

    def _from_token(token, oidc):
        seen["token"], seen["oidc"] = token, oidc
        return _User()

    import celine.sdk.auth as sdk_auth

    monkeypatch.setattr(sdk_auth.JwtUser, "from_token", staticmethod(_from_token))
    assert auth_mod._validated_claims("abc") == {"azp": "svc-edc"}
    assert seen["token"] == "abc"
    from celine.dataset.core.config import get_settings

    assert seen["oidc"] is get_settings().oidc
