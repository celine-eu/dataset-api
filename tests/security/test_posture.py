"""The development defaults are refused outside `CELINE_ENV=dev` (NIS2 R23).

Unset, `prod`, `staging`, `test` or a typo is hardened; only `dev` relaxes.
"""
from __future__ import annotations

import pytest
from fastapi import HTTPException

import celine.dataset.security.governance as gov
from celine.dataset.core.config import Settings, configure, reset_settings
from celine.dataset.core.posture import enforce_posture, posture_guard
from celine.dataset.security.disclosure import AccessLevel
from celine.sdk.posture import InsecureConfiguration
from celine.sdk.settings.models import OidcSettings

from .conftest import make_entry

HARDENED = ["", "prod", "staging", "test", "deev"]

REAL_DB = "postgresql+psycopg://dataset_api:Zr7-generated@db.example.org:5432/datasets"
REAL_OIDC = dict(
    base_url="https://auth.example.org/realms/celine",
    jwks_uri="https://auth.example.org/realms/celine/protocol/openid-connect/certs",
    audience="svc-dataset-api",
)


def _settings(**kw) -> Settings:
    return Settings(**kw)


def _configured(env: str, **kw) -> Settings:
    fields = dict(
        env=env,
        database_url=REAL_DB,
        datasets_database_url=REAL_DB,
        oidc=OidcSettings(**REAL_OIDC),
    )
    fields.update(kw)
    return Settings(**fields)


@pytest.fixture(autouse=True)
def _reset():
    reset_settings()
    yield
    reset_settings()


def test_the_signal_defaults_to_hardened(monkeypatch):
    for name in ("CELINE_ENV", "ENVIRONMENT", "ENV"):
        monkeypatch.delenv(name, raising=False)
    assert Settings().env == ""
    assert not Settings().is_dev


def test_the_legacy_env_name_is_still_read_last(monkeypatch):
    monkeypatch.delenv("CELINE_ENV", raising=False)
    monkeypatch.delenv("ENVIRONMENT", raising=False)
    monkeypatch.setenv("ENV", "dev")
    assert Settings().is_dev
    monkeypatch.setenv("CELINE_ENV", "staging")
    assert not Settings().is_dev


@pytest.mark.parametrize("env", HARDENED)
def test_the_development_defaults_refuse_to_start(env):
    with pytest.raises(InsecureConfiguration) as err:
        enforce_posture(_settings(env=env))
    text = str(err.value)
    for setting in (
        "DATABASE_URL",
        "DATASETS_DATABASE_URL",
        "CELINE_OIDC_BASE_URL",
        "CELINE_OIDC_JWKS_URI",
    ):
        assert setting in text


@pytest.mark.parametrize("env", HARDENED)
def test_policies_off_refuses_to_start(env):
    with pytest.raises(InsecureConfiguration, match="POLICIES_CHECK_ENABLED"):
        enforce_posture(_configured(env, policies_check_enabled=False))


@pytest.mark.parametrize("env", HARDENED)
def test_a_secret_equal_to_the_client_id_refuses_to_start(env):
    oidc = OidcSettings(**REAL_OIDC, client_id="svc-dataset-api", client_secret="svc-dataset-api")
    with pytest.raises(InsecureConfiguration, match="CELINE_OIDC_CLIENT_SECRET"):
        enforce_posture(_configured(env, oidc=oidc))


@pytest.mark.parametrize("env", HARDENED)
def test_a_configured_deployment_starts(env):
    enforce_posture(_configured(env))


def test_dev_starts_on_the_defaults():
    guard = posture_guard(_settings(env="dev", policies_check_enabled=False))
    assert not guard.hardened
    assert len(guard.violations) >= 5
    guard.enforce()


@pytest.mark.parametrize("env", HARDENED)
async def test_policies_off_refuses_a_request_outside_dev(env, user):
    configure(_configured(env, policies_check_enabled=False))
    gov._policy_engine = None
    with pytest.raises(HTTPException) as err:
        await gov.enforce_dataset_access(
            entry=make_entry(disclosure=AccessLevel.INTERNAL), user=user
        )
    assert err.value.status_code == 503


async def test_policies_off_still_allows_a_request_in_dev(user):
    configure(_configured("dev", policies_check_enabled=False))
    gov._policy_engine = None
    await gov.enforce_dataset_access(
        entry=make_entry(disclosure=AccessLevel.INTERNAL), user=user
    )
