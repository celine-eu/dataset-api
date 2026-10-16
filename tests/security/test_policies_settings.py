"""The policy bundle is located by the platform's CELINE_POLICIES_* names.

The infra chart sets `CELINE_POLICIES_DIR` on every service. This service used
to read its own unprefixed `POLICIES_DIR` instead, so the chart's value never
reached it; it now reads the SDK's `PoliciesSettings` like every other service.
"""

from __future__ import annotations

from pathlib import Path

from celine.dataset.core.config import Settings


def test_celine_policies_dir_reaches_the_settings(monkeypatch):
    monkeypatch.setenv("CELINE_POLICIES_DIR", "/srv/policies")
    monkeypatch.setenv("CELINE_POLICIES_DATA_DIR", "/srv/policy-data")
    monkeypatch.setenv("CELINE_POLICIES_CACHE_ENABLED", "false")

    policies = Settings().policies

    assert policies.policies_dir == Path("/srv/policies")
    assert policies.policies_data_dir == Path("/srv/policy-data")
    assert policies.policies_cache_enabled is False


def test_the_unprefixed_name_is_no_longer_read(monkeypatch):
    monkeypatch.delenv("CELINE_POLICIES_DIR", raising=False)
    monkeypatch.delenv("CELINE_POLICIES_POLICIES_DIR", raising=False)
    monkeypatch.setenv("POLICIES_DIR", "/srv/unprefixed")

    assert Settings().policies.policies_dir == Path("./policies")


def test_celine_policies_dir_is_read_from_dot_env(monkeypatch, tmp_path):
    monkeypatch.delenv("CELINE_POLICIES_DIR", raising=False)
    monkeypatch.delenv("CELINE_POLICIES_POLICIES_DIR", raising=False)
    (tmp_path / ".env").write_text("CELINE_POLICIES_DIR=/srv/from-dot-env\n")
    monkeypatch.chdir(tmp_path)

    assert Settings().policies.policies_dir == Path("/srv/from-dot-env")
