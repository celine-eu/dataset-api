"""`import catalogue` authenticates: `POST /admin/catalogue` requires `dataset.admin`.

The token is `--token` (or `DATASET_API_TOKEN`) when given, else a client-credentials
token for this service's own OIDC client. With neither, nothing is sent.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml
from typer.testing import CliRunner

from celine.dataset.cli import import_catalogue as mod
from celine.dataset.cli.main import app as cli_app

runner = CliRunner()


class _Recorder:
    def __init__(self) -> None:
        self.headers: list[dict | None] = []

    def __call__(self, *_: Any, **__: Any) -> "_Recorder":
        return self

    def __enter__(self) -> "_Recorder":
        return self

    def __exit__(self, *_: Any) -> bool:
        return False

    def post(self, url: str, json: dict, headers: dict | None = None) -> "_Recorder":  # noqa: A002
        self.headers.append(headers)
        return self

    def raise_for_status(self) -> None:
        return None


@pytest.fixture()
def recorder(monkeypatch: pytest.MonkeyPatch) -> _Recorder:
    rec = _Recorder()
    monkeypatch.setattr(mod.httpx, "Client", rec)
    monkeypatch.delenv("DATASET_API_TOKEN", raising=False)
    return rec


@pytest.fixture()
def catalogue(tmp_path: Path) -> Path:
    path = tmp_path / "catalogue.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "datasets": {
                    "datasets.ds_dev_gold.thing": {
                        "title": "Thing",
                        "backend_type": "postgres",
                        "backend_config": {"table": "ds_dev_gold.thing"},
                    }
                }
            }
        )
    )
    return path


def _run(path: Path, *extra: str):
    return runner.invoke(
        cli_app,
        ["import", "catalogue", "-i", str(path), "--api-url", "http://catalogue.invalid", *extra],
    )


def test_the_given_token_is_sent(catalogue: Path, recorder: _Recorder) -> None:
    result = _run(catalogue, "--token", "tok")
    assert result.exit_code == 0, result.output
    assert recorder.headers == [{"Authorization": "Bearer tok"}]


def test_the_token_can_come_from_the_environment(
    catalogue: Path, recorder: _Recorder, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("DATASET_API_TOKEN", "from-env")
    result = _run(catalogue)
    assert result.exit_code == 0, result.output
    assert recorder.headers == [{"Authorization": "Bearer from-env"}]


def test_without_a_token_the_service_client_credentials_are_used(
    catalogue: Path, recorder: _Recorder, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(mod, "resolve_admin_token", lambda: "client-credentials")
    result = _run(catalogue)
    assert result.exit_code == 0, result.output
    assert recorder.headers == [{"Authorization": "Bearer client-credentials"}]


def test_without_any_credential_nothing_is_sent(
    catalogue: Path, recorder: _Recorder, monkeypatch: pytest.MonkeyPatch
) -> None:
    from celine.dataset.core import config

    settings = config.get_settings()
    monkeypatch.setattr(settings.oidc, "client_id", None)
    monkeypatch.setattr(settings.oidc, "client_secret", None)
    result = _run(catalogue)
    assert result.exit_code == 1
    assert "Cannot authenticate" in result.output
    assert recorder.headers == []


def test_a_dry_run_needs_no_token(
    catalogue: Path, recorder: _Recorder, monkeypatch: pytest.MonkeyPatch
) -> None:
    def refuse() -> str:
        raise AssertionError("a dry run must not ask for a token")

    monkeypatch.setattr(mod, "resolve_admin_token", refuse)
    result = _run(catalogue, "--dry-run")
    assert result.exit_code == 0, result.output
    assert recorder.headers == []
