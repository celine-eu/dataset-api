"""`import catalogue` must not decide anything by directory order.

`expand_inputs` used to hand `glob.glob` straight to the collection loop, and
`glob.glob` returns whatever order `readdir` gives — neither sorted nor stable
across machines, checkouts or a `cp -r`. The loop then merged every file into
one flat `dataset_id → entry` map, and on a repeated key it printed
``Warning: duplicate dataset_id ... Overwriting.`` and kept the **last** one.

Put together, the winner of a repeated `dataset_id` was chosen by the
filesystem. In the deployment's own export directory that was 791 overwrites
across 100 files, and 141 of the current definitions lost to an older one.

Two properties are pinned here:

1. the expansion is ordered, so two runs over the same inputs read them in the
   same sequence;
2. a repeated `dataset_id` whose bodies **disagree** is refused, because no
   ordering rule can say which of two contradictory descriptions of one dataset
   is the true one. A repeated `dataset_id` whose bodies are *identical* is not
   a disagreement and is accepted.

Every test here fails against the pre-fix module: (1) and (3) on the ordering,
(2) on the refusal, (4) on the noise a bare re-declaration used to produce.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
import yaml
from typer.testing import CliRunner

from celine.dataset.cli import import_catalogue as mod
from celine.dataset.cli.main import app as cli_app

runner = CliRunner()


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _entry(title: str, table: str = "ds_dev_gold.thing") -> dict[str, Any]:
    return {
        "name": "datasets.ds_dev_gold.thing",
        "title": title,
        "physical_table": table,
        "backend_type": "postgres",
        "expose": True,
    }


def _write(path: Path, datasets: dict[str, dict]) -> Path:
    path.write_text(yaml.safe_dump({"datasets": datasets}, sort_keys=False))
    return path


class _Recorder:
    """Stands in for `httpx.Client` and keeps the payload instead of posting it."""

    def __init__(self, store: list[dict]) -> None:
        self._store = store

    def __call__(self, *_: Any, **__: Any) -> "_Recorder":
        return self

    def __enter__(self) -> "_Recorder":
        return self

    def __exit__(self, *_: Any) -> bool:
        return False

    def post(self, url: str, json: dict) -> "_Recorder":  # noqa: A002
        self._store.append(json)
        return self

    def raise_for_status(self) -> None:
        return None


@pytest.fixture()
def posted(monkeypatch: pytest.MonkeyPatch) -> list[dict]:
    store: list[dict] = []
    monkeypatch.setattr(mod.httpx, "Client", _Recorder(store))
    return store


def _run(pattern: str, *extra: str):
    return runner.invoke(
        cli_app,
        [
            "import",
            "catalogue",
            "--input",
            pattern,
            "--api-url",
            "http://catalogue.invalid",
            *extra,
        ],
    )


# ---------------------------------------------------------------------------
# 1. the expansion is ordered
# ---------------------------------------------------------------------------


def test_expand_inputs_returns_a_sorted_list(monkeypatch: pytest.MonkeyPatch) -> None:
    """The one thing a caller can rely on: the same inputs, the same sequence.

    `glob.glob` is patched to the order a filesystem is free to give — reverse,
    here — because the real `readdir` order cannot be arranged from a test.
    """
    unordered = ["/gov/zeta.yaml", "/gov/alpha.yaml", "/gov/mid.yaml"]
    monkeypatch.setattr(mod.glob, "glob", lambda _: list(unordered))

    assert [str(p) for p in mod.expand_inputs([Path("/gov/*.yaml")])] == sorted(unordered)


def test_expand_inputs_does_not_read_a_file_twice(monkeypatch: pytest.MonkeyPatch) -> None:
    """Two patterns may overlap; the same path must still be read once."""
    monkeypatch.setattr(mod.glob, "glob", lambda _: ["/gov/a.yaml", "/gov/b.yaml"])

    out = [str(p) for p in mod.expand_inputs([Path("/gov/*.yaml"), Path("/gov/*.yaml")])]
    assert out == ["/gov/a.yaml", "/gov/b.yaml"]


# ---------------------------------------------------------------------------
# 2. a conflicting re-declaration is refused
# ---------------------------------------------------------------------------


def test_a_conflicting_duplicate_is_refused(tmp_path: Path, posted: list[dict]) -> None:
    """Two files, one `dataset_id`, two different bodies: nothing is imported."""
    _write(tmp_path / "aaa.yaml", {"datasets.ds_dev_gold.thing": _entry("from aaa")})
    _write(tmp_path / "zzz.yaml", {"datasets.ds_dev_gold.thing": _entry("from zzz")})

    result = _run(f"{tmp_path}/*.yaml")

    assert result.exit_code == 1, result.output
    assert "datasets.ds_dev_gold.thing" in result.output
    assert "aaa.yaml" in result.output and "zzz.yaml" in result.output
    assert "title" in result.output  # the field they disagree about is named
    assert posted == []


def test_an_identical_redeclaration_is_accepted(tmp_path: Path, posted: list[dict]) -> None:
    """Re-declaring the same dataset with the same body is not a disagreement."""
    _write(tmp_path / "aaa.yaml", {"datasets.ds_dev_gold.thing": _entry("one title")})
    _write(tmp_path / "zzz.yaml", {"datasets.ds_dev_gold.thing": _entry("one title")})

    result = _run(f"{tmp_path}/*.yaml")

    assert result.exit_code == 0, result.output
    assert "Overwriting" not in result.output
    assert len(posted) == 1
    assert [d["dataset_id"] for d in posted[0]["datasets"]] == ["datasets.ds_dev_gold.thing"]


# ---------------------------------------------------------------------------
# 3. the outcome does not depend on the order the filesystem gives
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("reverse", [False, True])
def test_the_refusal_does_not_depend_on_directory_order(
    tmp_path: Path, posted: list[dict], monkeypatch: pytest.MonkeyPatch, reverse: bool
) -> None:
    """The same two files in either `readdir` order reach the same verdict.

    The pre-fix module fails both halves for the same reason: it reaches no
    verdict at all, it silently keeps whichever body came last.
    """
    _write(tmp_path / "aaa.yaml", {"datasets.ds_dev_gold.thing": _entry("from aaa")})
    _write(tmp_path / "zzz.yaml", {"datasets.ds_dev_gold.thing": _entry("from zzz")})

    real = mod.glob.glob
    monkeypatch.setattr(
        mod.glob, "glob", lambda p: sorted(real(p), reverse=reverse)
    )

    result = _run(f"{tmp_path}/*.yaml")

    assert result.exit_code == 1, result.output
    assert posted == []


@pytest.mark.parametrize("reverse", [False, True])
def test_allow_conflicts_still_picks_the_same_winner_in_either_order(
    tmp_path: Path, posted: list[dict], monkeypatch: pytest.MonkeyPatch, reverse: bool
) -> None:
    """The escape hatch is a *deterministic* last-writer-wins, not a coin toss.

    With `--allow-conflicts` the import proceeds; what it must not do is import
    a different dataset depending on the order the directory was read in. The
    winner is the last file in sorted order, `zzz.yaml`, both times.
    """
    _write(tmp_path / "aaa.yaml", {"datasets.ds_dev_gold.thing": _entry("from aaa")})
    _write(tmp_path / "zzz.yaml", {"datasets.ds_dev_gold.thing": _entry("from zzz")})

    real = mod.glob.glob
    monkeypatch.setattr(
        mod.glob, "glob", lambda p: sorted(real(p), reverse=reverse)
    )

    result = _run(f"{tmp_path}/*.yaml", "--allow-conflicts")

    assert result.exit_code == 0, result.output
    assert len(posted) == 1
    assert posted[0]["datasets"][0]["title"] == "from zzz", json.dumps(posted[0])
