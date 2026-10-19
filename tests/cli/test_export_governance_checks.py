"""`export governance` checks before it writes, and a file it cannot load fails it.

The export runs unattended in the catalogue sync, where exit 0 is read as "the
catalogue was synced". A governance file that did not load used to be skipped with
the run exiting 0, so the catalogue silently lost that app's datasets. The governance
checks (celine-utils `celine.governance.check`) run first, on the same files merged
with the same overlays: what would make an exported entry wrong stops the export,
and so does a missing row-policy layer on an exposed non-``open`` dataset (GS-10).
"""
from __future__ import annotations

from pathlib import Path

import yaml
from typer.testing import CliRunner

from celine.dataset.cli.main import app as cli_app

runner = CliRunner()

BOTH_LAYERS = [
    {"handler": "allow_all", "binds": "person"},
    {"handler": "member_wide", "binds": "organization"},
]


def _app(root: Path, name: str, rule: dict) -> None:
    d = root / name
    d.mkdir(parents=True)
    doc = {"defaults": {}, "sources": {"datasets.ds_dev_gold.thing": rule}}
    (d / "governance.yaml").write_text(yaml.safe_dump(doc, sort_keys=False))


def _export(root: Path, out: Path, *extra: str):
    return runner.invoke(
        cli_app, ["export", "governance", f"{root}/**/governance.yaml", "-o", str(out), *extra]
    )


# @verifies GS-11
def test_a_clean_tree_exports(tmp_path: Path) -> None:
    _app(tmp_path / "src", "a", {"expose": True, "access_level": "internal", "row_filters": BOTH_LAYERS})
    result = _export(tmp_path / "src", tmp_path / "out")
    assert result.exit_code == 0, result.output
    assert (tmp_path / "out" / "a.yaml").is_file()


# @verifies GS-11
def test_an_unknown_handler_stops_the_export_before_anything_is_written(tmp_path: Path) -> None:
    rule = {
        "expose": True,
        "access_level": "internal",
        "row_filters": [{"handler": "no_such", "binds": "organization"}, BOTH_LAYERS[0]],
    }
    _app(tmp_path / "src", "a", rule)
    result = _export(tmp_path / "src", tmp_path / "out")
    assert result.exit_code == 1
    assert "no_such" in result.output
    assert not (tmp_path / "out").exists()


# @verifies GS-11
def test_a_layer_verdict_without_binds_stops_the_export(tmp_path: Path) -> None:
    rule = {"expose": True, "access_level": "internal", "row_filters": [{"handler": "allow_all"}]}
    _app(tmp_path / "src", "a", rule)
    assert _export(tmp_path / "src", tmp_path / "out").exit_code == 1


# @verifies GS-10
def test_a_missing_layer_stops_the_export_before_anything_is_written(tmp_path: Path) -> None:
    rule = {"expose": True, "access_level": "internal", "row_filters": [BOTH_LAYERS[1]]}
    _app(tmp_path / "src", "a", rule)
    result = _export(tmp_path / "src", tmp_path / "out")
    assert result.exit_code == 1, result.output
    assert "row-policy" in result.output and "no person layer" in result.output
    assert not (tmp_path / "out").exists()


# @verifies GS-10
def test_an_open_dataset_missing_a_layer_still_exports(tmp_path: Path) -> None:
    _app(tmp_path / "src", "a", {"expose": True, "access_level": "open"})
    result = _export(tmp_path / "src", tmp_path / "out")
    assert result.exit_code == 0, result.output
    assert (tmp_path / "out" / "a.yaml").is_file()


# @verifies GS-11
def test_an_invalid_owners_registry_is_fatal(tmp_path: Path) -> None:
    _app(tmp_path / "src", "a", {"expose": True, "access_level": "open"})
    owners = tmp_path / "owners.yaml"
    owners.write_text(yaml.safe_dump({"owners": [{"id": "x", "name": "X", "type": "schema:NGO", "organisation": {}}]}))
    result = _export(tmp_path / "src", tmp_path / "out", "--owners", str(owners))
    assert result.exit_code == 1
    assert "owners registry" in result.output
