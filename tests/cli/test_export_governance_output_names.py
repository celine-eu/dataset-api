"""`export governance` must overwrite its own previous output, not rename past it.

The output file is named after the governance file's parent directory, and the
disambiguating `<grandparent>__<app>.yaml` form existed for the one case it is
needed: two apps whose directories share a name inside one run.

The condition it was guarding on, though, was `out_file.exists()` — which is
also true of a file the *previous* run wrote. So every re-export into a
directory that already held output left the old file in place and wrote a new
one beside it under a different name. With the caller staging sources in
`mktemp -d`, the grandparent is a fresh `tmp.XXXXXXXX` every time, so each run
minted a whole new generation of names.

That is how one deployment's export directory came to hold 100 files for 21
apps — five generations, four of them stale — which the importer then merged
into one flat catalogue with the filesystem deciding the winners.
"""
from __future__ import annotations

from pathlib import Path

import yaml
from typer.testing import CliRunner

from celine.dataset.cli.main import app as cli_app

runner = CliRunner()

def _app(root: Path, name: str, title: str = "Thing") -> Path:
    d = root / name
    d.mkdir(parents=True)
    block = {"sources": {"datasets.ds_dev_gold.thing": {"title": title}}}
    (d / "governance.yaml").write_text(yaml.safe_dump(block, sort_keys=False))
    return d


def _export(pattern: str, out: Path):
    return runner.invoke(cli_app, ["export", "governance", pattern, "-o", str(out)])


def test_re_exporting_overwrites_instead_of_accumulating(tmp_path: Path) -> None:
    """Two runs over the same app leave one file, holding the second run's content."""
    out = tmp_path / "out"

    first = tmp_path / "run-one"
    _app(first, "metering", title="old title")
    assert _export(f"{first}/**/governance.yaml", out).exit_code == 0

    second = tmp_path / "run-two"
    _app(second, "metering", title="new title")
    assert _export(f"{second}/**/governance.yaml", out).exit_code == 0

    assert sorted(p.name for p in out.glob("*.yaml")) == ["metering.yaml"]
    written = yaml.safe_load((out / "metering.yaml").read_text())
    assert written["datasets"]["datasets.ds_dev_gold.thing"]["title"] == "new title"


def test_two_apps_sharing_a_directory_name_are_still_disambiguated(tmp_path: Path) -> None:
    """The case the suffix exists for keeps working: both apps reach the output."""
    out = tmp_path / "out"
    root = tmp_path / "src"
    _app(root / "alpha", "metering")
    _app(root / "beta", "metering")

    result = _export(f"{root}/*/*/governance.yaml", out)

    assert result.exit_code == 0, result.output
    names = sorted(p.name for p in out.glob("*.yaml"))
    assert names == ["beta__metering.yaml", "metering.yaml"], result.output
