"""RF-12 — the declared celine-sdk floor carries `RecRegistryApiError`.

RF-12 reads the registry's "no member" answer off the `RecRegistryApiError`
that `RecRegistryUserClient.get_my_assets` raises (celine-sdk REQ-0132/0133).
celine-sdk 1.20.0 — the last release before those requirements — raises
`UnexpectedStatus` on that 403 instead, so a build that resolves 1.20.0 would
turn the non-member deny into a 500. The suite itself cannot see that: its
`.venv` may hold an editable SDK checkout whose version string still reads the
last release. So the floor in `pyproject.toml` is pinned here: it must exclude
every celine-sdk without REQ-0132, i.e. require 1.21.0 or later (the release
that carries it). Until 1.21.0 is on PyPI, `uv lock` cannot satisfy the floor,
which is the intended loud failure rather than a lock that quietly keeps 1.20.0.
"""
from __future__ import annotations

import tomllib
from pathlib import Path

from packaging.requirements import Requirement
from packaging.version import Version

PYPROJECT = Path(__file__).resolve().parent.parent / "pyproject.toml"

# The first celine-sdk release carrying REQ-0132/0133 (`RecRegistryApiError`
# with `status_code`, `code`, `detail`, raised by `get_my_assets`).
FIRST_SDK_WITH_API_ERROR = Version("1.21.0")
LAST_SDK_WITHOUT_API_ERROR = Version("1.20.0")


def _sdk_requirement() -> Requirement:
    deps = tomllib.loads(PYPROJECT.read_text())["project"]["dependencies"]
    found = [Requirement(d) for d in deps if Requirement(d).name == "celine-sdk"]
    assert len(found) == 1, "exactly one celine-sdk runtime dependency"
    return found[0]


def test_sdk_floor_excludes_releases_without_recregistryapierror() -> None:
    spec = _sdk_requirement().specifier
    assert not spec.contains(LAST_SDK_WITHOUT_API_ERROR, prereleases=True)
    assert spec.contains(FIRST_SDK_WITH_API_ERROR)


def test_sdk_floor_is_the_first_release_with_recregistryapierror() -> None:
    floors = [
        Version(s.version)
        for s in _sdk_requirement().specifier
        if s.operator in (">=", "==", "~=")
    ]
    assert floors == [FIRST_SDK_WITH_API_ERROR]
