"""The e2e layer runs only when asked.

These tests launch real processes and bind real ports. They are skipped unless
`DATASET_API_E2E=1`, so `uv run pytest` stays the fast, hermetic suite the
testing playbook describes.
"""
from __future__ import annotations

import os
from pathlib import Path

import pytest


#: This hook is handed **every** collected item, not only the ones under this
#: directory — a conftest's location scopes where it is loaded, never what it is
#: shown. Skipping without this check turned the whole suite green-by-absence.
_HERE = Path(__file__).parent


def pytest_collection_modifyitems(config, items) -> None:
    if os.environ.get("DATASET_API_E2E") == "1":
        return
    skip = pytest.mark.skip(reason="set DATASET_API_E2E=1 to run the e2e layer")
    for item in items:
        if _HERE in Path(str(item.fspath)).parents:
            item.add_marker(skip)
