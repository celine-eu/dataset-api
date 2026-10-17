"""`import catalogue` refuses an invalid entry by default (GS-11).

It used to skip one with a warning and import the rest, so a typo'd row filter
left a dataset out of the catalogue with the run reporting success. `--no-strict`
keeps the skip for a deliberate one-off.
"""
from __future__ import annotations

import pytest
import typer

from celine.dataset.cli.import_catalogue import validate_catalogue_payload

VALID = {"title": "ok", "backend_type": "postgres", "backend_config": {"table": "t"}}
INVALID = {**VALID, "lineage": {"facets": {"governance": {"rowFilters": "oops"}}}}


# @verifies GS-11
def test_an_invalid_entry_fails_the_import_by_default():
    with pytest.raises(typer.Exit) as exc:
        validate_catalogue_payload({"ok": VALID, "bad": INVALID})
    assert exc.value.exit_code == 1


# @verifies GS-11
def test_no_strict_skips_it_explicitly():
    models = validate_catalogue_payload({"ok": VALID, "bad": INVALID}, strict=False)
    assert [m.dataset_id for m in models] == ["ok"]
