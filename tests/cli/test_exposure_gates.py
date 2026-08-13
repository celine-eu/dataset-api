"""The two exposure gates as the exporter writes them.

`expose` and `dataspace_expose` used to be one boolean, and the export copied
`dataspace.expose` onto the catalogue flag — so a dataset that had to be in the
catalogue was thereby offered into the dataspace, and one withheld from the
dataspace was also unqueryable through the API.

The migration that separates them is two-step, and these tests pin both halves:
a file written against the old grammar must still produce exactly today's
catalogue entry, and a migrated file must produce a catalogue entry that is *not*
an offer.
"""
from __future__ import annotations

import pytest
from celine.governance import parse_rule

from celine.dataset.cli.export_governance import governance_rule_to_entry


def _entry(block: dict) -> dict:
    return governance_rule_to_entry(
        dataset_name="datasets.ds_dev_gold.thing",
        rule=parse_rule(block),
        backend_type="postgres",
    )


# ---------------------------------------------------------------------------
# unmigrated files — the API is already serving these
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("legacy", [True, False])
def test_a_file_without_top_level_expose_keeps_its_catalogue_entry(legacy: bool) -> None:
    """The guarantee that lets the code ship before the files are migrated.

    Deploying an exporter that read only the new field would drop every
    unmigrated dataset out of a catalogue the API is already serving.
    """
    entry = _entry({"dataspace": {"expose": legacy}})
    assert entry["expose"] is legacy


def test_an_unmigrated_offer_is_still_an_offer() -> None:
    """No silent withdrawal either: the export reflects the file as written.

    Withholding is done by migrating the file, not by the exporter second-guessing
    it — otherwise the same flag would mean different things in the file and in
    the catalogue.
    """
    assert _entry({"dataspace": {"expose": True}})["dataspace_expose"] is True


# ---------------------------------------------------------------------------
# migrated files — the point of the split
# ---------------------------------------------------------------------------


def test_catalogue_without_dataspace() -> None:
    """Visible and queryable, not on offer — the grid-topology case."""
    entry = _entry({"expose": True, "dataspace": {"expose": False}})
    assert entry["expose"] is True
    assert entry["dataspace_expose"] is False


def test_both_gates_open() -> None:
    entry = _entry({"expose": True, "dataspace": {"expose": True}})
    assert entry["expose"] is True
    assert entry["dataspace_expose"] is True


def test_neither_gate() -> None:
    entry = _entry({"expose": False})
    assert entry["expose"] is False
    assert entry["dataspace_expose"] is False


def test_no_dataspace_block_is_not_an_offer() -> None:
    """Absence is not consent — a missing block must never read as offered."""
    assert _entry({"expose": True})["dataspace_expose"] is False


def test_a_dataset_can_withdraw_from_a_file_default() -> None:
    """`exclude_unset` merge: a dataset overrides an inherited true with false.

    Under the previous truthiness merge this was inexpressible — the defect that
    let `dataspace.expose: false` silently do nothing in production.
    """
    from celine.governance import merge_rules

    rule = merge_rules(
        parse_rule({"expose": True, "dataspace": {"expose": True}}),
        parse_rule({"dataspace": {"expose": False}}),
    )
    entry = governance_rule_to_entry(
        dataset_name="datasets.ds_dev_gold.thing", rule=rule, backend_type="postgres"
    )
    assert entry["expose"] is True
    assert entry["dataspace_expose"] is False
