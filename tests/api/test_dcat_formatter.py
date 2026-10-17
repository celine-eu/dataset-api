from celine.dataset.api.catalogue.dcat_formatter import build_catalog
from celine.dataset.db.models.dataset_entry import DatasetEntry


def test_catalogue_listing_basic():
    entries = [
        DatasetEntry(
            dataset_id="test.ds",
            title="Test dataset",
            tags={},
            lineage={"namespace": "test"},
        ),
        DatasetEntry(
            dataset_id="test2.ds",
            title="Another dataset",
            tags={},
            lineage={"namespace": "test"},
        ),
    ]

    catalogue = build_catalog(entries)

    assert "dcat:dataset" in catalogue
    assert len(catalogue["dcat:dataset"]) == 2
    ids = {d["dct:identifier"] for d in catalogue["dcat:dataset"]}
    assert ids == {"test.ds", "test2.ds"}


# --- The ODRL offer advertises consent only for a person filter (GS-10) -------

import pytest  # noqa: E402

from celine.dataset.api.catalogue.dcat_formatter import _build_odrl_offer  # noqa: E402


def _consent_advertised(row_filters) -> bool:
    entry = DatasetEntry(
        dataset_id="datasets.gold.x",
        access_level="internal",
        lineage={"facets": {"governance": {"rowFilters": row_filters}}},
    )
    offer = _build_odrl_offer("urn:x", entry)
    constraints = offer["odrl:permission"][0].get("odrl:constraint", [])
    return any(c["odrl:leftOperand"]["@id"] == "ds:consentStatus" for c in constraints)


# @verifies GS-10
@pytest.mark.parametrize(
    "row_filters, advertised",
    [
        ([{"handler": "rec_registry", "args": {"column": "device_id"}}], True),
        ([{"handler": "organization_match", "binds": "organization", "args": {"column": "c"}}], False),
        ([{"handler": "allow_all", "binds": "person"}], False),
        ([{"handler": "deny_all", "binds": "person"}], False),
        (
            [
                {"handler": "allow_all", "binds": "person"},
                {"handler": "member_wide", "binds": "organization"},
            ],
            False,
        ),
    ],
    ids=["person filter", "organization filter", "allow_all", "deny_all", "both verdict-free"],
)
def test_consent_is_advertised_for_a_person_filter_only(row_filters, advertised):
    assert _consent_advertised(row_filters) is advertised
