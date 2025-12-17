from celine.dataset.api.catalogue.dcat_formatter import build_catalog
from celine.dataset.db.models.dataset_entry import DatasetEntry


def test_catalogue_listing_basic():
    entries = [
        DatasetEntry(
            dataset_id="test.ds",
            title="Test dataset",
            tags={},
            lineage={"namespace": "test"},  # REQUIRED
        ),
        DatasetEntry(
            dataset_id="test2.ds",
            title="Another dataset",
            tags={},
            lineage={"namespace": "test"},  # groups under same namespace
        ),
    ]

    catalogue = build_catalog(entries)

    assert "dcat:dataset" in catalogue
    assert len(catalogue["dcat:dataset"]) == 1
    assert catalogue["dcat:dataset"][0]["dct:identifier"] == "test"
