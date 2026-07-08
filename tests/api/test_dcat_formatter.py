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
