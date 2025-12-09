# tests/test_dcat_formatter.py
import pytest

from dataset.api.catalogue.dcat_formatter import build_catalog, build_dataset
from dataset.db.models import DatasetEntry


@pytest.mark.asyncio
async def test_catalogue_listing_basic():
    ds = DatasetEntry(
        dataset_id="test.ds",
        title="Test DS",
        description="Demo",
        backend_type="postgres",
        backend_config={"table": "t"},
        expose=True,
    )

    doc = build_catalog([ds])
    assert doc["@type"] == "dcat:Catalog"
    assert len(doc["dcat:dataset"]) == 1
    entry = doc["dcat:dataset"][0]
    assert entry["@type"] == "dcat:Dataset"
    assert entry["dct:title"] == "Test DS"


@pytest.mark.asyncio
async def test_single_dataset_metadata_minimal():
    ds = DatasetEntry(
        dataset_id="test.ds",
        title="Test DS",
        backend_type="postgres",
        backend_config={"table": "t"},
    )

    dcat = await build_dataset(ds)
    assert dcat["@type"] == "dcat:Dataset"
    assert dcat["dct:title"] == "Test DS"
    assert "dcat:distribution" in dcat
