# tests/test_catalogue_import.py
import pytest

from celine.dataset.schemas.catalogue_import import (
    BackendConfig,
    CatalogueImportModel,
    DatasetEntryModel,
)


def test_valid_catalog_import():
    cat = CatalogueImportModel(
        datasets=[
            DatasetEntryModel(
                dataset_id="ds",
                title="Dataset",
                backend_type="postgres",
                backend_config=BackendConfig(table="x"),
            )
        ]
    )
    assert len(cat.datasets) == 1


def test_invalid_backend_type():
    with pytest.raises(Exception):
        CatalogueImportModel(
            datasets=[
                DatasetEntryModel(
                    dataset_id="ds",
                    title="Dataset",
                    backend_type="invalid",
                    backend_config=BackendConfig(table="x"),
                )
            ]
        )
