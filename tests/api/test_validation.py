# tests/test_validation.py
from celine.dataset.schemas.catalogue_import import (
    BackendConfig,
    DatasetEntryModel,
    Lineage,
)


def test_lineage_optional_fields():
    model = DatasetEntryModel(
        dataset_id="x",
        title="X",
        backend_type="postgres",
        backend_config=BackendConfig(table="x"),
        lineage=Lineage(namespace="ns", name="x"),
    )
    assert model.lineage and model.lineage.namespace == "ns"


def test_lineage_accepts_extra_fields():
    model = Lineage.model_validate({"namespace": "ns", "extraField": "ignored"})
    assert model.namespace == "ns"
