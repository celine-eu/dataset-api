# tests/test_backend_resolution.py
from dataset.db.models.dataset_entry import DatasetEntry


def test_backend_config_roundtrip():
    ds = DatasetEntry(
        dataset_id="ds",
        title="X",
        backend_type="s3",
        backend_config={"path": "s3://bucket/key"},
    )
    assert ds.backend_config and ds.backend_config["path"] == "s3://bucket/key"
