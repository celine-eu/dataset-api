import pytest

from dataset.cli.export_openlineage import extract_lineage_info


@pytest.mark.asyncio
async def test_extract_lineage_info_minimal():
    mq = {
        "namespace": "ns",
        "name": "ds",
        "sourceName": "src",
        "createdAt": "2024-01-01T01:01:01Z",
        "tags": [],
        "facets": {},
    }
    out = extract_lineage_info(mq)
    assert out["namespace"] == "ns"
    assert "updatedAt" not in out or out["updatedAt"] is None
