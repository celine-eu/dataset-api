"""`POST /catalogue/{id}/conformance` — the endpoint, its switch, and its gate.

The unit-level behaviour of the check lives in `tests/api/test_conformance.py`.
What is tested here is what only the route can get wrong: that it is absent when
the feature is off, that a non-conforming dataset is still a *successful* check,
and that reading rows for a report is gated exactly like reading them through
`/query`.
"""
from __future__ import annotations

import importlib

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text

pytest.importorskip(
    "pyshacl",
    reason="needs the `conformance` extra (celine-ontologies[mapper])",
)
pytest.importorskip(
    "celine.mapper.profiles",
    reason="needs celine-ontologies>=1.10.0, which packages the ontology profiles",
)

from celine.dataset.core import config  # noqa: E402
from celine.dataset.db.engine import get_datasets_session, get_session  # noqa: E402
from celine.dataset.db.models.dataset_entry import DatasetEntry  # noqa: E402
from celine.dataset.main import create_app  # noqa: E402

TABLE = "dataset_api.conformance_kpi"

KPI_MAPPING = {
    "version": "1",
    "target_type": "celine:KPIDefinition",
    "id_template": "https://example.org/kpi/{kpi_id}",
    "profile": {"name": "celine", "version": "v0.10"},
    "fields": [
        {"source": "name", "target": "celine:hasKPIName", "datatype": "xsd:string"},
        {"source": "scope", "target": "celine:hasKPIScopeType", "kind": "iri"},
        {"source": "method", "target": "celine:hasKPICalculationMethod", "kind": "iri"},
        {
            "source": "granularity",
            "target": "celine:hasKPITemporalGranularity",
            "kind": "iri",
        },
    ],
}

CONFORMING_ROW = (
    "'k1', 'Self-sufficiency', "
    "'https://w3id.org/celine-eu#ScopeCommunity', "
    "'https://w3id.org/celine-eu#MethodTotal', "
    "'https://w3id.org/celine-eu#GranularityHourly'"
)
#: Same table, a row with no name — `celine:hasKPIName` is `sh:minCount 1`.
NON_CONFORMING_ROW = (
    "'k2', NULL, "
    "'https://w3id.org/celine-eu#ScopeCommunity', "
    "'https://w3id.org/celine-eu#MethodTotal', "
    "'https://w3id.org/celine-eu#GranularityHourly'"
)


@pytest.fixture
async def conformance_client(test_session):
    """A client for an app built with CONFORMANCE_ENABLED=true.

    The route module reads the setting at import to decide whether to register
    at all, so the setting has to be in place *and* the module reloaded before
    the app is created.
    """
    config.configure(config.Settings(conformance_enabled=True))
    module = importlib.import_module("celine.dataset.routes.catalogue_conformance")
    importlib.reload(module)

    async def override_get_session():
        try:
            yield test_session
        finally:
            if test_session.in_transaction():
                await test_session.rollback()

    app = create_app(use_lifespan=False)
    app.dependency_overrides[get_session] = override_get_session
    app.dependency_overrides[get_datasets_session] = override_get_session

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as c:
        yield c

    app.dependency_overrides.clear()
    config.reset_settings()
    importlib.reload(module)


async def _seed(session, *, rows: list[str], access_level: str = "open", **kw):
    entry = DatasetEntry(
        dataset_id=kw.pop("dataset_id", "kpi_defs"),
        title="KPI definitions",
        backend_type="postgres",
        backend_config={"table": TABLE},
        expose=kw.pop("expose", True),
        access_level=access_level,
        ontology_path=kw.pop("ontology_path", "./mappings/kpi.yaml"),
        ontology_mapping=kw.pop("ontology_mapping", KPI_MAPPING),
        **kw,
    )
    session.add(entry)
    await session.execute(
        text(
            f"CREATE TABLE {TABLE} ("
            "kpi_id TEXT, name TEXT, scope TEXT, method TEXT, granularity TEXT)"
        )
    )
    for row in rows:
        await session.execute(text(f"INSERT INTO {TABLE} VALUES ({row})"))
    await session.commit()
    return entry


@pytest.fixture
async def drop_table(test_session):
    yield
    await test_session.execute(text(f"DROP TABLE IF EXISTS {TABLE}"))
    await test_session.commit()


# ---------------------------------------------------------------------------
# The switch
# ---------------------------------------------------------------------------

async def test_route_is_absent_when_the_feature_is_off(client):
    """Not registered, rather than registered and answering 404. "Not deployed"
    is what is true when the setting is off, and it is what the OpenAPI document
    should say."""
    res = await client.post("/catalogue/kpi_defs/conformance", json={})
    assert res.status_code == 404


# ---------------------------------------------------------------------------
# The check
# ---------------------------------------------------------------------------

async def test_conforming_rows_report_conformance(
    conformance_client, test_session, drop_table
):
    await _seed(test_session, rows=[CONFORMING_ROW])
    res = await conformance_client.post("/catalogue/kpi_defs/conformance", json={})
    assert res.status_code == 200, res.text
    body = res.json()
    assert body["conforms"] is True, body["violations"]
    assert body["sample_size"] == 1
    assert body["profile_version"] == "v0.10"
    assert body["profile_pinned"] is True
    assert body["checked_at"]


async def test_violations_are_a_successful_check(
    conformance_client, test_session, drop_table
):
    """200 with `conforms: false`, not 4xx. A 4xx would conflate "the check
    failed to run" with "the check ran and found violations" — and the second is
    the endpoint working."""
    await _seed(test_session, rows=[NON_CONFORMING_ROW])
    res = await conformance_client.post("/catalogue/kpi_defs/conformance", json={})
    assert res.status_code == 200, res.text
    body = res.json()
    assert body["conforms"] is False
    assert any("hasKPIName" in v for v in body["violations"]), body["violations"]


async def test_sample_is_bounded_by_the_request(
    conformance_client, test_session, drop_table
):
    await _seed(test_session, rows=[CONFORMING_ROW, NON_CONFORMING_ROW])
    res = await conformance_client.post(
        "/catalogue/kpi_defs/conformance", json={"limit": 1}
    )
    assert res.status_code == 200, res.text
    assert res.json()["sample_size"] == 1


async def test_unknown_version_is_a_bad_request(
    conformance_client, test_session, drop_table
):
    """And the message names what is available — otherwise the three-version
    window is a policy nobody can discover."""
    await _seed(test_session, rows=[CONFORMING_ROW])
    res = await conformance_client.post(
        "/catalogue/kpi_defs/conformance", json={"profile_version": "v0.1"}
    )
    assert res.status_code == 400
    assert "v0.10" in res.json()["detail"]


async def test_dataset_without_a_mapping_is_404(
    conformance_client, test_session, drop_table
):
    """Same semantics as `/vocabulary`: no declared model, nothing to check."""
    await _seed(test_session, rows=[CONFORMING_ROW], ontology_mapping=None)
    res = await conformance_client.post("/catalogue/kpi_defs/conformance", json={})
    assert res.status_code == 404


async def test_unexposed_dataset_is_404(conformance_client, test_session, drop_table):
    """A caller who may not see the dataset may not learn it exists."""
    await _seed(test_session, rows=[CONFORMING_ROW], expose=False)
    res = await conformance_client.post("/catalogue/kpi_defs/conformance", json={})
    assert res.status_code == 404


async def test_reading_rows_for_a_report_is_gated_like_reading_them(
    conformance_client, test_session, drop_table
):
    """The report quotes row values back. An anonymous caller who cannot query a
    restricted dataset must not obtain them through this endpoint either — which
    is why it goes through `execute_query` rather than reading the table."""
    await _seed(test_session, rows=[CONFORMING_ROW], access_level="restricted")
    res = await conformance_client.post("/catalogue/kpi_defs/conformance", json={})
    assert res.status_code in (401, 403), res.text
