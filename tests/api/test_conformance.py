"""SHACL conformance of served rows.

A validator that never rejects is indistinguishable from one that never runs, so
the negative controls here are not optional extras — they are the tests that give
the positive one meaning.
"""
from __future__ import annotations

import copy

import pytest

pytest.importorskip(
    "pyshacl",
    reason="needs the `conformance` extra (celine-ontologies[mapper])",
)
pytest.importorskip(
    "celine.mapper.profiles",
    reason="needs celine-ontologies>=1.10.0, which packages the ontology profiles",
)

from celine.dataset.api.catalogue.conformance import (  # noqa: E402
    ConformanceUnavailable,
    check_conformance,
)

#: A mapping onto SOSA, in the shape the catalogue stores: resolved, pinned, and
#: naming the source columns `/query` returns.
MAPPING = {
    "version": "1",
    "target_type": "sosa:Observation",
    "id_template": "https://example.org/obs/{observation_id}",
    "profile": {"name": "celine", "version": "v0.10"},
    "fields": [
        {
            "source": "result_time",
            "target": "sosa:resultTime",
            "datatype": "xsd:dateTime",
            "required": True,
        },
        {"source": "value", "target": "sosa:hasSimpleResult", "datatype": "xsd:decimal"},
        {"source": "sensor_iri", "target": "sosa:madeBySensor", "kind": "iri"},
    ],
}

#: A mapping onto a class the CELINE profile actually constrains.
#:
#: Not incidental: the profile carries no shape for `sosa:Observation`, which is
#: the `target_type` of most packaged observation specs, so an observation graph
#: conforms trivially — there is nothing to violate. The negative controls below
#: therefore use `celine:KPIDefinition`, which has `sh:minCount` constraints, or
#: they would pass for the wrong reason.
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

ROWS = [
    {
        "observation_id": "a1",
        "result_time": "2026-08-14T10:00:00Z",
        "value": "12.5",
        "sensor_iri": "https://example.org/sensor/1",
    },
    {
        "observation_id": "a2",
        "result_time": "2026-08-14T10:15:00Z",
        "value": "13.0",
        "sensor_iri": "https://example.org/sensor/1",
    },
]


# ---------------------------------------------------------------------------
# The positive control
# ---------------------------------------------------------------------------

def test_conformant_rows_conform() -> None:
    report = check_conformance(dataset_id="ds.gold.obs", mapping=MAPPING, rows=ROWS)
    assert report.conforms, report.violations
    assert report.sample_size == 2


def test_empty_sample_says_it_is_empty() -> None:
    """Zero rows conforms — over an empty graph. `sample_size` is in the report
    precisely so that pass cannot be mistaken for one about data."""
    report = check_conformance(dataset_id="ds.gold.obs", mapping=MAPPING, rows=[])
    assert report.conforms
    assert report.sample_size == 0


# ---------------------------------------------------------------------------
# The negative controls — what the check exists for
# ---------------------------------------------------------------------------

def test_mapping_drifted_from_its_table_is_reported() -> None:
    """A renamed column: the mapping requires `result_time`, the table now emits
    `ts`. This is the failure that actually happens, and it must come back as a
    result naming the column — not as a broken checker."""
    drifted = []
    for row in copy.deepcopy(ROWS):
        renamed = {k: v for k, v in row.items() if k != "result_time"}
        renamed["ts"] = row["result_time"]
        drifted.append(renamed)

    report = check_conformance(dataset_id="ds.gold.obs", mapping=MAPPING, rows=drifted)
    assert not report.conforms
    assert any("result_time" in v for v in report.violations), report.violations


def test_rows_that_map_cleanly_but_violate_a_shape_are_reported() -> None:
    """Maps without error, then fails validation.

    `celine:KPIDefinition` requires `hasKPIName` — a row that carries no name
    maps to a well-formed node that the shapes reject. This is the half of the
    check that the drift test cannot cover: nothing is wrong with the *mapping*,
    only with what it produced.
    """
    report = check_conformance(
        dataset_id="ds.gold.kpi", mapping=KPI_MAPPING, rows=[{"kpi_id": "k1"}]
    )
    assert not report.conforms
    assert any("hasKPIName" in v for v in report.violations), report.violations


def test_the_same_shape_passes_when_the_row_carries_the_property() -> None:
    """The other side of the previous test. Without it, that one is also
    satisfied by a checker that rejects everything."""
    report = check_conformance(
        dataset_id="ds.gold.kpi",
        mapping=KPI_MAPPING,
        rows=[
            {
                "kpi_id": "k1",
                "name": "Self-sufficiency",
                # Real concept IRIs from the profile's own vocabulary: the
                # shapes require `sh:class skos:Concept`, and only the ontology
                # merged into the data graph types them as such. An invented
                # IRI would fail for a reason that has nothing to do with the
                # row.
                "scope": "https://w3id.org/celine-eu#ScopeCommunity",
                "method": "https://w3id.org/celine-eu#MethodTotal",
                "granularity": "https://w3id.org/celine-eu#GranularityHourly",
            }
        ],
    )
    assert report.conforms, report.violations


# ---------------------------------------------------------------------------
# Which shapes ran
# ---------------------------------------------------------------------------

def test_report_names_the_version_that_ran() -> None:
    report = check_conformance(dataset_id="ds.gold.obs", mapping=MAPPING, rows=ROWS)
    assert report.profile_name == "celine"
    assert report.profile_version == "v0.10"
    assert report.profile_pinned is True


def test_unpinned_mapping_is_marked_as_such() -> None:
    """Still checked, against the newest profile — but a weaker assertion, and
    the consumer has to be able to tell."""
    mapping = {k: v for k, v in MAPPING.items() if k != "profile"}
    report = check_conformance(dataset_id="ds.gold.obs", mapping=mapping, rows=ROWS)
    assert report.profile_pinned is False
    assert report.profile_version


def test_override_is_not_a_pin() -> None:
    """Asking 'would this conform under another version' does not turn the
    answer into the dataset's own claim."""
    report = check_conformance(
        dataset_id="ds.gold.obs", mapping=MAPPING, rows=ROWS, profile_version="v0.8"
    )
    assert report.profile_version == "v0.8"
    assert report.profile_pinned is False


def test_aged_out_pin_is_unavailable_not_a_violation() -> None:
    """"The shapes could not be loaded" and "the rows violate the shapes" are
    different answers. Collapsing them files a broken deployment as a data
    finding."""
    mapping = copy.deepcopy(MAPPING)
    mapping["profile"]["version"] = "v0.1"
    with pytest.raises(ConformanceUnavailable) as exc:
        check_conformance(dataset_id="ds.gold.obs", mapping=mapping, rows=ROWS)
    assert "v0.1" in str(exc.value)


def test_stored_mapping_that_is_not_a_spec_is_unavailable() -> None:
    with pytest.raises(ConformanceUnavailable):
        check_conformance(dataset_id="ds.gold.obs", mapping={"nonsense": True}, rows=ROWS)
