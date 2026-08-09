"""Resolving a dataset's semantic-model binding, from either of its two homes.

A mapping is either shared — published in celine-ontologies and named — or
specific to one dataset and kept beside the pipeline that emits its columns. The
split is not organisational: a spec names *source columns*, and those columns are
the pipeline's output, so a bespoke mapping kept in another repository goes stale
on a rename with nothing to detect it. Shared shapes (meter readings, forecasts)
have the opposite problem and should not be restated per dataset.
"""
from __future__ import annotations

import yaml
import pytest

from celine.dataset.cli.export_governance import OntologyConfig
from celine.dataset.cli.ontology_resolver import OntologyResolutionError, resolve_mapping

LOCAL_SPEC = """
version: "1"
target_type: "sosa:Observation"
id_template: "https://example.org/obs/{plant_id}/{ts}"
fields:
  - source: ts
    target: "sosa:resultTime"
    datatype: xsd:dateTime
  - source: pv_kw
    target: "saref:hasValue"
    datatype: xsd:decimal
"""


def test_no_binding_is_not_an_error(tmp_path):
    """A dataset stating no semantic model and one failing to state it are
    different claims. Silence is the first, and must stay cheap."""
    assert resolve_mapping(None, tmp_path, "ds") == (None, None)


def test_shared_spec_resolves_from_the_package(tmp_path):
    provenance, mapping = resolve_mapping(
        OntologyConfig(spec="obs_rec_energy"), tmp_path, "ds"
    )
    assert provenance == "obs_rec_energy"
    assert mapping["target_type"] == "sosa:Observation"


def test_local_spec_resolves_relative_to_its_governance_file(tmp_path):
    """Relative to the governance.yaml, not the process cwd — the CLI is run from
    wherever the operator happens to be, and `owners.yaml` beside the governance
    file already sets this precedent."""
    (tmp_path / "mappings").mkdir()
    (tmp_path / "mappings" / "custom.yaml").write_text(LOCAL_SPEC, encoding="utf-8")

    provenance, mapping = resolve_mapping(
        OntologyConfig(spec_file="./mappings/custom.yaml"), tmp_path, "ds"
    )
    assert provenance == "./mappings/custom.yaml"
    assert [f["source"] for f in mapping["fields"]] == ["ts", "pv_kw"]


def test_the_provenance_is_what_was_declared_not_what_it_resolved_to(tmp_path):
    """`ontology_path` records the declaration so a binding traces back to the
    governance file that made it. Storing the resolved absolute path instead
    would record this machine's checkout layout."""
    (tmp_path / "custom.yaml").write_text(LOCAL_SPEC, encoding="utf-8")
    provenance, _ = resolve_mapping(
        OntologyConfig(spec_file="./custom.yaml"), tmp_path, "ds"
    )
    assert provenance == "./custom.yaml"
    assert str(tmp_path) not in provenance


def test_a_local_spec_faces_the_same_schema_as_a_shared_one(tmp_path):
    """A pipeline-local mapping is not a lesser one — same route, same consumers,
    same bar. Without this the only specs anyone validates are the ones already
    least likely to be wrong."""
    (tmp_path / "broken.yaml").write_text(
        yaml.safe_dump({"version": "1", "target_type": "sosa:Observation"}),
        encoding="utf-8",
    )
    with pytest.raises(OntologyResolutionError):
        resolve_mapping(OntologyConfig(spec_file="./broken.yaml"), tmp_path, "ds")


def test_a_missing_local_spec_is_fatal(tmp_path):
    """Not skipped. A dataset that declares a mapping and silently gets none
    serves 404 from /vocabulary, which a consumer reads as "declares no model" —
    the opposite of what the governance file says."""
    with pytest.raises(OntologyResolutionError, match="does not resolve"):
        resolve_mapping(OntologyConfig(spec_file="./nope.yaml"), tmp_path, "ds")


def test_an_unknown_shared_name_lists_the_alternatives(tmp_path):
    """The usual cause is a typo in a governance file written in another
    repository, so the message has to be readable by someone who cannot see the
    specs directory."""
    with pytest.raises(OntologyResolutionError, match="obs_rec_energy"):
        resolve_mapping(OntologyConfig(spec="obs_rec_enrgy"), tmp_path, "ds")


def test_declaring_both_homes_is_rejected(tmp_path):
    """Two bindings for one dataset is two answers to what one column means. The
    governance schema forbids it; this is the guard for a file that never met the
    schema — which is every file written before it gained the field."""
    (tmp_path / "custom.yaml").write_text(LOCAL_SPEC, encoding="utf-8")
    with pytest.raises(OntologyResolutionError, match="both"):
        resolve_mapping(
            OntologyConfig(spec="obs_rec_energy", spec_file="./custom.yaml"),
            tmp_path,
            "ds",
        )


def test_a_local_spec_may_use_a_non_celine_vocabulary(tmp_path):
    """The point of two homes. This spec maps onto SAREF and names no CELINE
    term; nothing in resolution privileges the CELINE profile."""
    (tmp_path / "custom.yaml").write_text(LOCAL_SPEC, encoding="utf-8")
    _, mapping = resolve_mapping(
        OntologyConfig(spec_file="./custom.yaml"), tmp_path, "ds"
    )
    targets = [f["target"] for f in mapping["fields"]]
    assert "saref:hasValue" in targets
    assert not any(t.startswith("celine:") for t in targets)
