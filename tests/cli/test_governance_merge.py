"""Overlaying a dataset's governance block onto the file's defaults.

The rule under test is narrow and was violated for `dcat`: a dataset that states
*one* DCAT field must keep the file's defaults for the rest. Whole-object
replacement made

    defaults:
      dcat:
        themes: [.../data-theme/ENER]
    sources:
      datasets.x:
        dcat:
          conforms_to: http://www.w3.org/ns/sosa/

mean *"and no themes"*, silently — the defaults still sitting in the file looking
like they applied. It surfaced when the first dataset declared a semantic model
and lost its DCAT-AP metadata in the same edit.
"""
from __future__ import annotations

import textwrap
from pathlib import Path

from celine.dataset.cli.export_governance import load_governance_yaml, resolve_rule

THEME = "http://publications.europa.eu/resource/authority/data-theme/ENER"
SOSA = "http://www.w3.org/ns/sosa/"


def _config(tmp_path: Path, body: str):
    path = tmp_path / "governance.yaml"
    path.write_text(textwrap.dedent(body), encoding="utf-8")
    return load_governance_yaml(path)


def test_declaring_conforms_to_keeps_the_default_dcat_metadata(tmp_path: Path):
    config = _config(
        tmp_path,
        f"""
        defaults:
          access_level: internal
          dcat:
            themes: [{THEME}]
            accrual_periodicity: http://example.org/freq/IRREG
        sources:
          datasets.gold.measurements:
            dcat:
              conforms_to: {SOSA}
        """,
    )
    rule = resolve_rule(config, "datasets.gold.measurements")

    assert rule.dcat is not None
    assert rule.dcat.conforms_to == SOSA
    # The half that regressed: stating one field must not erase the others.
    assert rule.dcat.themes == [THEME]
    assert rule.dcat.accrual_periodicity == "http://example.org/freq/IRREG"


def test_a_dataset_overrides_the_default_it_restates(tmp_path: Path):
    """Merging is not union — a value a dataset states still wins."""
    other = "http://publications.europa.eu/resource/authority/data-theme/ENVI"
    config = _config(
        tmp_path,
        f"""
        defaults:
          dcat:
            themes: [{THEME}]
            conforms_to: {SOSA}
        sources:
          datasets.gold.grid:
            dcat:
              themes: [{other}]
              conforms_to: http://iec.ch/TC57/CIM100#
        """,
    )
    rule = resolve_rule(config, "datasets.gold.grid")

    assert rule.dcat.themes == [other]
    assert rule.dcat.conforms_to == "http://iec.ch/TC57/CIM100#"


def test_an_explicit_null_overrides_an_inherited_default(tmp_path: Path):
    """"Silent" and "said no" are different claims and merge differently.

    A dataset writing `conforms_to: null` states it has **no** payload model —
    the distinction the seam is built on, and the reason the catalogue emits
    `dct:conformsTo` absent rather than null. A truthiness-based overlay cannot
    see it: `None or <default>` inherits, so the dataset silently advertises a
    model it explicitly disclaimed. `exclude_unset` keeps the two apart.
    """
    config = _config(
        tmp_path,
        f"""
        defaults:
          dcat:
            themes: [{THEME}]
            conforms_to: {SOSA}
        sources:
          datasets.gold.unmodelled:
            dcat:
              conforms_to: null
        """,
    )
    rule = resolve_rule(config, "datasets.gold.unmodelled")

    assert rule.dcat.conforms_to is None
    # ...while everything it stayed silent about is still inherited.
    assert rule.dcat.themes == [THEME]


def test_defaults_apply_to_a_dataset_that_declares_no_dcat(tmp_path: Path):
    config = _config(
        tmp_path,
        f"""
        defaults:
          dcat:
            themes: [{THEME}]
        sources:
          datasets.gold.plain:
            tags: [gold]
        """,
    )
    rule = resolve_rule(config, "datasets.gold.plain")

    assert rule.dcat is not None
    assert rule.dcat.themes == [THEME]
    assert rule.dcat.conforms_to is None


def test_a_semantic_view_shares_its_sources_governance_by_anchor(tmp_path: Path):
    """The property `celine-pipelines` relies on to keep a view from leaking.

    An unpivoted `*_measurements_*` view is a projection of its source table and
    must be exactly as readable: same access level, same row filters. The
    governance files express that with a YAML anchor rather than a copy, so the
    two are one object and cannot drift. This pins that anchors survive parsing —
    a parser that resolved them differently, or a rewrite that expanded them into
    two independent blocks, would remove the guarantee without failing anything.
    """
    config = _config(
        tmp_path,
        """
        defaults:
          access_level: internal
        sources:
          datasets.gold.meters_data_15m: &meters
            access_level: restricted
            row_filters:
              - handler: rec_registry
                args:
                  column: device_id

          datasets.gold.meters_measurements_15m:
            <<: *meters
            ontology:
              spec: obs_energy_measurement
        """,
    )
    source = resolve_rule(config, "datasets.gold.meters_data_15m")
    view = resolve_rule(config, "datasets.gold.meters_measurements_15m")

    assert view.row_filters == source.row_filters
    assert view.access_level == source.access_level == "restricted"
    # ...and the view still adds what is its own.
    assert view.ontology is not None and view.ontology.spec == "obs_energy_measurement"
    assert source.ontology is None
