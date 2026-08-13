"""Owner aliases, from a governance rule to the DCAT agent node it publishes.

This repo used to carry its own copy of the owner registry, byte-identical to
the one in ``celine-utils`` except that it had no ``aliases``. Open-source
pipelines carry generic owner labels — ``dso``, ``rec`` — that a deployment's
``owners.yaml`` maps to its real organisations, and that mapping did nothing
here: every grid and REC dataset published ``urn:owner:dso`` / ``urn:owner:rec``.

The failure had two visible halves, and both are exercised below, because
fixing only the first would leave the catalogue no better off:

1. **Export.** ``canonical_uri`` returned ``None`` and the exporter fell back to
   a synthetic ``urn:owner:<alias>``.
2. **Serving.** ``_build_agent_node`` looks the *stored* URI up in an index
   built from ``did``/``url``, which never contains a ``urn:owner:`` value — so
   the dataset published a bare ``{"@id": "urn:owner:rec"}`` with no name and no
   homepage, rather than an identifiable publisher.

Both halves are covered here in their working and their failing directions: an
unresolvable owner must still degrade to the URN rather than vanish, because a
dataset with no publisher at all is worse than one with an opaque publisher.
"""
from __future__ import annotations

import pytest
from celine.governance import GovernanceRule, OwnerEntry, OwnersRegistry, parse_rule

from celine.dataset.api.catalogue.dcat_formatter import build_dataset
from celine.dataset.cli.export_governance import governance_rule_to_entry
from celine.dataset.db.models.dataset_entry import DatasetEntry


@pytest.fixture()
def registry() -> OwnersRegistry:
    """A deployment registry: generic labels mapped onto real organisations."""
    return OwnersRegistry(
        [
            OwnerEntry(
                id="greenland",
                type="schema:NGO",
                name="Greenland Soc. Coop.",
                url="https://www.greenland.it",
                aliases=["rec"],
            ),
            OwnerEntry(
                id="set-distribuzione",
                type="schema:Corporation",
                name="SET Distribuzione S.p.A.",
                url="https://www.setdistribuzione.it",
                did="did:web:set.dataspaces.localhost",
                aliases=["dso"],
            ),
        ]
    )


def _rule(owner: str | None = None, **block) -> GovernanceRule:
    """Build a rule the way a governance.yaml does — through ``parse_rule``.

    Deliberately not ``GovernanceRule(...)`` with keyword arguments: that is a
    different code path (it marks every field in ``model_fields_set``, which is
    what the overlay merge reads), so a test built that way would not be
    exercising what the exporter actually receives.
    """
    if owner:
        block["ownership"] = [{"name": owner}]
    return parse_rule(block)


def _entry(owner: str | None, owners: OwnersRegistry | None) -> dict:
    return governance_rule_to_entry(
        dataset_name="datasets.ds_dev_gold.rec_energy",
        rule=_rule(owner),
        backend_type="postgres",
        owners=owners,
    )


# ---------------------------------------------------------------------------
# export — happy path
# ---------------------------------------------------------------------------


def test_alias_exports_the_deployments_canonical_uri(registry: OwnersRegistry) -> None:
    """`rec` is not an owner; it is a label the deployment resolves."""
    entry = _entry("rec", registry)
    assert entry["publisher_uri"] == "https://www.greenland.it"
    assert entry["rights_holder_uri"] == "https://www.greenland.it"


def test_did_outranks_url_when_the_owner_has_both(registry: OwnersRegistry) -> None:
    """Once an owner runs a connector, its DID is the identifier it publishes under."""
    entry = _entry("dso", registry)
    assert entry["publisher_uri"] == "did:web:set.dataspaces.localhost"


def test_an_owner_id_resolves_as_well_as_an_alias(registry: OwnersRegistry) -> None:
    assert _entry("greenland", registry)["publisher_uri"] == "https://www.greenland.it"


def test_explicit_dcat_publisher_overrides_the_owner(registry: OwnersRegistry) -> None:
    """A governance file may name a publisher that is not the data's owner."""
    entry = governance_rule_to_entry(
        dataset_name="datasets.ds_dev_gold.rec_energy",
        rule=_rule("rec", dcat={"publisher_uri": "https://example.gov/agency"}),
        backend_type="postgres",
        owners=registry,
    )
    assert entry["publisher_uri"] == "https://example.gov/agency"
    # rights_holder still follows ownership — they are different claims
    assert entry["rights_holder_uri"] == "https://www.greenland.it"


# ---------------------------------------------------------------------------
# export — unhappy paths
# ---------------------------------------------------------------------------


def test_unknown_owner_degrades_to_a_urn(registry: OwnersRegistry) -> None:
    """Not every owner is registered, and that must not drop the attribution."""
    entry = _entry("acme-energy", registry)
    assert entry["publisher_uri"] == "urn:owner:acme-energy"
    assert entry["rights_holder_uri"] == "urn:owner:acme-energy"


def test_no_registry_at_all_degrades_to_a_urn() -> None:
    """`--owners` is optional; the export must still run."""
    assert _entry("rec", None)["publisher_uri"] == "urn:owner:rec"


def test_no_ownership_declared_publishes_no_owner(registry: OwnersRegistry) -> None:
    """Absent is absent — not an empty string, not a URN of nothing."""
    entry = _entry(None, registry)
    assert entry["publisher_uri"] is None
    assert entry["rights_holder_uri"] is None


# ---------------------------------------------------------------------------
# serving — the other half of the same defect
# ---------------------------------------------------------------------------


def _dataset_node(publisher_uri: str | None, owners: OwnersRegistry | None) -> dict:
    entry = DatasetEntry(
        dataset_id="datasets.ds_dev_gold.rec_energy",
        title="REC energy",
        tags={},
        lineage={"namespace": "datasets"},
        publisher_uri=publisher_uri,
    )
    return build_dataset(entry, owners=owners)


def test_a_resolvable_publisher_is_inlined_as_a_named_agent(
    registry: OwnersRegistry,
) -> None:
    """The point of resolving the alias: a publisher a consumer can identify."""
    node = _dataset_node("https://www.greenland.it", registry)["dct:publisher"]
    assert node["@id"] == "https://www.greenland.it"
    assert node["foaf:name"] == "Greenland Soc. Coop."
    assert node["foaf:homepage"] == {"@id": "https://www.greenland.it"}
    # DCAT-AP requires foaf:Organization; the Schema.org subtype rides alongside
    assert node["@type"] == ["foaf:Organization", "schema:NGO"]


def test_a_did_publisher_is_inlined_too(registry: OwnersRegistry) -> None:
    node = _dataset_node("did:web:set.dataspaces.localhost", registry)["dct:publisher"]
    assert node["foaf:name"] == "SET Distribuzione S.p.A."
    assert node["foaf:homepage"] == {"@id": "https://www.setdistribuzione.it"}


def test_a_urn_publisher_stays_bare(registry: OwnersRegistry) -> None:
    """This is what every REC dataset served before the alias was resolved.

    A `urn:owner:` value is never in the URI index — that index is built from
    `did`/`url` — so it cannot be enriched. Asserted rather than merely noted:
    it is the observable symptom, and if a future change starts inventing names
    for unresolvable URIs this should fail.
    """
    node = _dataset_node("urn:owner:rec", registry)["dct:publisher"]
    assert node == {"@id": "urn:owner:rec"}


def test_no_registry_serves_a_bare_node(registry: OwnersRegistry) -> None:
    """`owners.yaml` is optional at startup; the catalogue still serves."""
    node = _dataset_node("https://www.greenland.it", None)["dct:publisher"]
    assert node == {"@id": "https://www.greenland.it"}
