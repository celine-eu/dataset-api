"""`GET /catalogue/{id}/vocabulary` — what the columns mean.

The counterpart to `/schema`, which says what they *are*. The pair is the point:
a JSON Schema can say a column is `xsd:decimal`; only this can say it carries a
`sosa:hasSimpleResult`.
"""
from __future__ import annotations

import json

import pytest

from celine.dataset.db.models.dataset_entry import DatasetEntry

SAREF_MAPPING = {
    "version": "1",
    "target_type": "sosa:Observation",
    "id_template": "https://example.org/obs/{plant_id}/{ts}",
    "fields": [
        {"source": "ts", "target": "sosa:resultTime", "datatype": "xsd:dateTime"},
        {"source": "pv_kw", "target": "saref:hasValue", "datatype": "xsd:decimal"},
        {"source": "device", "target": "sosa:madeBySensor", "kind": "iri"},
        # Not a column: a fixed property every row shares. Must not appear.
        {"target": "sosa:observedProperty", "kind": "constant", "value": "https://x/p"},
    ],
}


async def _entry(session, **kw) -> DatasetEntry:
    entry = DatasetEntry(
        dataset_id=kw.pop("dataset_id", "ds.gold.pv"),
        title="PV",
        backend_type="postgres",
        backend_config={"table": "pv"},
        expose=kw.pop("expose", True),
        access_level=kw.pop("access_level", "open"),
        **kw,
    )
    session.add(entry)
    await session.commit()
    return entry


async def test_returns_the_context_as_json_ld(client, test_session):
    """`application/ld+json`, not `application/json`. A consumer content-
    negotiating for JSON-LD skips the latter."""
    await _entry(
        test_session,
        ontology_path="./mappings/pv.yaml",
        ontology_mapping=SAREF_MAPPING,
    )
    res = await client.get("/catalogue/ds.gold.pv/vocabulary")
    assert res.status_code == 200
    assert res.headers["content-type"].startswith("application/ld+json")


async def test_context_is_keyed_by_source_column(client, test_session):
    """Keyed by the column names `/query` returns, not by ontology term. A
    context keyed the other way describes the model but cannot be applied to a
    row."""
    await _entry(test_session, ontology_mapping=SAREF_MAPPING)
    res = await client.get("/catalogue/ds.gold.pv/vocabulary")
    context = json.loads(res.text)["@context"]

    assert context["ts"] == {"@id": "sosa:resultTime", "@type": "xsd:dateTime"}
    assert context["pv_kw"] == {"@id": "saref:hasValue", "@type": "xsd:decimal"}


async def test_iri_columns_are_typed_as_references(client, test_session):
    """A column holding an identifier needs `@type: @id`, or a consumer reads the
    IRI as a string literal."""
    await _entry(test_session, ontology_mapping=SAREF_MAPPING)
    res = await client.get("/catalogue/ds.gold.pv/vocabulary")
    assert json.loads(res.text)["@context"]["device"]["@type"] == "@id"


async def test_constants_are_not_in_the_context(client, test_session):
    """`kind: constant` is part of the mapping but is not a column. A context
    entry for it would describe something /query never returns."""
    await _entry(test_session, ontology_mapping=SAREF_MAPPING)
    context = json.loads((await client.get("/catalogue/ds.gold.pv/vocabulary")).text)["@context"]

    assert "sosa:observedProperty" not in context
    assert not any(
        isinstance(v, dict) and v.get("@id") == "sosa:observedProperty"
        for v in context.values()
    )


async def test_only_the_prefixes_this_mapping_uses_are_declared(client, test_session):
    """Not every prefix the registry knows. This mapping is SAREF and SOSA, so
    `peco` and `bigg` have no business in its context."""
    await _entry(test_session, ontology_mapping=SAREF_MAPPING)
    context = json.loads((await client.get("/catalogue/ds.gold.pv/vocabulary")).text)["@context"]

    declared = {k for k, v in context.items() if isinstance(v, str)}
    assert {"sosa", "saref", "xsd"} <= declared
    assert "peco" not in declared and "bigg" not in declared


async def test_a_dataset_need_not_use_celine_at_all(client, test_session):
    """The property the two-homes design exists for. This mapping names no CELINE
    term, and nothing in the response introduces one."""
    await _entry(test_session, ontology_mapping=SAREF_MAPPING)
    body = (await client.get("/catalogue/ds.gold.pv/vocabulary")).text
    assert "celine" not in body


async def test_every_curie_in_the_document_is_declared(client, test_session):
    """Including `dct:conformsTo`, whose prefix a SAREF-only mapping does not
    otherwise use. An unexpandable CURIE in a served context is the same failure
    as a missing term, discovered by the consumer instead."""
    await _entry(
        test_session,
        ontology_mapping=SAREF_MAPPING,
        tags={"conformsTo": "https://saref.etsi.org/core/"},
    )
    doc = json.loads((await client.get("/catalogue/ds.gold.pv/vocabulary")).text)
    context = doc["@context"]
    declared = {k for k, v in context.items() if isinstance(v, str)}

    curies = [doc.get("@type")] + [
        v.get("@id") for v in context.values() if isinstance(v, dict)
    ]
    curies += [k for k in doc if ":" in k]
    for curie in filter(None, curies):
        if "://" not in curie:
            assert curie.split(":")[0] in declared, f"{curie} has no prefix declaration"


async def test_conforms_to_matches_what_the_catalogue_advertises(client, test_session):
    """Same value, from the same tags entry the catalogue reads. A dataset must
    not advertise one model in its catalogue entry and another in its
    vocabulary."""
    await _entry(
        test_session,
        ontology_mapping=SAREF_MAPPING,
        tags={"conformsTo": "https://saref.etsi.org/core/"},
    )
    doc = json.loads((await client.get("/catalogue/ds.gold.pv/vocabulary")).text)
    assert doc["dct:conformsTo"] == {"@id": "https://saref.etsi.org/core/"}


async def test_no_mapping_is_404(client, test_session):
    """Which means "not exposed, or declares no mapping" — *not* "this dataset
    has no semantic model". The catalogue entry's dct:conformsTo is where a
    declared model lives."""
    await _entry(test_session, ontology_mapping=None)
    assert (await client.get("/catalogue/ds.gold.pv/vocabulary")).status_code == 404


async def test_unexposed_dataset_is_404(client, test_session):
    await _entry(test_session, expose=False, ontology_mapping=SAREF_MAPPING)
    assert (await client.get("/catalogue/ds.gold.pv/vocabulary")).status_code == 404


async def test_unknown_dataset_is_404(client):
    assert (await client.get("/catalogue/nope/vocabulary")).status_code == 404


async def test_is_unauthenticated(client, test_session):
    """Like /catalogue. A consumer decides whether it can read a dataset before it
    negotiates access, so a gated vocabulary gates discovery."""
    await _entry(
        test_session, access_level="restricted", ontology_mapping=SAREF_MAPPING
    )
    res = await client.get("/catalogue/ds.gold.pv/vocabulary")
    assert res.status_code == 200
