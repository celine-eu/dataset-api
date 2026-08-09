"""Derive a dataset's JSON-LD context from its mapping spec.

`GET /catalogue/{id}/schema` says *what the columns are*; this says *what they
mean*. Neither substitutes for the other — a JSON Schema can say a column is
`xsd:decimal`, but not that it carries the `sosa:hasSimpleResult` of a
self-consumption observation.

The mapping spec is the only source. A hand-maintained response would be a third
holder of one fact, and drift between two is why this exists at all.

Nothing here privileges any particular ontology. Prefixes resolve through the
`celine-ontologies` registry, which catalogues every supported vocabulary; a
dataset mapped entirely onto SAREF goes through the same path as one mapped onto
CELINE, and its context names only the vocabularies it actually uses.
"""
from __future__ import annotations

import logging
from typing import Any

from celine.mapper.registry import PrefixError, prefix_map

logger = logging.getLogger(__name__)


class VocabularyError(ValueError):
    """A mapping could not be turned into a context."""


def _curie_prefix(value: str) -> str | None:
    """The prefix of a CURIE, or None if it is already an absolute IRI."""
    if not isinstance(value, str) or "://" in value:
        return None
    prefix, sep, _ = value.partition(":")
    return prefix if sep and prefix else None


def _term(field: dict[str, Any]) -> dict[str, str] | None:
    """The context entry for one field, or None if it does not describe a column.

    `kind: constant` and any field without `source` are excluded deliberately.
    They are part of the mapping — a fixed `sosa:observedProperty`, a templated
    feature-of-interest IRI — but they are not columns, and a context entry for
    one would describe something `/query` never returns.
    """
    source = field.get("source")
    if not source:
        return None

    kind = field.get("kind", "literal")
    if kind == "constant":
        return None

    term: dict[str, str] = {"@id": field["target"]}
    if kind == "iri":
        # The column holds an identifier, not a value. Without this a consumer
        # reads the IRI as a string literal.
        term["@type"] = "@id"
    elif field.get("datatype"):
        term["@type"] = field["datatype"]
    return term


def _walk(fields: list[dict[str, Any]] | None, out: dict[str, Any]) -> None:
    for field in fields or []:
        term = _term(field)
        if term is not None:
            out[field["source"]] = term
        # Nested fields describe columns of a sub-node, not of this table's rows.
        # They are walked for prefix collection but keyed under their own source,
        # which is the column holding the nested list.
        _walk(field.get("nested_fields"), out)


def _collect_prefixes(mapping: dict[str, Any], terms: dict[str, Any]) -> dict[str, str]:
    """Prefix declarations for exactly the CURIEs this mapping uses.

    Only the ones in play: a spec spanning `sosa:`, `rdf:` and `peco:` gets those
    three and nothing else, rather than every prefix the registry knows.
    """
    used: set[str] = set()

    for candidate in (mapping.get("target_type"),):
        if (prefix := _curie_prefix(candidate or "")) is not None:
            used.add(prefix)
    for term in terms.values():
        for value in (term.get("@id"), term.get("@type")):
            if value and value != "@id":
                if (prefix := _curie_prefix(value)) is not None:
                    used.add(prefix)

    registry = prefix_map()
    unknown = used - set(registry)
    if unknown:
        # Fail rather than emit a context with unexpandable CURIEs in it. An
        # unexpanded CURIE is not a smaller failure than a missing one — it is
        # the same failure, discovered by the consumer instead of here.
        raise VocabularyError(
            f"mapping uses prefixes not declared in the vocabulary registry: "
            f"{sorted(unknown)}"
        )
    return {prefix: registry[prefix] for prefix in sorted(used)}


def build_vocabulary(
    mapping: dict[str, Any],
    conforms_to: str | None = None,
) -> dict[str, Any]:
    """Build the JSON-LD document served at `/catalogue/{id}/vocabulary`.

    Args:
        mapping: the resolved mapping spec, as stored in
            ``DatasetEntry.ontology_mapping``.
        conforms_to: the model IRI the dataset declares, if any.

    Returns:
        A JSON-LD document whose ``@context`` maps this dataset's **source
        columns** to their ontology terms, so it lines up with what ``/query``
        actually returns.
    """
    if not isinstance(mapping, dict):
        raise VocabularyError("mapping is not a document")

    terms: dict[str, Any] = {}
    _walk(mapping.get("fields"), terms)

    context: dict[str, Any] = _collect_prefixes(mapping, terms)
    context.update(terms)

    document: dict[str, Any] = {"@context": context}
    if mapping.get("target_type"):
        document["@type"] = mapping["target_type"]
    if conforms_to:
        # The locator names the identity. `conformsTo` is the same value the
        # catalogue entry carries, so a dataset cannot advertise one model here
        # and another there.
        #
        # `dct` has to be declared for it, and is not necessarily among the
        # mapping's own prefixes — a spec mapping purely onto SAREF uses none of
        # Dublin Core. Emitting the key without the declaration would put an
        # unexpandable CURIE in the document, which is the failure this module
        # refuses to ship for every other term.
        if "dct" not in context:
            registry = prefix_map()
            if "dct" not in registry:
                raise VocabularyError(
                    "cannot emit dct:conformsTo — no `dct` prefix in the "
                    "vocabulary registry"
                )
            context["dct"] = registry["dct"]
        document["dct:conformsTo"] = {"@id": conforms_to}
    return document


__all__ = ["PrefixError", "VocabularyError", "build_vocabulary"]
