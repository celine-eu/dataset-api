"""SHACL conformance of served rows: does the graph this dataset's mapping
produces satisfy the shapes of the ontology version it pins?

**What a green result asserts.** That the RDF graph produced by applying this
dataset's mapping to these rows satisfies the SHACL profile of the pinned
ontology version. Nothing more. It does *not* say the columns mean what the
mapping claims: a spec mapping `kwh` onto the wrong observed property, or onto
the right one with the wrong unit, produces a perfectly conformant graph of
wrong statements. SHACL closes the **structural** half of the promise
`dct:conformsTo` makes; the semantic half stays the producer's assertion and no
validator recovers it.

**What it is not.** Not a gate — nothing here refuses an import. Not a filter —
no row is ever dropped from any other endpoint's result because of a violation.
A query result that depended on shape conformance would be indistinguishable, to
the consumer, from a small one.

**Why the pinned version and not the newest.** A dataset asserts conformance
against the ontology version its mapping was written for. Validating it against
whatever the deployed library happens to carry would let an ontology release
decide overnight that a dataset stopped conforming, which is a fact about the
release rather than about the data. The pin lives in the mapping spec
(`profile.version`); the report always names the version that actually ran, so
an unpinned mapping is still distinguishable from a pinned one.
"""
from __future__ import annotations

import logging
from dataclasses import asdict, dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

#: Violation lines carried in a report. pyshacl's text report is verbose and a
#: broken mapping produces one block per row; the shapes that fail repeat, so
#: past this point the list stops informing and starts being a payload.
MAX_REPORTED_VIOLATIONS = 200


class ConformanceUnavailable(RuntimeError):
    """The check cannot run — missing dependency, or an unresolvable profile.

    Distinct from a failed check on purpose. "The shapes could not be loaded"
    and "the rows violate the shapes" are different answers, and collapsing them
    reports a broken deployment as a data-quality finding.
    """


@dataclass
class ConformanceReport:
    """The answer to one conformance check."""

    dataset_id: str
    conforms: bool
    sample_size: int
    violations: list[str] = field(default_factory=list)
    violations_truncated: bool = False
    #: Which shapes ran. `conforms` alone is not a claim — conforming to v0.8
    #: and to v0.10 are different statements about the same rows.
    profile_name: str | None = None
    profile_version: str | None = None
    #: False when the mapping declared no version. The check still runs, against
    #: the newest profile available, but it is a weaker assertion and the
    #: consumer has to be able to tell.
    profile_pinned: bool = False
    checked_at: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def check_conformance(
    *,
    dataset_id: str,
    mapping: dict[str, Any],
    rows: list[dict[str, Any]],
    context: dict[str, Any] | None = None,
    profile_version: str | None = None,
) -> ConformanceReport:
    """Map ``rows`` through ``mapping`` and validate the result.

    Args:
        dataset_id: for the report and for error messages.
        mapping: the resolved mapping spec, as stored in
            ``DatasetEntry.ontology_mapping``.
        rows: the sample. Zero rows is a legitimate input and reports
            ``conforms=True`` over an empty graph — see the note below.
        context: variables the spec's ``context_vars`` need.
        profile_version: override the mapping's pin. For the deliberate
            question — *would this dataset still conform under version N+1?* —
            which is how an ontology upgrade gets decided rather than
            discovered.

    Raises:
        ConformanceUnavailable: the mapper extra is not installed, the mapping
            is not a valid spec, or the profile cannot be resolved.

    A note on the empty sample: it conforms, and says so with ``sample_size: 0``.
    That is not a pass anyone should read as one, and it is why the field is in
    the report rather than only in the request.
    """
    from datetime import datetime, timezone

    try:
        from celine.mapper.engine import MappingError
        from celine.mapper.graph import GraphBuilder
        from celine.mapper.output_mapper import OutputMapper
        from celine.mapper.profiles import ProfileError
        from celine.mapper.spec import MappingSpecLoader, SpecValidationError
    except ImportError as exc:  # pragma: no cover - depends on install extras
        raise ConformanceUnavailable(
            "SHACL conformance needs the `conformance` extra "
            "(celine-ontologies[mapper]); install it or set "
            "CONFORMANCE_ENABLED=false"
        ) from exc

    try:
        spec = MappingSpecLoader().load_from_dict(mapping, source=dataset_id)
    except SpecValidationError as exc:
        # The mapping was schema-valid when the catalogue imported it, so this
        # means the stored document and the current spec schema have diverged —
        # a deployment problem, not a data one.
        raise ConformanceUnavailable(
            f"{dataset_id}: stored mapping is not a valid mapping spec — {exc}"
        ) from exc

    try:
        builder = GraphBuilder.for_spec(spec, version=profile_version)
    except ProfileError as exc:
        raise ConformanceUnavailable(f"{dataset_id}: {exc}") from exc

    pinned = bool(spec.profile and spec.profile.version) and profile_version is None
    checked_at = datetime.now(timezone.utc).isoformat()

    mapper = OutputMapper(spec=spec, context=context or {})
    try:
        nodes = mapper.map_many(rows)
    except MappingError as exc:
        # A mapping that has drifted from its table — a renamed column, a
        # required field gone null — fails here, before any shape is evaluated.
        # This is the failure the check exists to find, so it is a *result*
        # (conforms=False, naming what broke) and not ConformanceUnavailable.
        # Reporting it as unavailable would file the most common real defect
        # under "the checker is broken".
        return ConformanceReport(
            dataset_id=dataset_id,
            conforms=False,
            sample_size=len(rows),
            violations=[f"mapping failed before validation: {exc}"],
            profile_name=builder.profile.name,
            profile_version=builder.profile.version,
            profile_pinned=pinned,
            checked_at=checked_at,
        )

    document = builder.build_document(nodes)
    result = builder.validate_shacl(builder.to_rdf_graph(document))

    violations = list(result.violations)
    truncated = len(violations) > MAX_REPORTED_VIOLATIONS

    return ConformanceReport(
        dataset_id=dataset_id,
        conforms=result.conforms,
        sample_size=len(rows),
        violations=violations[:MAX_REPORTED_VIOLATIONS],
        violations_truncated=truncated,
        profile_name=result.profile_name,
        profile_version=result.profile_version,
        profile_pinned=pinned,
        checked_at=checked_at,
    )


__all__ = [
    "ConformanceReport",
    "ConformanceUnavailable",
    "check_conformance",
]
