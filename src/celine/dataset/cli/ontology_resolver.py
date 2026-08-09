"""Resolve a dataset's ``ontology`` binding to an actual mapping spec.

Two homes, one resolver. A mapping is either shared — published in
``celine-ontologies`` and named — or specific to one dataset and kept beside the
pipeline that produces its columns. Both end up here as a plain dict, so nothing
downstream has to care which it was.

**Why resolution happens at export, not at request time.** The exported YAML is
imported into the catalogue and becomes ``DatasetEntry``; every other field there
is already a snapshot of ``governance.yaml`` (access level, tags, DCAT metadata),
and updating any of them means re-importing. The mapping follows the same rule,
which buys the property that matters: a ``spec_file`` lives in the pipelines
checkout, which the API does not have at request time. Resolving both kinds here
means ``/vocabulary`` never depends on a directory that may not exist on the box
serving it.

The cost, stated plainly: a shared spec updated in celine-ontologies does not
reach a deployed catalogue until the next import. That is the same staleness
every other governance field already has.
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)


class OntologyResolutionError(ValueError):
    """A declared mapping could not be resolved."""


def resolve_mapping(
    ontology: Any,
    base_dir: Path,
    dataset_name: str,
) -> tuple[str | None, dict[str, Any] | None]:
    """Resolve an ``OntologyConfig`` to ``(provenance, mapping)``.

    Args:
        ontology: the parsed ``ontology`` block, or None.
        base_dir: directory holding the governance.yaml that declared it —
            ``spec_file`` is relative to this, mirroring how a sibling
            ``owners.yaml`` is already found.
        dataset_name: for error messages only.

    Returns:
        ``(provenance, mapping)``. ``provenance`` is what was declared — a spec
        name or a relative path — and is stored in ``DatasetEntry.ontology_path``
        so the binding can be traced back to the governance file that made it.
        ``mapping`` is the spec content. ``(None, None)`` when nothing is
        declared, which is a dataset stating no semantic model — not an error.

    Raises:
        OntologyResolutionError: a binding was declared and could not be honoured.
            Deliberately fatal rather than skipped: a dataset that declares a
            mapping and silently gets none would serve 404 from /vocabulary,
            which is indistinguishable from declaring nothing at all.
    """
    if ontology is None:
        return None, None

    spec = getattr(ontology, "spec", None)
    spec_file = getattr(ontology, "spec_file", None)

    if spec and spec_file:
        raise OntologyResolutionError(
            f"{dataset_name}: declares both ontology.spec ({spec!r}) and "
            f"ontology.spec_file ({spec_file!r}) — two answers to what one "
            f"column means. The governance schema rejects this; the file was "
            f"probably not validated against it."
        )

    if spec_file:
        return spec_file, _load_local(spec_file, base_dir, dataset_name)
    if spec:
        return spec, _load_shared(spec, dataset_name)
    return None, None


def _load_local(spec_file: str, base_dir: Path, dataset_name: str) -> dict[str, Any]:
    path = (base_dir / spec_file).resolve()
    if not path.is_file():
        raise OntologyResolutionError(
            f"{dataset_name}: ontology.spec_file {spec_file!r} does not resolve "
            f"to a file (looked at {path}). Paths are relative to the "
            f"governance.yaml that declares them."
        )
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise OntologyResolutionError(
            f"{dataset_name}: {path} is not a mapping spec document"
        )
    _validate(data, source=str(path), dataset_name=dataset_name)
    return data


def _load_shared(spec: str, dataset_name: str) -> dict[str, Any]:
    try:
        from celine.mapper import MappingSpecLoader  # noqa: PLC0415
    except ImportError as exc:  # pragma: no cover - dependency is declared
        raise OntologyResolutionError(
            f"{dataset_name}: ontology.spec {spec!r} names a shared mapping, "
            f"which needs celine-ontologies installed."
        ) from exc

    from celine.mapper.spec import SpecValidationError  # noqa: PLC0415

    try:
        # Loaded, then re-read as a dict: MappingSpecLoader validates against
        # mapping_spec.schema.json and is the authority on whether a name exists,
        # but downstream wants the raw document, not a frozen dataclass.
        MappingSpecLoader().load_by_name(spec)
    except SpecValidationError as exc:
        raise OntologyResolutionError(f"{dataset_name}: {exc}") from exc

    from importlib import resources  # noqa: PLC0415

    text = (
        resources.files("celine.mapper")
        .joinpath("specs")
        .joinpath(f"{spec}.yaml")
        .read_text(encoding="utf-8")
    )
    return yaml.safe_load(text)


def _validate(data: dict[str, Any], source: str, dataset_name: str) -> None:
    """Validate a pipeline-local spec against the same schema shared ones face.

    A local mapping is not a lesser one — it is served from the same route and
    read by the same consumers, so it meets the same bar. Without this, the only
    specs anybody checks are the ones already least likely to be wrong.
    """
    try:
        from celine.mapper import MappingSpecLoader  # noqa: PLC0415
        from celine.mapper.spec import SpecValidationError  # noqa: PLC0415
    except ImportError:  # pragma: no cover - dependency is declared
        logger.warning(
            "celine-ontologies not installed; %s was not validated", source
        )
        return

    try:
        MappingSpecLoader().load_from_string(yaml.safe_dump(data), source=source)
    except SpecValidationError as exc:
        raise OntologyResolutionError(f"{dataset_name}: {exc}") from exc
