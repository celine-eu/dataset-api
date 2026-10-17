from __future__ import annotations

import logging
from typing import Any, Dict, List

from celine.governance import ROW_FILTER_BINDS, row_filter_binds

from celine.dataset.db.models.dataset_entry import DatasetEntry

logger = logging.getLogger(__name__)


class RowFilterSpecError(ValueError):
    """A dataset's declared row filters cannot be read (GS-11).

    Raised, never dropped: a filter that is skipped reads as a layer nobody
    declared, and before the deny default that was every row served.
    """


def _governance(entry: DatasetEntry) -> dict[str, Any]:
    if not entry.lineage:
        return {}
    facets = entry.lineage.get("facets", {}) or {}
    return (facets.get("governance", {}) or {})  # type: ignore[return-value]


def get_row_filter_specs(entry: DatasetEntry) -> List[dict[str, Any]]:
    """Return row filter specs for a dataset.

    Supported governance keys:
    - rowFilters (camelCase)
    - row_filters (snake_case)
    - legacy: userFilterColumn / user_filter_column

    Legacy userFilterColumn is migrated into handler 'direct_user_match'.

    A list that is not a list, an item that is not a mapping, an item naming no
    handler and ``args`` that is not a mapping raise :class:`RowFilterSpecError`
    (GS-11). Absent and ``[]`` are the same: nothing declared.
    """
    gov = _governance(entry)

    specs: List[dict[str, Any]] = []
    rf = gov.get("rowFilters")
    if rf is None:
        rf = gov.get("row_filters")
    if rf is not None and not isinstance(rf, list):
        raise RowFilterSpecError(
            f"{entry.dataset_id}: rowFilters is a {type(rf).__name__}, not a list"
        )
    for index, item in enumerate(rf or []):
        if not isinstance(item, dict):
            raise RowFilterSpecError(
                f"{entry.dataset_id}: rowFilters[{index}] is a "
                f"{type(item).__name__}, not a mapping"
            )
        handler = item.get("handler")
        if not isinstance(handler, str) or not handler:
            raise RowFilterSpecError(
                f"{entry.dataset_id}: rowFilters[{index}] names no handler"
            )
        args = item.get("args")
        if args is not None and not isinstance(args, dict):
            raise RowFilterSpecError(
                f"{entry.dataset_id}: rowFilters[{index}] ({handler}) args is not a mapping"
            )
        specs.append(item)

    # Legacy support: migrate userFilterColumn -> direct handler
    legacy_col = gov.get("userFilterColumn") or gov.get("user_filter_column")
    if legacy_col and isinstance(legacy_col, str):
        specs.append({"handler": "direct_user_match", "args": {"column": legacy_col}})

    if specs:
        logger.debug("Dataset %s row filter specs: %s", entry.dataset_id, specs)

    return specs


def split_layers(
    entry: DatasetEntry, specs: List[dict[str, Any]]
) -> Dict[str, List[dict[str, Any]]]:
    """The specs by row-policy layer — a spec's ``binds`` (GS-10).

    Every layer is a key, an undeclared one with ``[]``. A ``binds`` outside
    the two raises :class:`RowFilterSpecError`; the import refuses it first
    (GS-08), so reaching this with one means the stored entry predates that.
    """
    layers: Dict[str, List[dict[str, Any]]] = {layer: [] for layer in ROW_FILTER_BINDS}
    for index, spec in enumerate(specs):
        try:
            layers[row_filter_binds(spec)].append(spec)
        except ValueError as exc:
            raise RowFilterSpecError(
                f"{entry.dataset_id}: rowFilters[{index}]: {exc}"
            ) from exc
    return layers
