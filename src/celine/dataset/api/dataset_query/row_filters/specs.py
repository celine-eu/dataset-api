from __future__ import annotations

import logging
from typing import Any, List

from celine.dataset.db.models.dataset_entry import DatasetEntry

logger = logging.getLogger(__name__)


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
    """
    gov = _governance(entry)

    specs: List[dict[str, Any]] = []
    rf = gov.get("rowFilters") or gov.get("row_filters")
    if isinstance(rf, list):
        for item in rf:
            if isinstance(item, dict):
                specs.append(item)

    # Legacy support: migrate userFilterColumn -> direct handler
    legacy_col = gov.get("userFilterColumn") or gov.get("user_filter_column")
    if legacy_col and isinstance(legacy_col, str):
        specs.append({"handler": "direct_user_match", "args": {"column": legacy_col}})

    if specs:
        logger.debug("Dataset %s row filter specs: %s", entry.dataset_id, specs)

    return specs
