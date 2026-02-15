from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal, Optional

from sqlglot import exp


RowFilterPlanKind = Literal["predicate", "deny"]


@dataclass(frozen=True)
class RowFilterPlan:
    """A resolved row-filter plan ready to be applied to a SQL AST.

    - table: physical table name (after mapping), as used in the SQL AST
    - kind:
        - predicate: add `predicate_template` qualified for each table alias occurrence
        - deny: deny access (no rows)
    - predicate_template:
        A sqlglot expression that may contain unqualified Column nodes.
        The applier will qualify those Columns with the actual alias.
    """

    table: str
    kind: RowFilterPlanKind
    predicate_template: Optional[exp.Expression] = None
    meta: dict[str, Any] | None = None
