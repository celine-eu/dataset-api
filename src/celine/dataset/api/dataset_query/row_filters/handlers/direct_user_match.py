from __future__ import annotations

from typing import Any

from sqlglot import exp

from celine.dataset.api.dataset_query.row_filters.models import RowFilterPlan
from celine.dataset.security.models import AuthenticatedUser


class DirectUserMatchHandler:
    """Row filter: direct match `column = jwt.sub`.

    Governance args:
      - column: str (required)
    """

    name = "direct_user_match"

    async def resolve(
        self,
        *,
        table: str,
        user: AuthenticatedUser,
        args: dict[str, Any],
        request_context: dict[str, Any] | None = None,
    ) -> RowFilterPlan:
        col = args.get("column")
        if not isinstance(col, str) or not col:
            raise ValueError("direct_user_match requires args.column")

        predicate = exp.EQ(
            this=exp.Column(this=exp.Identifier(this=col, quoted=False)),
            expression=exp.Literal.string(user.sub),
        )
        return RowFilterPlan(table=table, kind="predicate", predicate_template=predicate)
