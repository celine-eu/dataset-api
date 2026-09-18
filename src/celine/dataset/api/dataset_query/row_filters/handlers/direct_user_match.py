from __future__ import annotations

from typing import Any

from sqlglot import exp

from celine.dataset.api.dataset_query.row_filters.models import RowFilterPlan
from celine.dataset.security.models import AuthenticatedUser


class DirectUserMatchHandler:
    """Row filter: direct match `column = jwt.sub`.

    Governance args:
      - column: str (required)

    It reads the decision's `principals`, never its `keys`: the column holds the
    principal itself, and a typed data key is a value from another vocabulary
    entirely.
    """

    name = "direct_user_match"

    async def resolve(
        self,
        *,
        table: str,
        user: AuthenticatedUser,
        args: dict[str, Any],
        request_context: dict[str, Any] | None = None,
        principals: list[str] | None = None,
        keys: list[str] | None = None,
    ) -> RowFilterPlan:
        col = args.get("column")
        if not isinstance(col, str) or not col:
            raise ValueError("direct_user_match requires args.column")

        if principals is not None:
            # Delegated: the rows belong to these people, not to the caller.
            # Same column, same comparison — only *whose* value changes, which
            # is the whole difference between the two modes.
            #
            # `[]` is included on purpose. It is an allow-list naming nobody, so
            # it narrows to nothing. Falling through to the caller's own filter
            # would answer a decision about other people with the *service
            # account's* rows, and rendering `IN ()` is a syntax error in one
            # dialect and a tautology in another.
            if not principals:
                return RowFilterPlan(table=table, kind="deny")
            predicate = exp.In(
                this=exp.Column(this=exp.Identifier(this=col, quoted=False)),
                expressions=[exp.Literal.string(p) for p in principals],
            )
            return RowFilterPlan(
                table=table,
                kind="predicate",
                predicate_template=predicate,
                meta={"items": len(principals)},
            )

        predicate = exp.EQ(
            this=exp.Column(this=exp.Identifier(this=col, quoted=False)),
            expression=exp.Literal.string(user.sub),
        )
        return RowFilterPlan(table=table, kind="predicate", predicate_template=predicate)
