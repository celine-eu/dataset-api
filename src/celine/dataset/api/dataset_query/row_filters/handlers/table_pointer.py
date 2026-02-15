from __future__ import annotations

from typing import Any

from sqlglot import exp

from celine.dataset.api.dataset_query.row_filters.models import RowFilterPlan
from celine.dataset.security.models import AuthenticatedUser


class TablePointerHandler:
    """Row filter: pointer table subquery.

    Filters dataset rows by joining to a separate pointer table that maps user->keys.

    Governance args:
      - column: str (required) target column on dataset table (e.g. sensor_id)
      - pointer_table: str (required) fully qualified table name (physical)
      - pointer_key_column: str (required) column in pointer_table that matches dataset column
      - pointer_subject_column: str (optional, default "user_id") column in pointer_table holding subject identifier
    """

    name = "table_pointer"

    async def resolve(
        self,
        *,
        table: str,
        user: AuthenticatedUser,
        args: dict[str, Any],
        request_context: dict[str, Any] | None = None,
    ) -> RowFilterPlan:
        column = args.get("column")
        pointer_table = args.get("pointer_table")
        pointer_key_column = args.get("pointer_key_column")
        pointer_subject_column = args.get("pointer_subject_column") or "user_id"

        if not isinstance(column, str) or not column:
            raise ValueError("table_pointer requires args.column")
        if not isinstance(pointer_table, str) or not pointer_table:
            raise ValueError("table_pointer requires args.pointer_table")
        if not isinstance(pointer_key_column, str) or not pointer_key_column:
            raise ValueError("table_pointer requires args.pointer_key_column")
        if not isinstance(pointer_subject_column, str) or not pointer_subject_column:
            raise ValueError("table_pointer requires args.pointer_subject_column")

        subq_select = (
            exp.select(exp.Column(this=exp.Identifier(this=pointer_key_column, quoted=False)))
            .from_(exp.Table(this=exp.Identifier(this=pointer_table, quoted=False)))
            .where(
                exp.EQ(
                    this=exp.Column(this=exp.Identifier(this=pointer_subject_column, quoted=False)),
                    expression=exp.Literal.string(user.sub),
                )
            )
        )

        predicate = exp.In(
            this=exp.Column(this=exp.Identifier(this=column, quoted=False)),
            query=exp.Subquery(this=subq_select),
        )
        return RowFilterPlan(table=table, kind="predicate", predicate_template=predicate)
