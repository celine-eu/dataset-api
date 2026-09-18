from __future__ import annotations

import logging
from typing import Any

from sqlglot import exp

from celine.dataset.api.dataset_query.row_filters.keys import values_of_type
from celine.dataset.api.dataset_query.row_filters.models import RowFilterPlan
from celine.dataset.security.models import AuthenticatedUser

logger = logging.getLogger(__name__)


class SubjectKeyMatchHandler:
    """Row filter: the column holds a data key the consent carried.

    Governance args:
      - column: str (required) — the column holding the key
      - key_type: str (required) — which type of the decision's `keys` it holds

    **This handler resolves nothing and ignores `principals`.** A grid operator's
    readings are keyed by supply point; which supply point belongs to whom is
    known to the organisation that collected the consent and to nobody here. So
    the allow-list arrives already in the column's own vocabulary and the
    handler's whole job is to match it — no registry, no lookup, no call back to
    the collector at query time.

    That is also why the principals are not a fallback. They name the same
    people in *this system's* vocabulary, which for this column is the wrong
    vocabulary entirely: matching a username against a supply point either
    returns nothing or, worse, returns something by coincidence.
    """

    name = "subject_key_match"

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
        column = args.get("column")
        if not isinstance(column, str) or not column:
            raise ValueError("subject_key_match requires args.column")

        key_type = args.get("key_type")
        if not isinstance(key_type, str) or not key_type:
            # Without a type there is nothing to select from the list, and
            # matching every type would let a key minted for one column narrow
            # another. A filter that cannot be applied is not permission to
            # serve unfiltered rows.
            raise ValueError("subject_key_match requires args.key_type")

        values = values_of_type(keys or [], key_type)
        if not values:
            # Nobody's key of this type is in the allow-list. That is "no rows",
            # never "no filter": the two are indistinguishable once the
            # predicate is gone, and one of them serves the whole table.
            logger.info(
                "subject_key_match: no %r key in the decision for %s — no rows",
                key_type,
                table,
            )
            return RowFilterPlan(table=table, kind="deny")

        # Sorted so the same allow-list always renders the same SQL: a predicate
        # whose literal order changes between requests defeats the statement
        # cache and makes two identical decisions look different in a plan.
        predicate = exp.In(
            this=exp.Column(this=exp.Identifier(this=column, quoted=False)),
            expressions=[exp.Literal.string(v) for v in sorted(values)],
        )
        return RowFilterPlan(
            table=table,
            kind="predicate",
            predicate_template=predicate,
            # The count and the type, never the values: keys are personal data
            # and `meta` is the half of a plan that reaches an audit record.
            meta={"items": len(values), "key_type": key_type},
        )
