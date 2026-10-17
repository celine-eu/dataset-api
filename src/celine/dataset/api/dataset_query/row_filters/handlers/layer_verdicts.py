from __future__ import annotations

from typing import Any

from celine.governance import ALLOW_ALL, DENY_ALL

from celine.dataset.api.dataset_query.row_filters.models import RowFilterPlan
from celine.dataset.security.models import AuthenticatedUser


class AllowAllHandler:
    """A row-policy layer that admits every row, stated (GS-10).

    Governance args: none. ``binds`` is required and names the layer.

    An undeclared layer falls to ``ROW_FILTERS_DEFAULT``; this is how a dataset
    says the layer narrows nothing on purpose — every row of a grid table is the
    operator's, not a person's, so its person layer is ``allow_all``.
    """

    name = ALLOW_ALL
    #: Either layer: the filter's own ``binds`` says which.
    binds = None

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
        return RowFilterPlan(table=table, kind="predicate", meta={ALLOW_ALL: True})


class DenyAllHandler:
    """A row-policy layer that admits no row (GS-10).

    The executor refuses a dataset carrying it with ``403 row_policy_denied``
    before any handler runs, so a caller is told rather than handed an empty page;
    the plan here is what any other caller of the registry gets — no rows.
    """

    name = DENY_ALL
    binds = None

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
        return RowFilterPlan(table=table, kind="deny", meta={DENY_ALL: True})
