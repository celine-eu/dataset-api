from __future__ import annotations

from typing import Any

from celine.dataset.api.dataset_query.row_filters.models import RowFilterPlan
from celine.dataset.security.models import AuthenticatedUser


class MemberWideHandler:
    """Row filter that narrows nothing, declared on purpose (GS-07).

    Governance args: none.

    An ``internal`` dataset with no row filter is closed to organization readers:
    nobody decided whose rows it holds. Declaring ``member_wide`` is that
    decision — every row belongs to every member of every organization (weather,
    public building data). It is a statement in governance that a reviewer can
    see and challenge, which an absent filter is not.

    Delegated requests are refused: a dataspace decision names consenting
    subjects, and "every member" is none of them.
    """

    name = "member_wide"
    #: What this filter narrows rows to (celine-utils REQ-0010).
    binds = "organization"

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
        if principals is not None or keys:
            raise NotImplementedError(
                "member_wide does not support a delegated allow-list"
            )
        return RowFilterPlan(table=table, kind="predicate", meta={"member_wide": True})
