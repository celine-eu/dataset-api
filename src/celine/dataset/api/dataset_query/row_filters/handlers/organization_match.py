from __future__ import annotations

import logging
from typing import Any

from sqlglot import exp

from celine.dataset.api.dataset_query.row_filters.models import RowFilterPlan
from celine.dataset.security.groups import reading_organizations
from celine.dataset.security.models import AuthenticatedUser
from celine.sdk.auth.jwt import is_service_account

logger = logging.getLogger(__name__)


class OrganizationMatchHandler:
    """Row filter: the rows of the organizations the caller reads in (GS-06).

    Governance args (at least one):
      - column: str — the column holding the organization a row belongs to. Its
        values **are** Keycloak organization aliases, by convention: a REC's
        `community_id` is its organization's alias, as rec-registry's community key is.
        No mapping is applied, so a column in another vocabulary narrows to
        nothing rather than to the wrong organization.
      - org_type: str — only organizations of this type (`dso`, `rec`) count.
        Alone, without `column`, every row is readable by a reader of any
        organization of that type: a dataset that is one kind of organization's
        business but does not yet say which one's.

    The caller's organizations are those where it holds a reading group
    (`admins`, `managers`, `viewers`). None → no rows.

    A service account is not narrowed: the policy admitted it by scope, and it
    belongs to no organization. A service relaying rows to a person forwards the
    person's token or narrows them itself — as with `rec_registry`.

    **Delegated requests are refused.** A dataspace decision names consenting
    subjects; which organization a caller belongs to says nothing about them, so
    the filter cannot be enforced and the executor serves no rows.
    """

    name = "organization_match"
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
            # Refused like `table_pointer`, before the args are read: the
            # executor turns this into "serve no rows".
            raise NotImplementedError(
                "organization_match does not support a delegated allow-list: "
                "an organization is not a consenting subject"
            )

        column = args.get("column")
        org_type = args.get("org_type")
        if column is not None and (not isinstance(column, str) or not column):
            raise ValueError("organization_match args.column must be a column name")
        if org_type is not None and (not isinstance(org_type, str) or not org_type):
            raise ValueError("organization_match args.org_type must be a type name")
        if column is None and org_type is None:
            # Neither says whose rows these are; applying nothing would serve
            # every row to any organization's reader.
            raise ValueError("organization_match requires args.column or args.org_type")

        if is_service_account(user.claims):
            return RowFilterPlan(table=table, kind="predicate", meta={"service": True})

        aliases = reading_organizations(user.claims, org_type=org_type)
        if not aliases:
            logger.info(
                "organization_match: caller reads in no %sorganization — no rows from %s",
                f"{org_type} " if org_type else "",
                table,
            )
            return RowFilterPlan(table=table, kind="deny")

        if column is None:
            # Any organization of the type reads every row.
            return RowFilterPlan(
                table=table, kind="predicate", meta={"organizations": len(aliases)}
            )

        predicate = exp.In(
            this=exp.Column(this=exp.Identifier(this=column, quoted=False)),
            expressions=[exp.Literal.string(a) for a in aliases],
        )
        return RowFilterPlan(
            table=table,
            kind="predicate",
            predicate_template=predicate,
            meta={"organizations": len(aliases)},
        )
