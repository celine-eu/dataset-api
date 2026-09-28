from __future__ import annotations

import json
import logging
from typing import Any

from celine.sdk.auth import OidcClientCredentialsProvider
from celine.sdk.auth.jwt import is_service_account
from celine.sdk.rec_registry import (
    RecRegistryAdminClient,
    RecRegistryApiError,
    RecRegistryUserClient,
)
from fastapi import HTTPException
from sqlglot import exp

from celine.dataset.api.dataset_query.row_filters.models import RowFilterPlan
from celine.dataset.core.config import get_settings
from celine.dataset.security.models import AuthenticatedUser

logger = logging.getLogger(__name__)

# The registry's answer on `GET /user/assets` for a caller who is no member of
# any community (rec-registry `api/user.py`): `403 {"detail": …, "code":
# "not_a_member"}`. It is the only 403 that route gives: its middleware answers a
# missing or invalid token with 401. A registry older than the error codes sends
# the detail alone.
_NOT_A_MEMBER_STATUS = 403
_NOT_A_MEMBER_CODE = "not_a_member"
_NOT_A_MEMBER_DETAIL = "You are not a member of any community"


def _is_not_a_member(exc: BaseException) -> bool:
    """RF-12: the registry said this caller is no member — and nothing else did.

    Read from the SDK's own error (`RecRegistryUserClient.get_my_assets` raises
    `RecRegistryApiError` on anything but `200`, with the status and the
    registry's `code` and `detail` read from the top level of a JSON object
    body; both `None` for a body that is not one). The status must be 403. A
    refusal carrying a `code` is judged by that code alone: `not_a_member`
    matches whatever the detail says, any other code does not match even beside
    the old detail. One with no `code` (a registry that predates the codes)
    matches only on the exact detail. A bare 403 could come from anything in
    front of the registry (a gateway, a proxy) and would then be read as "owns
    nothing"; an answer neither rule recognises stays an error, which is loud,
    rather than a silent deny.
    """
    if not isinstance(exc, RecRegistryApiError):
        return False
    if exc.status_code != _NOT_A_MEMBER_STATUS:
        return False
    if exc.code is not None:
        return exc.code == _NOT_A_MEMBER_CODE
    return _names_no_code(exc.body) and exc.detail == _NOT_A_MEMBER_DETAIL


def _names_no_code(body: object) -> bool:
    """The raw refusal body has no `code` at all (absent or `null`).

    The SDK reads `code` only when it is a string, so a body whose `code` is
    something else (a list, a number) reaches here with `exc.code` `None`. That
    body did name a code — not ours — and must not fall back to the detail
    match meant for a registry older than the codes.
    """
    try:
        parsed = json.loads(body) if isinstance(body, (bytes, str)) and body else None
    except ValueError:
        return False
    return isinstance(parsed, dict) and parsed.get("code") is None


class RecRegistryHandler:
    name = "rec_registry"

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

        # Delegation: the rows belong to **these** members, not to the caller.
        #
        # This check comes before the service-account bypass and must stay
        # there. A dataspace query always arrives on a service identity, so the
        # bypass below would otherwise fire on every delegated request and serve
        # the whole table — the failure that looks most like success, because
        # the bypass is correct in the case it was written for.
        #
        # `is not None`, not truthiness. An **empty** principal list is still a
        # delegated request, and reading it as self-service dropped it straight
        # into that same bypass — every row of the table, for a decision that
        # named nobody. The control plane can now send one, because a decision
        # may name its subjects by typed keys instead, so the empty case is no
        # longer hypothetical. This handler resolves members, not keys: with no
        # member named there is nothing to resolve, and the answer is no rows.
        if principals is not None:
            return await self._resolve_for_members(
                table=table, args=args, user_ids=principals
            )

        # Service accounts are not registry members and see all rows unfiltered.
        # (Policy-level access control already validated dataset.query scope.)
        if is_service_account(user.claims):
            logger.debug(
                "Service account %s — bypassing rec_registry row filter for %s",
                user.sub, table,
            )
            return RowFilterPlan(table=table, kind="predicate", predicate_template=None)

        base_url = args.get("url") or get_settings().rec_registry_url
        if not isinstance(base_url, str) or not base_url:
            raise ValueError("rec_registry requires a base_url")

        user_token = user.token or None

        client = RecRegistryUserClient(
            base_url=base_url,
        )

        column = args.get("column")
        if not isinstance(column, str) or not column:
            raise ValueError("rec_registry requires args.column")

        try:
            assets = await client.get_my_assets(token=user_token)
        except Exception as e:
            if _is_not_a_member(e):
                # RF-12: a person with no membership owns no asset, so no row is
                # theirs — the same deny as RF-11's member without a meter. Only
                # this answer; every other failure stays an error (RF-05).
                logger.info(
                    "rec_registry: caller is no registry member for %s — no rows",
                    table,
                )
                return RowFilterPlan(table=table, kind="deny")
            # Status and code only: the error's message carries the registry's
            # sentence, which is not this service's to log (QE-03).
            logger.error(
                "rec_registry: registry request failed for %s: %s status=%s code=%s",
                table,
                type(e).__name__,
                getattr(e, "status_code", None),
                getattr(e, "code", None),
            )
            raise

        # `None` means no parsed answer at all (the SDK raises on anything but
        # a readable `200`, so this is a guard, not an expected path). That is an error, never "owns nothing" (RF-05's
        # distinction): read as an empty list it would silently deny a member
        # data they are entitled to. `is None`, not truthiness — an empty page
        # is a valid answer and must reach the deny below, not this 500.
        if assets is None:
            raise HTTPException(500, "Failed to enumerate user assets")

        user_device_ids: list[str] = []
        for asset in assets.items:
            if asset.sensor_id:
                user_device_ids.append(asset.sensor_id)

        if not user_device_ids:
            # RF-11 (celine-eu/dataset-api#74): no metered asset — no meter
            # attached yet, or only a PV plant or a battery — is an ordinary
            # member between approval and a manager attaching their meter. Deny,
            # the shape the delegated path uses, rather than emit `column IN ()`:
            # PostgreSQL rejects an empty IN as a syntax error, so the query
            # failed where it should have answered empty.
            logger.info(
                "rec_registry: caller resolved to no devices for %s — no rows", table
            )
            return RowFilterPlan(table=table, kind="deny")

        logger.debug(
            "rec_registry: caller %s resolved to %d device(s) for %s",
            user.sub, len(user_device_ids), table,
        )

        literals: list[exp.Expression] = []
        for v in user_device_ids:
            literals.append(exp.Literal.string(str(v)))

        predicate = exp.In(
            this=exp.Column(this=exp.Identifier(this=column, quoted=False)),
            expressions=literals,
        )

        return RowFilterPlan(
            table=table,
            kind="predicate",
            predicate_template=predicate,
            meta={"items": len(user_device_ids)},
        )

    async def _resolve_for_members(
        self, *, table: str, args: dict[str, Any], user_ids: list[str]
    ) -> RowFilterPlan:
        """Devices owned by a named set of members.

        The self-service path asks the registry "what is mine" with the caller's
        own token. Here there is no such caller: the members are the subjects a
        control plane says consented, and this service asks on its own identity
        (`rec-registry.lookup`) because it is the one with a relationship to the
        registry.

        The plan keeps `{member: [device…]}` in `meta`. Attribution is what makes
        an audit record able to say which consent covered which rows; whether it
        also reaches the consumer is a property of the sharing offer, decided
        upstream, not something this handler leaks by default.
        """
        base_url = args.get("url") or get_settings().rec_registry_url
        if not isinstance(base_url, str) or not base_url:
            raise ValueError("rec_registry requires a base_url")

        column = args.get("column")
        if not isinstance(column, str) or not column:
            raise ValueError("rec_registry requires args.column")

        if not user_ids:
            # An allow-list naming nobody. Deny without asking the registry: the
            # lookup would answer "no assets", which is the same deny one round
            # trip later, and an outage on that trip would turn it into a 500.
            logger.info("rec_registry: no member named for %s — no rows", table)
            return RowFilterPlan(table=table, kind="deny")

        # `RecRegistryAdminClient`, not the user client: the latter is
        # user-scoped (`/user/*`, "what is mine") and this is an admin lookup on
        # *this service's* identity. Mixing them puts a service-account token on
        # a self-service route, where it resolves to no member and quietly
        # returns nothing.
        assets = await self._lookup_assets(base_url, user_ids)

        by_member: dict[str, list[str]] = {}
        for asset in assets or []:
            sensor_id = getattr(asset, "sensor_id", None)
            owner = getattr(asset, "owner_user_id", None)
            # Assets without a sensor id (a PV plant, a battery) carry no value
            # for this column and must not become an empty literal in the IN.
            if sensor_id and owner:
                by_member.setdefault(owner, []).append(str(sensor_id))

        device_ids = [d for devices in by_member.values() for d in devices]
        if not device_ids:
            # The members consented, but none of them owns anything measured in
            # this table. Deny rather than emit `IN ()`: an empty predicate is a
            # syntax error in some dialects and a tautology in others, and one
            # of those serves everything.
            logger.info(
                "rec_registry: %d member(s) resolved to no devices for %s",
                len(user_ids), table,
            )
            return RowFilterPlan(table=table, kind="deny")

        predicate = exp.In(
            this=exp.Column(this=exp.Identifier(this=column, quoted=False)),
            expressions=[exp.Literal.string(v) for v in device_ids],
        )
        return RowFilterPlan(
            table=table,
            kind="predicate",
            predicate_template=predicate,
            meta={"items": len(device_ids), "by_member": by_member},
        )

    async def _lookup_assets(self, base_url: str, user_ids: list[str]):
        """`POST /admin/lookup/assets-by-user-ids`, on this service's identity.

        Requires `rec-registry.lookup`, which `svc-ds-dataset-api` holds in both
        realms. A failure propagates rather than degrading to an empty list: an
        empty answer means "these members own nothing", and turning a registry
        outage into that sentence would silently deny data that is authorised.
        """
        settings = get_settings()
        token = None
        oidc = getattr(settings, "oidc", None)
        if oidc is not None and getattr(oidc, "client_id", None):
            provider = OidcClientCredentialsProvider(
                base_url=oidc.base_url,
                client_id=oidc.client_id,
                client_secret=oidc.client_secret,
            )
            token = (await provider.get_token()).access_token

        client = RecRegistryAdminClient(base_url=base_url)
        return await client.lookup_assets_by_user_ids(user_ids, token=token)
