from __future__ import annotations

import logging
from typing import Any

from fastapi import HTTPException
from sqlglot import exp

from celine.dataset.api.dataset_query.row_filters.models import RowFilterPlan
from celine.dataset.core.config import get_settings
from celine.dataset.security.models import AuthenticatedUser

from celine.sdk.auth.jwt import is_service_account
from celine.sdk.auth import OidcClientCredentialsProvider
from celine.sdk.rec_registry import RecRegistryAdminClient, RecRegistryUserClient

logger = logging.getLogger(__name__)


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
    ) -> RowFilterPlan:

        # Delegation: the rows belong to **these** members, not to the caller.
        #
        # This check comes before the service-account bypass and must stay
        # there. A dataspace query always arrives on a service identity, so the
        # bypass below would otherwise fire on every delegated request and serve
        # the whole table — the failure that looks most like success, because
        # the bypass is correct in the case it was written for.
        if principals:
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
            logger.error(f"REC Registry request failed: {e}")
            raise

        if not assets:
            raise HTTPException(500, "Failed to enumerate user assets")

        user_device_ids: list[str] = []
        for asset in assets.items:
            if asset.sensor_id:
                user_device_ids.append(asset.sensor_id)

        logger.debug(f"User {user.sub} assets {user_device_ids}")

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
