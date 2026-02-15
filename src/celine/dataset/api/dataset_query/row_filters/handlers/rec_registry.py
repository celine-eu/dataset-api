from __future__ import annotations

import logging
import token
from typing import Any

from fastapi import HTTPException
from sqlglot import exp

from celine.dataset.api.dataset_query.row_filters.models import RowFilterPlan
from celine.dataset.core.config import settings
from celine.dataset.security.models import AuthenticatedUser

from celine.sdk.rec_registry import RecRegistryUserClient

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
    ) -> RowFilterPlan:

        base_url = args.get("url") or settings.rec_registry_url
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
