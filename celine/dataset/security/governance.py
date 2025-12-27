import logging
from typing import Any, Dict, Optional

from fastapi import HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from celine.dataset.security.disclosure import DisclosureLevel, DISCLOSURE_MATRIX
from celine.dataset.db.models.dataset_entry import DatasetEntry
from celine.dataset.security.models import AuthenticatedUser
from celine.dataset.security.opa import OPAClient
from celine.dataset.core.config import settings

logger = logging.getLogger(__name__)

_opa_client: Optional[OPAClient] = None


def _get_opa_client() -> Optional[OPAClient]:
    global _opa_client
    if not settings.opa_enabled:
        return None

    if _opa_client is None:
        _opa_client = OPAClient(
            base_url=settings.opa_url,
            policy_path=settings.opa_policy_path,
        )

    return _opa_client


async def enforce_dataset_access(
    *,
    entry: DatasetEntry,
    user: Optional[AuthenticatedUser],
) -> None:
    """
    Final access-control gate for dataset usage.
    """

    try:
        level = DisclosureLevel.from_value(entry.access_level)
    except ValueError as exc:
        logger.warning(f"Failed to parse access_level={entry.access_level}")
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    policy = DISCLOSURE_MATRIX[level]

    # Step 1 — authentication
    if policy.requires_auth and user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required for this dataset",
        )

    # Step 2 — policy evaluation
    if policy.requires_policy:
        opa = _get_opa_client()

        if not settings.opa_enabled:
            logger.warning("OPA disabled, all request are allowed.")
            return

        if opa is None:
            logger.warning(f"Failed to create opa client")
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Policy engine unavailable",
            )

        allowed = await opa.evaluate(dataset=entry, user=user)
        if not allowed:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access denied by policy",
            )


async def resolve_datasets_for_tables(
    *,
    db: AsyncSession,
    table_names: set[str],
) -> dict[str, DatasetEntry]:
    """
    Resolve dataset IDs referenced in SQL to DatasetEntry objects.

    table_names are logical dataset IDs (not physical table names).
    """

    if not table_names:
        raise HTTPException(400, "No datasets referenced in query")

    stmt = select(DatasetEntry).where(DatasetEntry.dataset_id.in_(table_names))
    res = await db.execute(stmt)
    entries = res.scalars().all()

    by_id = {e.dataset_id: e for e in entries}

    missing = table_names - by_id.keys()
    if missing:
        raise HTTPException(
            400,
            f"Query references unknown datasets: {sorted(missing)}",
        )

    return by_id
