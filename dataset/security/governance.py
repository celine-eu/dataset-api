from typing import Any, Dict, Optional

from fastapi import HTTPException, status

from dataset.security.disclosure import DisclosureLevel, DISCLOSURE_MATRIX
from dataset.db.models.dataset_entry import DatasetEntry
from dataset.security.opa import OPAClient
from dataset.core.config import settings


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
    user: Optional[Dict[str, Any]],
) -> None:
    """
    Final access-control gate for dataset usage.
    """

    try:
        level = DisclosureLevel.from_value(entry.access_level)
    except ValueError as exc:
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
        if opa is None:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Policy engine unavailable",
            )

        input_doc = {
            "dataset": {
                "id": entry.dataset_id,
                "disclosure_level": level.value,
                "governance": entry.governance or {},
            },
            "user": user,
        }

        allowed = await opa.evaluate(input_doc)
        if not allowed:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access denied by policy",
            )
