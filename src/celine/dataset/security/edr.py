"""EDR (Endpoint Data Reference) PEP — dataspace-aware access enforcement.

The EDC data plane already validates the EDR JWT before proxying a request to
the Dataset API, so we do NOT re-validate the signature here.  What we DO
enforce:

1. Agreement is still active  (``/internal/agreements/{id}/status``)
2. If the dataset carries a ``userFilterColumn``, resolve the set of
   consented subject IDs from ds-connector
   (``/internal/consent/check?dataset_id=…&consumer_id=…``).

An empty ``subject_ids`` list means consent was required but none is granted
→ the executor will produce a ``deny`` row-filter plan (zero rows returned).
``subject_ids=None`` means consent is not required for this dataset → no row
filter is injected.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional

import httpx
from fastapi import HTTPException

from celine.dataset.core.config import settings

logger = logging.getLogger(__name__)


@dataclass
class EDRRequestContext:
    """Parsed EDC headers attached to an incoming EDR-proxied request."""

    agreement_id: str
    consumer_id: str  # Edc-Bpn or equivalent; empty string when absent


@dataclass
class EDRAuthResult:
    """Outcome of a PEP check for one dataset within an EDR request."""

    agreement_id: str
    consumer_id: str
    # None  → no row filtering needed (dataset has no userFilterColumn)
    # []    → consent required but none granted → deny all rows
    # [...]  → filter rows to these subject IDs
    subject_ids: Optional[list[str]] = field(default=None)


async def edr_pep_check(
    *,
    agreement_id: str,
    consumer_id: str,
    dataset_id: str,
    user_filter_column: Optional[str],
) -> EDRAuthResult:
    """Validate an EDR request against ds-connector and resolve row-filter IDs.

    Raises ``HTTPException(403)`` if the agreement is not active.
    Raises ``HTTPException(503)`` if ds-connector is not configured or unreachable.
    """
    if not settings.connector_internal_url:
        raise HTTPException(
            503,
            "EDR PEP is enabled but CONNECTOR_INTERNAL_URL is not configured",
        )

    base = settings.connector_internal_url.rstrip("/")

    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            # 1 — Check agreement status
            resp = await client.get(
                f"{base}/internal/agreements/{agreement_id}/status"
            )
            if resp.status_code == 404:
                raise HTTPException(
                    403, f"Agreement {agreement_id!r} not found or not tracked"
                )
            resp.raise_for_status()

            status_data = resp.json()
            if status_data.get("status") != "active":
                raise HTTPException(
                    403,
                    f"Agreement {agreement_id!r} is not active "
                    f"(status={status_data.get('status')!r})",
                )

            # 2 — Consent / row-filter resolution (only when dataset uses it)
            if not user_filter_column:
                return EDRAuthResult(
                    agreement_id=agreement_id,
                    consumer_id=consumer_id,
                    subject_ids=None,
                )

            resp = await client.get(
                f"{base}/internal/consent/check",
                params={"dataset_id": dataset_id, "consumer_id": consumer_id},
            )
            resp.raise_for_status()
            data = resp.json()
            subject_ids: list[str] = data.get("subject_ids", [])

            logger.debug(
                "EDR consent check dataset=%s consumer=%s subject_ids=%s",
                dataset_id,
                consumer_id,
                subject_ids,
            )
            return EDRAuthResult(
                agreement_id=agreement_id,
                consumer_id=consumer_id,
                subject_ids=subject_ids,
            )

    except HTTPException:
        raise
    except httpx.HTTPStatusError as exc:
        logger.error("ds-connector returned %s for EDR PEP", exc.response.status_code)
        raise HTTPException(
            502, f"ds-connector error: {exc.response.status_code}"
        ) from exc
    except httpx.RequestError as exc:
        logger.error("ds-connector unreachable for EDR PEP: %s", exc)
        raise HTTPException(502, "ds-connector unreachable") from exc
