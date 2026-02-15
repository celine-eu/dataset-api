# src/celine/dataset/security/governance.py
"""
Dataset access governance using in-process policy evaluation.

This module provides dataset-specific authorization logic using
the celine-sdk in-process policy engine instead of HTTP client.
"""
from __future__ import annotations

import logging
import time
from typing import Optional

from fastapi import HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from celine.dataset.security.disclosure import AccessLevel, ACCESS_LEVEL_MATRIX
from celine.dataset.db.models.dataset_entry import DatasetEntry
from celine.dataset.security.models import AuthenticatedUser
from celine.dataset.core.config import settings

# Import from celine-sdk (in-process policies)
from celine.sdk.policies import (
    Action,
    CachedPolicyEngine,
    PolicyEngine,
    PolicyEngineError,
    PolicyInput,
    Resource,
    ResourceType,
    Subject,
    SubjectType,
)

logger = logging.getLogger(__name__)

# Global policy engine instance
_policy_engine: Optional[CachedPolicyEngine] = None


def _get_policy_engine() -> Optional[CachedPolicyEngine]:
    """
    Get or create the policy engine singleton.

    Returns None if policies are disabled.
    """
    global _policy_engine

    if not settings.policies_check_enabled:
        return None

    if _policy_engine is None:
        try:
            # Create base engine
            engine = PolicyEngine(
                policies_dir=settings.policies_dir,
                data_dir=settings.policies_data_dir,
            )
            engine.load()

            # Wrap with cache
            _policy_engine = CachedPolicyEngine(
                engine=engine,
                enabled=settings.policies_cache_enabled,
            )

            logger.info(
                f"Policy engine initialized: "
                f"{engine.policy_count} policies loaded, "
                f"packages: {engine.get_packages()}"
            )

        except Exception as e:
            logger.error(f"Failed to initialize policy engine: {e}")
            # Don't cache the failure - return None and allow retry
            return None

    return _policy_engine


def _build_subject_from_user(user: Optional[AuthenticatedUser]) -> Subject:
    """
    Build Subject from AuthenticatedUser.

    Args:
        user: Authenticated user (None for anonymous)

    Returns:
        Subject for policy evaluation
    """
    if user is None:
        return Subject.anonymous()

    # Extract scopes from user claims
    scopes = user.claims.get("scope", "")
    if isinstance(scopes, str):
        scopes = scopes.split()
    elif not isinstance(scopes, list):
        scopes = []

    # Extract groups from user claims
    groups = user.claims.get("groups", [])
    if not isinstance(groups, list):
        groups = []

    if _is_service_account(user.claims):
        subject_type = SubjectType.SERVICE
    else:
        subject_type = SubjectType.USER

    return Subject(
        id=user.sub,
        type=subject_type,
        groups=groups,
        scopes=scopes,
        claims=user.claims,
    )


def _is_service_account(claims: dict) -> bool:
    # client have scopes, users have groups
    return claims.get("scopes", None) is not None


async def enforce_dataset_access(
    *,
    entry: DatasetEntry,
    user: Optional[AuthenticatedUser],
) -> None:
    """
    Final access-control gate for dataset usage.

    This enforces the dataset's access level policy:
    1. Check if authentication is required
    2. Evaluate authorization policy if required

    Raises:
        HTTPException: 401 if auth required but missing, 403 if policy denies,
                      503 if policy service unavailable
    """

    # Parse access level
    try:
        level = AccessLevel.from_value(entry.access_level)
    except ValueError as exc:
        logger.warning(f"Failed to parse access_level={entry.access_level}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Invalid dataset access level configuration",
        ) from exc

    policy = ACCESS_LEVEL_MATRIX[level]

    # Step 1 — Authentication check
    if policy.requires_auth and user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required for this dataset",
        )

    # Step 2 — Policy evaluation
    if policy.requires_policy:

        # Get policy engine
        engine = _get_policy_engine()

        # If policies are disabled, log warning and allow
        if not settings.policies_check_enabled:
            logger.warning(
                "Policies disabled, allowing access to dataset %s",
                entry.dataset_id,
            )
            return

        # If engine failed to initialize, return 503
        if engine is None:
            logger.error("Failed to create policy engine")
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Policy engine unavailable",
            )

        # Build resource attributes
        resource_attributes = {
            "access_level": entry.access_level,
            "backend_type": entry.backend_type,
        }

        # Add namespace if available
        if entry.lineage:
            namespace = entry.lineage.get("namespace")
            if namespace:
                resource_attributes["namespace"] = namespace

        if entry.lineage:
            governance = entry.lineage.get("facets", {}).get("governance")
            if governance:
                resource_attributes["governance"] = {
                    k: v for k, v in governance.items() if not k.startswith("_")
                }

        # Build subject
        subject = _build_subject_from_user(user)

        # Build policy input
        policy_input = PolicyInput(
            subject=subject,
            resource=Resource(
                type=ResourceType.DATASET,
                id=entry.dataset_id,
                attributes=resource_attributes,
            ),
            action=Action(
                name="read",  # Dataset query is a read action
                context={},
            ),
            environment={
                "timestamp": time.time(),
                "source_service": "dataset-api",
            },
        )

        # Evaluate policy
        try:
            decision = engine.evaluate_decision(
                policy_package=settings.policies_package,
                policy_input=policy_input,
            )

            if not decision.allowed:
                logger.info(
                    "Access denied by policy for dataset %s: %s",
                    entry.dataset_id,
                    decision.reason,
                    extra={
                        "user": user.sub if user else "anonymous",
                        "dataset_id": entry.dataset_id,
                        "reason": decision.reason,
                        "policy": decision.policy,
                    },
                )
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=decision.reason or "Access denied by policy",
                )

            # Log based on cache status
            if decision.cached:
                logger.debug(
                    "Access allowed by policy (cached) for dataset %s",
                    entry.dataset_id,
                    extra={
                        "user": user.sub if user else "anonymous",
                        "dataset_id": entry.dataset_id,
                        "cached": True,
                    },
                )
            else:
                logger.info(
                    "Access allowed by policy for dataset %s: %s",
                    entry.dataset_id,
                    decision.reason,
                    extra={
                        "user": user.sub if user else "anonymous",
                        "dataset_id": entry.dataset_id,
                        "reason": decision.reason,
                        "policy": decision.policy,
                    },
                )

        except PolicyEngineError as e:
            logger.error(
                "Policy evaluation failed for dataset %s: %s",
                entry.dataset_id,
                str(e),
            )
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Policy evaluation failed",
            ) from e
        except Exception as e:
            logger.error(
                "Unexpected error during policy evaluation for dataset %s: %s",
                entry.dataset_id,
                str(e),
                exc_info=True,
            )
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Policy evaluation failed",
            ) from e


async def resolve_datasets_for_tables(
    *,
    db: AsyncSession,
    table_names: set[str],
) -> dict[str, DatasetEntry]:
    """
    Resolve dataset IDs referenced in SQL to DatasetEntry objects.

    table_names are logical dataset IDs (not physical table names).

    Returns:
        Dictionary mapping dataset ID to DatasetEntry

    Raises:
        HTTPException: 400 if no datasets referenced or if datasets not found
    """

    if not table_names:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No datasets referenced in query",
        )

    # Query database for datasets
    stmt = select(DatasetEntry).where(DatasetEntry.dataset_id.in_(table_names))
    res = await db.execute(stmt)
    entries = res.scalars().all()

    # Build lookup map
    by_id = {e.dataset_id: e for e in entries}

    # Check for missing datasets
    missing = table_names - by_id.keys()
    if missing:
        logger.warning(
            "Query references unknown datasets: %s",
            sorted(missing),
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Query references unknown datasets: {sorted(missing)}",
        )

    return by_id


def get_policy_stats() -> dict:
    """
    Get policy engine statistics.

    Returns:
        Dictionary with engine stats
    """
    if _policy_engine is None:
        return {"enabled": False}

    return {
        "enabled": True,
        "policy_count": _policy_engine.policy_count,
        "packages": _policy_engine.get_packages(),
        "cache_stats": (
            _policy_engine.cache_stats
            if hasattr(_policy_engine, "cache_stats")
            else None
        ),
    }
