from __future__ import annotations

import time
from typing import Any, Optional

from celine.dataset.security.models import AuthenticatedUser

ADMIN_GROUPS = {"admins"}


def is_admin_user(user: Optional[AuthenticatedUser]) -> bool:
    if user is None:
        return False
    groups = user.claims.get("groups", [])
    if not isinstance(groups, list):
        groups = []
    return bool(ADMIN_GROUPS & set(groups))


def token_ttl_seconds(user: Optional[AuthenticatedUser]) -> Optional[int]:
    """Return remaining TTL (seconds) based on JWT exp claim, if present."""
    if user is None:
        return None
    exp_claim = user.claims.get("exp")
    if exp_claim is None:
        return None
    try:
        exp_ts = int(exp_claim)
    except Exception:
        return None
    now = int(time.time())
    remaining = exp_ts - now
    if remaining <= 0:
        return 0
    return remaining
