from __future__ import annotations

import time
from typing import Any, Optional

from celine.dataset.security.models import AuthenticatedUser
from celine.dataset.security.groups import is_realm_admin


def is_admin_user(user: Optional[AuthenticatedUser]) -> bool:
    """True for a realm-level `admins` member, whom row filters do not narrow.

    An organization's own `admins` group is not a platform administrator: it is
    the community's operator, and must see only what its row filters allow.
    """
    if user is None:
        return False
    return is_realm_admin(user.claims)


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
