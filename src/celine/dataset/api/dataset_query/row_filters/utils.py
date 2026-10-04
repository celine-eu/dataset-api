from __future__ import annotations

import time
from typing import Any, Optional

from celine.dataset.security.models import AuthenticatedUser
from celine.dataset.security.groups import is_platform_admin


def is_admin_user(user: Optional[AuthenticatedUser]) -> bool:
    """True for a holder of the realm role `platform-admin`: no row filter narrows it.

    An organization's own `admins` group is not a platform administrator: it is
    the community's operator, and sees only what its row filters allow. A realm
    group still present in a token (`groups: ["/admins"]`) grants nothing.
    """
    if user is None:
        return False
    return is_platform_admin(user.claims)


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
