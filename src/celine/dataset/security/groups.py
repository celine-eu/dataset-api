"""Which of a caller's groups this service authorizes on.

A token carries groups at two levels: the realm's top-level ``groups`` claim, and
``organization.<alias>.groups`` for every organization the caller belongs to. The
SDK's ``extract_groups`` merges the two into one list, and that merge is what
made a community's own ``admins`` the platform's ``admins`` here: skipping every
row filter, reading ``restricted`` datasets and writing the catalogue — so one
community's operator could read every community's meter data.

**``admins`` is honoured at realm level only.** An ``admins`` group held inside
an organization grants nothing here. ``managers`` and ``viewers`` are still read
from both levels, because members are placed in their community organization's
``viewers`` group and no realm group, and the Digital Twin and the assistant
query with the member's own token; row filters still apply to them. Whether an
organization-level group should reach datasets outside that organization is an
open question with its own plan, not something this module decides.
"""

from __future__ import annotations

from typing import Any, Mapping

from celine.sdk.auth.jwt import organization_aliases, organization_groups, realm_groups

#: The platform administrator group.
ADMIN_GROUP = "admins"

#: Groups that grant platform-wide power and are therefore read from the realm
#: level only — never from inside an organization.
REALM_ONLY_GROUPS = frozenset({ADMIN_GROUP})


def authorization_groups(claims: Mapping[str, Any]) -> list[str]:
    """Realm groups, then organization groups other than the realm-only ones.

    Deduplicated, leading slashes stripped, first-seen order preserved.
    """
    result = list(realm_groups(dict(claims)))
    seen = set(result)
    for alias in organization_aliases(dict(claims)):
        for group in organization_groups(dict(claims), alias):
            if group in REALM_ONLY_GROUPS or group in seen:
                continue
            seen.add(group)
            result.append(group)
    return result


def is_realm_admin(claims: Mapping[str, Any]) -> bool:
    """True only for a realm-level ``admins`` member."""
    return ADMIN_GROUP in realm_groups(dict(claims))
