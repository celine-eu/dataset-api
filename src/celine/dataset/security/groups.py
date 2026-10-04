"""What a caller is granted here, read at the level it was granted at.

A token carries grants at exactly two levels, and this module never mixes them:

- **Platform.** The realm role ``platform-admin`` (``realm_access.roles``) is the
  only platform-wide grant. Its holder skips every row filter, reads
  ``restricted`` datasets and writes the catalogue.
- **Organization.** ``organization.<alias>.groups`` are the groups a caller holds
  inside one organization. ``managers`` and ``viewers`` read ``internal``
  datasets, with row filters still applied. An organization's ``admins`` and
  ``editors`` grant nothing here. Whether an organization's group should reach
  datasets beyond its own organization is an open question with its own plan,
  not something this module decides.

**A realm group grants nothing.** The top-level ``groups`` claim is never read,
so a token still carrying ``/admins`` there is not an administrator. An
organization's ``admins`` is not the platform's either: Keycloak gives both the
same ``/admins`` path, which is why the platform level is a role whose name no
organization group carries.
"""

from __future__ import annotations

from typing import Any, Mapping

from celine.sdk.auth import PLATFORM_ADMIN_ROLE, Grants, realm_roles
from celine.sdk.auth import is_platform_admin as _sdk_is_platform_admin

__all__ = [
    "PLATFORM_ADMIN_ROLE",
    "is_platform_admin",
    "organization_groups_held",
    "platform_roles",
]


def platform_roles(claims: Mapping[str, Any]) -> list[str]:
    """The caller's realm roles, from ``realm_access.roles`` only."""
    return realm_roles(dict(claims))


def is_platform_admin(claims: Mapping[str, Any]) -> bool:
    """True exactly when the caller holds the realm role ``platform-admin``."""
    return _sdk_is_platform_admin(dict(claims))


def organization_groups_held(claims: Mapping[str, Any]) -> list[str]:
    """The groups the caller holds inside at least one of its organizations.

    Organization groups only, never a realm group or a realm role. Sorted by
    organization alias, then by group, and deduplicated.

    The list says *which* groups, not *where*: a ``viewers`` held in one
    organization reads like a ``viewers`` held in another. That is today's
    reading of ``internal`` datasets, where row filters narrow the rows. It stays
    until organization-scoped access is decided. Never derive anything
    platform-wide from this list.
    """
    grants = Grants.from_claims(dict(claims))
    result: list[str] = []
    for alias in grants.aliases:
        for group in sorted(grants.in_org(alias)):
            if group not in result:
                result.append(group)
    return result
