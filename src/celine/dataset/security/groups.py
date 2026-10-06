"""What a caller is granted here, read at the level it was granted at.

A token carries grants at exactly two levels, and this module never mixes them:

- **Platform.** The realm role ``platform-admin`` (``realm_access.roles``) is the
  only platform-wide grant. Its holder skips every row filter, reads
  ``restricted`` datasets and writes the catalogue.
- **Organization.** ``organization.<alias>.groups`` are the groups a caller holds
  inside one organization. ``admins``, ``managers`` and ``viewers`` read
  ``internal`` datasets (an organization's ``admins`` reads what its ``managers``
  read), and only the rows of the organization they are held in: a dataset's
  row filter does the scoping (``organization_match`` matches the alias against
  a column), and an ``internal`` dataset that declares no row filter is not
  readable through an organization at all (GS-06, GS-07 in
  ``docs/governance-security.md``).

**A realm group grants nothing.** The top-level ``groups`` claim is never read,
so a token still carrying ``/admins`` there is not an administrator. An
organization's ``admins`` is not the platform's either: Keycloak gives both the
same ``/admins`` path, which is why the platform level is a role whose name no
organization group carries.
"""

from __future__ import annotations

from typing import Any, Mapping, Optional

from celine.sdk.auth import PLATFORM_ADMIN_ROLE, Grants, Organization, realm_roles
from celine.sdk.auth import is_platform_admin as _sdk_is_platform_admin

__all__ = [
    "PLATFORM_ADMIN_ROLE",
    "READING_GROUPS",
    "is_platform_admin",
    "organization_groups_held",
    "platform_roles",
    "reading_organizations",
]

#: The organization groups that read ``internal`` datasets, inside their own
#: organization only. ``admins`` reads what ``managers`` read; ``editors`` grants
#: nothing here.
READING_GROUPS: frozenset[str] = frozenset({"admins", "managers", "viewers"})


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
    organization reads like a ``viewers`` held in another. It is the policy's
    coarse gate only — "a reading group somewhere" — and is safe as such because
    the policy admits an organization's reader only to a dataset that declares a
    row filter, and the row filter decides *where*
    (:func:`reading_organizations`). Never derive anything platform-wide, or any
    organization's rows, from this list.
    """
    grants = Grants.from_claims(dict(claims))
    result: list[str] = []
    for alias in grants.aliases:
        for group in sorted(grants.in_org(alias)):
            if group not in result:
                result.append(group)
    return result


def reading_organizations(
    claims: Mapping[str, Any], *, org_type: Optional[str] = None
) -> list[str]:
    """The aliases of the organizations where the caller holds a reading group.

    Sorted. With *org_type*, only organizations of that type (``rec``, ``dso``),
    read from the token's ``organization.<alias>.type`` as celine-sdk parses it.

    A caller in two organizations gets both: an operator of two communities reads
    both communities' rows. A member released from a community stops holding its
    organization when the token is next issued, not before — the token lifetime
    is the window.
    """
    grants = Grants.from_claims(dict(claims))
    aliases = [a for a in grants.aliases if grants.in_org(a) & READING_GROUPS]
    if org_type is None:
        return aliases
    raw = claims.get("organization")
    if not isinstance(raw, Mapping):
        return []
    # The SDK's own parser, so a type nested under `attributes` by a differently
    # configured mapper reads the same as the realm's flattened one.
    return [
        a
        for a in aliases
        if Organization._from_claim(a, raw.get(a)).type == org_type
    ]
