from enum import Enum
from dataclasses import dataclass


@dataclass(frozen=True)
class DisclosurePolicy:
    requires_auth: bool
    requires_policy: bool


class DisclosureLevel(str, Enum):
    OPEN = "open"
    INTERNAL = "internal"
    RESTRICTED = "restricted"

    @classmethod
    def from_value(cls, value: str | None) -> "DisclosureLevel":
        if not value:
            return cls.OPEN
        try:
            return cls(value.lower())
        except ValueError as exc:
            raise ValueError(f"Invalid disclosure level: {value}") from exc


DISCLOSURE_MATRIX: dict[DisclosureLevel, DisclosurePolicy] = {
    DisclosureLevel.OPEN: DisclosurePolicy(False, False),
    DisclosureLevel.INTERNAL: DisclosurePolicy(True, True),
    DisclosureLevel.RESTRICTED: DisclosurePolicy(True, True),
}


def requires_auth(access_level: str | None) -> bool:
    """
    Returns True if the given disclosure level requires authentication.

    Used by API-layer dependencies to decide whether anonymous access
    is acceptable.
    """
    level = DisclosureLevel.from_value(access_level)
    return DISCLOSURE_MATRIX[level].requires_auth
