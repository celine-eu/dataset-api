from enum import Enum
from dataclasses import dataclass


@dataclass(frozen=True)
class AccessLevelPolicy:
    requires_auth: bool
    requires_policy: bool


class AccessLevel(str, Enum):
    OPEN = "open"
    INTERNAL = "internal"
    RESTRICTED = "restricted"

    @classmethod
    def from_value(cls, value: str | None) -> "AccessLevel":
        if not value:
            return cls.OPEN
        try:
            return cls(value.lower())
        except ValueError as exc:
            raise ValueError(f"Invalid disclosure level: {value}") from exc


ACCESS_LEVEL_MATRIX: dict[AccessLevel, AccessLevelPolicy] = {
    AccessLevel.OPEN: AccessLevelPolicy(False, False),
    AccessLevel.INTERNAL: AccessLevelPolicy(True, True),
    AccessLevel.RESTRICTED: AccessLevelPolicy(True, True),
}


def requires_auth(access_level: str | None) -> bool:
    """
    Returns True if the given disclosure level requires authentication.

    Used by API-layer dependencies to decide whether anonymous access
    is acceptable.
    """
    level = AccessLevel.from_value(access_level)
    return ACCESS_LEVEL_MATRIX[level].requires_auth
