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
    SECRET = "secret"

    @classmethod
    def from_value(cls, value: str | None) -> "AccessLevel":
        """Parse a stored level. **An entry that states none is `internal`.**

        It used to be `open`, so a dataset whose governance forgot the field was
        served to anonymous callers — while the catalogue's ODRL offer advertised
        the same dataset as `internal`. Unset now takes the safe side, the one
        the catalogue already showed.
        """
        if not value:
            return cls.INTERNAL
        try:
            return cls(value.lower())
        except ValueError as exc:
            raise ValueError(f"Invalid disclosure level: {value}") from exc


#: The levels a catalogue entry may declare, as the import validates them.
ACCESS_LEVELS = frozenset(level.value for level in AccessLevel)


ACCESS_LEVEL_MATRIX: dict[AccessLevel, AccessLevelPolicy] = {
    AccessLevel.OPEN: AccessLevelPolicy(False, False),
    AccessLevel.INTERNAL: AccessLevelPolicy(True, True),
    AccessLevel.RESTRICTED: AccessLevelPolicy(True, True),
    # Never reached: `is_available` refuses a secret dataset before any policy.
    AccessLevel.SECRET: AccessLevelPolicy(True, True),
}


def is_available(expose: bool | None, access_level: str | None) -> bool:
    """May this dataset be queried at all, by anyone?

    The query-side twin of `core.datasets.catalogue_visible`: listed (`expose`)
    and not `secret`. A level this service cannot read is not available either —
    an unreadable level is a misconfiguration, and the safe reading of it is
    "nobody". Every refusal here answers the same `403 Dataset not available`,
    so a caller cannot tell a secret dataset from an unexposed one.
    """
    if not expose:
        return False
    try:
        return AccessLevel.from_value(access_level) is not AccessLevel.SECRET
    except ValueError:
        return False


def requires_auth(access_level: str | None) -> bool:
    """
    Returns True if the given disclosure level requires authentication.

    Used by API-layer dependencies to decide whether anonymous access
    is acceptable.
    """
    level = AccessLevel.from_value(access_level)
    return ACCESS_LEVEL_MATRIX[level].requires_auth
