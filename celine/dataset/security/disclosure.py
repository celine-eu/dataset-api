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
