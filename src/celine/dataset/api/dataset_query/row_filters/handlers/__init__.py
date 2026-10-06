from __future__ import annotations

from .direct_user_match import DirectUserMatchHandler
from .http_in_list import HttpInListHandler
from .member_wide import MemberWideHandler
from .organization_match import OrganizationMatchHandler
from .subject_key_match import SubjectKeyMatchHandler
from .table_pointer import TablePointerHandler
from .rec_registry import RecRegistryHandler

__all__ = [
    "HANDLER_BINDS",
    "DirectUserMatchHandler",
    "HttpInListHandler",
    "MemberWideHandler",
    "OrganizationMatchHandler",
    "SubjectKeyMatchHandler",
    "TablePointerHandler",
    "RecRegistryHandler",
]

#: What each built-in handler binds rows to, from the handler itself. A governance
#: `binds` that disagrees is refused at catalogue import (GS-08).
HANDLER_BINDS: dict[str, str] = {
    h.name: h.binds
    for h in (
        DirectUserMatchHandler,
        HttpInListHandler,
        MemberWideHandler,
        OrganizationMatchHandler,
        SubjectKeyMatchHandler,
        TablePointerHandler,
        RecRegistryHandler,
    )
}
