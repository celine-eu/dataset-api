from __future__ import annotations

from .direct_user_match import DirectUserMatchHandler
from .http_in_list import HttpInListHandler
from .layer_verdicts import AllowAllHandler, DenyAllHandler
from .member_wide import MemberWideHandler
from .organization_match import OrganizationMatchHandler
from .subject_key_match import SubjectKeyMatchHandler
from .table_pointer import TablePointerHandler
from .rec_registry import RecRegistryHandler

__all__ = [
    "HANDLER_BINDS",
    "AllowAllHandler",
    "DenyAllHandler",
    "DirectUserMatchHandler",
    "HttpInListHandler",
    "MemberWideHandler",
    "OrganizationMatchHandler",
    "SubjectKeyMatchHandler",
    "TablePointerHandler",
    "RecRegistryHandler",
]

#: What each built-in handler binds rows to, from the handler itself. A governance
#: `binds` that disagrees is refused at catalogue import (GS-08). `None` for the two
#: layer verdicts, which bind whichever layer their own `binds` names (GS-10).
HANDLER_BINDS: dict[str, str | None] = {
    h.name: h.binds
    for h in (
        AllowAllHandler,
        DenyAllHandler,
        DirectUserMatchHandler,
        HttpInListHandler,
        MemberWideHandler,
        OrganizationMatchHandler,
        SubjectKeyMatchHandler,
        TablePointerHandler,
        RecRegistryHandler,
    )
}
