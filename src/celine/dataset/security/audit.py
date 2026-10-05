"""The access audit: who read which dataset, and who was refused (GS-01 – GS-03).

Records are written by `celine.sdk.audit` on the `celine.audit` logger, one JSON
line each, in the shape every CELINE service shares. This module adds what is
particular to this service: one record per data request, naming the datasets the
statement resolved to, and the short reason code a refusal carries.
"""
from __future__ import annotations

from typing import Any, Self

from celine.sdk.audit import ERROR, audit_access, audit_denied
from fastapi import HTTPException
from starlette.exceptions import HTTPException as StarletteHTTPException

SERVICE = "dataset-api"

#: A statement read through `/query` or a DPS pull.
QUERY = "dataset.query"
#: A sample read by the conformance check.
CONFORMANCE = "dataset.conformance"
#: `POST /admin/catalogue`.
CATALOGUE_IMPORT = "catalogue.import"
#: A bearer token this service refused before any route ran.
AUTHENTICATE = "auth.token"
#: Control-plane signalling on the DPS data plane.
DPS_SIGNAL = "dps.signal"


class Refused(HTTPException):
    """An HTTP refusal that names its audit reason (GS-02).

    `reason` is a short code, never the detail: a detail may quote the caller's
    statement. A `400` raised this way is a refusal by the SQL guard or the
    catalogue gate and is recorded as `denied`, not as an error.
    """

    def __init__(self, status_code: int, detail: Any = None, *, reason: str) -> None:
        super().__init__(status_code=status_code, detail=detail)
        self.reason = reason


def dataspace_caller(consumer_id: str | None) -> dict[str, str] | None:
    """The caller of a dataspace request: the consumer participant, as verified.

    The EDR or pull token's `aud`, never a header. A participant id is a platform
    identifier, not a person's.
    """
    if not consumer_id:
        return None
    return {"sub": consumer_id, "client_id": consumer_id}


class ReadAudit:
    """One audit record for one data request (GS-01).

    Used as a context manager around the request's work. The executor sets
    `datasets` once the statement's references are resolved; the record is written
    on exit, from the outcome:

    - no exception: `access` / `allowed`, `resource` the datasets read
    - a `Refused`: `denied` with its reason
    - another 401 or 403: `denied`, `http <status>`
    - any other HTTP error or exception: `access` / `error`
    """

    def __init__(self, action: str, *, request: Any = None, caller: Any = None) -> None:
        self.action = action
        self.request = request
        self.caller = caller
        self.datasets: list[str] = []

    @property
    def resource(self) -> str | None:
        # The catalogue ids, sorted and joined: one record names every dataset a
        # join read. Catalogue ids are platform identifiers, never personal data.
        return ",".join(sorted(set(self.datasets))) or None

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        kwargs = {
            "caller": self.caller,
            "resource": self.resource,
            "service": SERVICE,
            "request": self.request,
        }
        if exc is None:
            audit_access(self.action, **kwargs)
        elif isinstance(exc, Refused):
            audit_denied(self.action, reason=exc.reason, **kwargs)
        elif isinstance(exc, StarletteHTTPException):
            code = f"http {exc.status_code}"
            if exc.status_code in (401, 403):
                audit_denied(self.action, reason=code, **kwargs)
            else:
                audit_access(self.action, outcome=ERROR, reason=code, **kwargs)
        elif isinstance(exc, Exception):
            audit_access(self.action, outcome=ERROR, reason=type(exc).__name__, **kwargs)
        return False


def token_refused(request: Any, exc: StarletteHTTPException) -> None:
    """A presented bearer token that did not verify (GS-02).

    The caller stays unnamed: nothing in a token that failed verification can be
    trusted, its `sub` included.
    """
    audit_denied(
        AUTHENTICATE,
        reason="invalid_token" if exc.status_code == 401 else f"http {exc.status_code}",
        service=SERVICE,
        request=request,
    )
