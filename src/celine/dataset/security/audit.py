"""The access audit: who read which dataset, and who was refused (GS-01 – GS-03).

Records are written by `celine.sdk.audit` on the `celine.audit` logger, one JSON
line each, in the shape every CELINE service shares. This module adds what is
particular to this service: one record per dataset a request read or was refused,
all sharing the request's `request_id`, and the short reason code a refusal carries.
"""
from __future__ import annotations

import uuid
from typing import Any, Self

from celine.sdk.audit import ERROR, audit_access, audit_denied, request_fields
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
    """The audit records of one data request: one per dataset read (GS-01).

    Used as a context manager around the request's work. The executor sets
    `datasets` once the statement's references are resolved; the records are
    written on exit, one per dataset with `resource` that dataset's id, all
    carrying the same `request_id`. When no dataset resolved, one record is
    written with no `resource`. The outcome decides the event:

    - no exception: `access` / `allowed`
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
    def resources(self) -> list[str | None]:
        # Catalogue ids are platform identifiers, never personal data.
        return sorted(set(self.datasets)) or [None]

    def _request_id(self) -> str:
        # The caller's `X-Request-ID` when it sent a usable one; otherwise one is
        # minted, so the records of a join can still be told apart from another
        # request's.
        return request_fields(self.request)["request_id"] or uuid.uuid4().hex

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        if exc is not None and not isinstance(exc, Exception):
            return False
        if exc is None:
            write, extra = audit_access, {}
        elif isinstance(exc, Refused):
            write, extra = audit_denied, {"reason": exc.reason}
        elif isinstance(exc, StarletteHTTPException):
            code = f"http {exc.status_code}"
            if exc.status_code in (401, 403):
                write, extra = audit_denied, {"reason": code}
            else:
                write, extra = audit_access, {"outcome": ERROR, "reason": code}
        else:
            write, extra = audit_access, {"outcome": ERROR, "reason": type(exc).__name__}
        request_id = self._request_id()
        for resource in self.resources:
            write(
                self.action,
                caller=self.caller,
                resource=resource,
                service=SERVICE,
                request=self.request,
                request_id=request_id,
                **extra,
            )
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
