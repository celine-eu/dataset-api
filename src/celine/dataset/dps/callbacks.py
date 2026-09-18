"""Data plane → control plane notifications (DPS "Control Plane Endpoint").

`POST {base}/transfers/{id}/dataflow/{prepared|started|completed|errored}` with a
`DataFlowStatusMessage`. EDC 0.18.0 serves these on its `signaling` context
(`DataPlaneTransferApiController`) and admits a caller only if its
authorization profile yields the flow's data plane id
(`DataPlaneTransferAuthorizationFilter`).

The synchronous data plane in `service` does not need `prepared` or `started`;
`errored` and `completed` are what a long-running flow would send.
"""
from __future__ import annotations

from typing import Any, Awaitable, Callable, Optional

import httpx

ACTIONS = frozenset({"prepared", "started", "completed", "errored"})

TokenSource = Callable[[], Awaitable[Optional[str]]]


class CallbackFailed(Exception):
    pass


class ControlPlaneClient:
    def __init__(
        self,
        token_source: TokenSource,
        *,
        transport: Optional[httpx.AsyncBaseTransport] = None,
        timeout: float = 5.0,
    ):
        self._token_source = token_source
        self._transport = transport
        self._timeout = timeout

    async def notify(
        self,
        base_url: Optional[str],
        flow_id: str,
        action: str,
        message: Optional[dict[str, Any]] = None,
    ) -> None:
        if action not in ACTIONS:
            raise ValueError(f"unknown callback {action!r}")
        if not base_url:
            raise CallbackFailed(f"no control plane endpoint known for data flow {flow_id}")
        url = f"{base_url.rstrip('/')}/transfers/{flow_id}/dataflow/{action}"
        headers = {}
        token = await self._token_source()
        if token:
            headers["Authorization"] = f"Bearer {token}"
        try:
            async with httpx.AsyncClient(transport=self._transport, timeout=self._timeout) as client:
                response = await client.post(url, json=message or {}, headers=headers)
        except httpx.HTTPError as exc:
            raise CallbackFailed(f"{action} for {flow_id}: {exc}") from exc
        if response.status_code >= 300:
            raise CallbackFailed(f"{action} for {flow_id}: HTTP {response.status_code}")
