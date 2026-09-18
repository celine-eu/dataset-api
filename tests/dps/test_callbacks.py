"""DPS-12 — notifications to the control plane's signalling API."""
from __future__ import annotations

import httpx
import pytest

from celine.dataset.dps.callbacks import CallbackFailed, ControlPlaneClient
from celine.dataset.dps.messages import status_message


def _client(handler, token: str | None = "svc-token") -> ControlPlaneClient:
    async def _token():
        return token

    return ControlPlaneClient(_token, transport=httpx.MockTransport(handler))


async def test_a_notification_follows_edc_signaling_paths() -> None:
    seen: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        seen["auth"] = request.headers.get("authorization")
        seen["body"] = request.read()
        return httpx.Response(200)

    body = status_message("tp-1", "STARTED", error="boom")
    await _client(handler).notify("https://cp.example.org/api/signaling/", "tp-1", "errored", body)

    assert seen["url"] == "https://cp.example.org/api/signaling/transfers/tp-1/dataflow/errored"
    assert seen["auth"] == "Bearer svc-token"
    assert b'"error":"boom"' in seen["body"]


async def test_completed_sends_an_empty_object_without_a_token() -> None:
    seen: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["auth"] = request.headers.get("authorization")
        seen["body"] = request.read()
        return httpx.Response(204)

    await _client(handler, token=None).notify("https://cp.example.org", "tp-2", "completed")
    assert seen["auth"] is None and seen["body"] == b"{}"


async def test_a_refusal_is_a_failure() -> None:
    client = _client(lambda r: httpx.Response(401))
    with pytest.raises(CallbackFailed):
        await client.notify("https://cp.example.org", "tp-3", "started", {})


async def test_an_unreachable_control_plane_is_a_failure() -> None:
    def handler(request):
        raise httpx.ConnectError("down")

    with pytest.raises(CallbackFailed):
        await _client(handler).notify("https://cp.example.org", "tp-4", "started", {})


async def test_no_base_is_a_failure() -> None:
    with pytest.raises(CallbackFailed):
        await _client(lambda r: httpx.Response(200)).notify(None, "tp-5", "started")


async def test_only_spec_callbacks_exist() -> None:
    with pytest.raises(ValueError):
        await _client(lambda r: httpx.Response(200)).notify("https://cp", "tp-6", "terminated")
