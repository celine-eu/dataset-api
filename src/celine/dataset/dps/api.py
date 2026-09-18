"""The DPS routes: signalling (control plane → data plane) and the pull.

Paths follow the reference SDK (`/v1/dataflows`, `/v1/controlplanes`) under a
`/dps` prefix. The registered endpoint is `{DPS_SIGNALING_URL}/dps/v1/dataflows`;
EDC appends `/prepare`, `/start`, `/{id}/…` to it.
"""
from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, Header, HTTPException, Response
from sqlalchemy.ext.asyncio import AsyncSession

from celine.dataset.api.dataset_query.executor import execute_query
from celine.dataset.db.engine import get_datasets_session, get_session, get_sessionmaker
from celine.dataset.dps.auth import signaling_caller
from celine.dataset.dps.flows import FlowError
from celine.dataset.dps.messages import (
    ControlPlaneRegistrationMessage,
    DataFlowPrepareMessage,
    DataFlowResumeMessage,
    DataFlowStartMessage,
    DataFlowStartedNotificationMessage,
    DataFlowSuspendMessage,
    DataFlowTerminateMessage,
)
from celine.dataset.dps.service import DataPlane, PullRefused
from celine.dataset.dps.settings import get_dps_settings
from celine.dataset.dps.store import SqlDataFlowRepository
from celine.dataset.dps.tokens import TokenIssuer
from celine.dataset.schemas.dataset_query import DatasetQueryModel, DatasetQueryResult
from celine.dataset.security.edr import EDRRequestContext

SIGNALING_PATH = "/dps/v1/dataflows"
PUBLIC_PATH = "/dps/public"

_dataplane: Optional[DataPlane] = None


def get_dataplane() -> DataPlane:
    global _dataplane
    if _dataplane is None:
        s = get_dps_settings()
        _dataplane = DataPlane(
            dataplane_id=s.dataplane_id,
            signaling_endpoint=s.signaling_url.rstrip("/") + SIGNALING_PATH,
            pull_endpoint=s.public_url.rstrip("/") + PUBLIC_PATH,
            transfer_types=s.transfer_types,
            labels=s.labels,
            tokens=TokenIssuer(s.token_signing_key, s.token_key_id),
            # The catalogue database: the one this service already migrates.
            store=SqlDataFlowRepository(get_sessionmaker()),
        )
    return _dataplane


def _fail(exc: FlowError) -> HTTPException:
    return HTTPException(exc.status_code, exc.detail)


def _status_response(body: dict, response: Response) -> dict:
    # 202 for a transition still under way, as the spec requires; this data
    # plane never leaves one under way, so this is the 200 path in practice.
    if body["state"].endswith("ING"):
        response.status_code = 202
        response.headers["Location"] = f"{SIGNALING_PATH}/{body['dataFlowId']}/status"
    return body


signaling_router = APIRouter(prefix="/dps")


@signaling_router.get("/registration")
async def registration(
    _caller: str = Depends(signaling_caller),
    dp: DataPlane = Depends(get_dataplane),
):
    """This data plane's `DataPlaneRegistrationMessage`, minus `authorization`."""
    return dp.registration_message()


@signaling_router.put("/v1/controlplanes")
async def register_control_plane(
    body: ControlPlaneRegistrationMessage,
    caller: str = Depends(signaling_caller),
    dp: DataPlane = Depends(get_dataplane),
):
    try:
        await dp.register_control_plane(caller, body)
    except FlowError as exc:
        raise _fail(exc) from exc
    return Response(status_code=200)


@signaling_router.delete("/v1/controlplanes/{controlplane_id}", status_code=204)
async def delete_control_plane(
    controlplane_id: str,
    caller: str = Depends(signaling_caller),
    dp: DataPlane = Depends(get_dataplane),
):
    try:
        await dp.delete_control_plane(caller, controlplane_id)
    except FlowError as exc:
        raise _fail(exc) from exc
    return Response(status_code=204)


@signaling_router.post("/v1/dataflows/prepare")
async def prepare(
    body: DataFlowPrepareMessage,
    response: Response,
    caller: str = Depends(signaling_caller),
    dp: DataPlane = Depends(get_dataplane),
):
    try:
        return _status_response(await dp.prepare(caller, body), response)
    except FlowError as exc:
        raise _fail(exc) from exc


@signaling_router.post("/v1/dataflows/start")
async def start(
    body: DataFlowStartMessage,
    response: Response,
    caller: str = Depends(signaling_caller),
    dp: DataPlane = Depends(get_dataplane),
):
    try:
        return _status_response(await dp.start(caller, body), response)
    except FlowError as exc:
        raise _fail(exc) from exc


@signaling_router.post("/v1/dataflows/{flow_id}/started")
async def started(
    flow_id: str,
    body: DataFlowStartedNotificationMessage,
    caller: str = Depends(signaling_caller),
    dp: DataPlane = Depends(get_dataplane),
):
    try:
        await dp.started(caller, flow_id, body)
    except FlowError as exc:
        raise _fail(exc) from exc
    return Response(status_code=200)


@signaling_router.post("/v1/dataflows/{flow_id}/suspend")
async def suspend(
    flow_id: str,
    body: DataFlowSuspendMessage,
    caller: str = Depends(signaling_caller),
    dp: DataPlane = Depends(get_dataplane),
):
    try:
        await dp.suspend(caller, flow_id, body)
    except FlowError as exc:
        raise _fail(exc) from exc
    return Response(status_code=200)


@signaling_router.post("/v1/dataflows/{flow_id}/resume")
async def resume(
    flow_id: str,
    body: DataFlowResumeMessage,
    caller: str = Depends(signaling_caller),
    dp: DataPlane = Depends(get_dataplane),
):
    try:
        return await dp.resume(caller, flow_id, body)
    except FlowError as exc:
        raise _fail(exc) from exc


@signaling_router.post("/v1/dataflows/{flow_id}/terminate")
async def terminate(
    flow_id: str,
    body: DataFlowTerminateMessage,
    caller: str = Depends(signaling_caller),
    dp: DataPlane = Depends(get_dataplane),
):
    try:
        await dp.terminate(caller, flow_id, body)
    except FlowError as exc:
        raise _fail(exc) from exc
    return Response(status_code=200)


@signaling_router.post("/v1/dataflows/{flow_id}/completed")
async def completed(
    flow_id: str,
    caller: str = Depends(signaling_caller),
    dp: DataPlane = Depends(get_dataplane),
):
    # No body model: EDC sends `{}`, the spec says empty.
    try:
        await dp.completed(caller, flow_id)
    except FlowError as exc:
        raise _fail(exc) from exc
    return Response(status_code=200)


@signaling_router.get("/v1/dataflows/{flow_id}/status")
async def status(
    flow_id: str,
    caller: str = Depends(signaling_caller),
    dp: DataPlane = Depends(get_dataplane),
):
    try:
        return await dp.status(caller, flow_id)
    except FlowError as exc:
        raise _fail(exc) from exc


public_router = APIRouter(prefix=PUBLIC_PATH)


@public_router.post("/query", response_model=DatasetQueryResult)
async def pull_query(
    body: DatasetQueryModel,
    catalogue_db: AsyncSession = Depends(get_session),
    datasets_db: AsyncSession = Depends(get_datasets_session),
    authorization: Optional[str] = Header(default=None),
    edc_contract_agreement_id: Optional[str] = Header(default=None),
    edc_transfer_process_id: Optional[str] = Header(default=None),
    edc_purpose: Optional[str] = Header(default=None),
    dp: DataPlane = Depends(get_dataplane),
):
    """The governed query, for the holder of a DPS pull token (DPS-09).

    The agreement is the signalled flow's. The consumer is the token's `aud`.
    From here on the request is the legacy EDR path: the `dataspace_expose`
    gate, ds's `/internal/dataplane/authorize`, the row filters, the audit.
    """
    try:
        flow, consumer = await dp.resolve_pull(authorization)
    except PullRefused as exc:
        raise HTTPException(exc.status_code, exc.detail) from exc

    if edc_contract_agreement_id and edc_contract_agreement_id != flow.agreement_id:
        raise HTTPException(403, "the agreement header does not match the data flow")

    context = EDRRequestContext(
        agreement_id=flow.agreement_id,
        consumer_id=consumer,
        # Which control plane this pull belongs to. Better than the EDR path's
        # `iss`: the flow is this data plane's own record of a transfer it was
        # signalled, and `resolve_pull` has already refused a token whose `iss`
        # disagrees with it.
        provider_id=flow.participant_id,
        # Client-asserted, as on the legacy path: ds checks it against the
        # agreement. The flow id is the *provider's* transfer id, which ds's
        # transfer check does not look up.
        transfer_id=edc_transfer_process_id,
        purpose=[p.strip() for p in (edc_purpose or "").split(",") if p.strip()],
    )
    return await execute_query(
        catalogue_db=catalogue_db,
        datasets_db=datasets_db,
        raw_sql=body.sql,
        limit=body.limit,
        offset=body.offset,
        user=None,  # dataspace mode never falls back to user auth
        edr_context=context,
        skip_count=body.skip_count,
    )
