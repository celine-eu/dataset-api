"""The data plane: DPS signals in, status messages out.

Transitions are synchronous. `/prepare` answers `PREPARED` and `/start` answers
`STARTED`, which the spec allows ("Synchronous Operation"). In that form the
data plane sends no `/prepared` or `/started` callback: the HTTP response is the
transition.

Every change to an existing flow happens inside `store.locked(flow_id)`, so
concurrent signals for one flow are serialised even across workers.
"""
from __future__ import annotations

import logging
from typing import Any, Iterable, Optional

from celine.dataset.dps.flows import (
    BadSignal,
    ControlPlane,
    DataFlow,
    DataFlowRepository,
    FlowConflict,
    FlowRole,
    FlowState,
    UnknownFlow,
)
from celine.dataset.dps.messages import (
    EDR_AUTHORIZATION,
    ControlPlaneRegistrationMessage,
    DataAddress,
    DataFlowPrepareMessage,
    DataFlowResumeMessage,
    DataFlowStartedNotificationMessage,
    DataFlowStartMessage,
    DataFlowSuspendMessage,
    DataFlowTerminateMessage,
    http_pull_address,
    is_pull,
    status_message,
)
from celine.dataset.dps.tokens import InvalidToken, TokenIssuer

logger = logging.getLogger(__name__)


class PullRefused(Exception):
    def __init__(self, status_code: int, detail: str):
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


class DataPlane:
    def __init__(
        self,
        *,
        dataplane_id: str,
        signaling_endpoint: str,
        pull_endpoint: str,
        transfer_types: Iterable[str],
        tokens: TokenIssuer,
        store: DataFlowRepository,
        labels: Iterable[str] = (),
    ):
        self.dataplane_id = dataplane_id
        self.signaling_endpoint = signaling_endpoint
        self.pull_endpoint = pull_endpoint
        self.transfer_types = list(transfer_types)
        self.labels = list(labels)
        self.tokens = tokens
        self.store = store
        push = [t for t in self.transfer_types if not is_pull(t)]
        if push:
            raise ValueError(f"this data plane serves pull transfers only, not {push}")

    # -- registration (DPS-10, DPS-11) ------------------------------------

    def registration_message(self) -> dict[str, Any]:
        """This data plane's half of a `DataPlaneRegistrationMessage`.

        `transferTypes` is the name EDC 0.18.0 reads (spec RC2). The
        `authorization` profile is added by whoever registers: it carries the
        control plane's credentials, which this service does not hold.
        """
        return {
            "dataplaneId": self.dataplane_id,
            "endpoint": self.signaling_endpoint,
            "transferTypes": list(self.transfer_types),
            "labels": list(self.labels),
        }

    async def register_control_plane(self, caller: str, msg: ControlPlaneRegistrationMessage) -> None:
        cp = ControlPlane(controlplane_id=msg.controlplaneId, endpoint=msg.endpoint, registered_by=caller)
        if not await self.store.put_control_plane(cp):
            raise FlowConflict(f"control plane {msg.controlplaneId} is registered by another caller")

    async def delete_control_plane(self, caller: str, controlplane_id: str) -> None:
        if not await self.store.delete_control_plane(controlplane_id, caller):
            raise UnknownFlow(f"control plane {controlplane_id} is not registered")

    async def callback_base(self, flow: DataFlow) -> Optional[str]:
        """Where to notify the flow's control plane (DPS-12).

        The message's `callbackAddress` (sent by EDC 0.18.0), or else the
        endpoint the same caller registered (the spec from RC4 on).
        """
        if flow.callback_address:
            return flow.callback_address
        cp = await self.store.control_plane_registered_by(flow.caller)
        return cp.endpoint if cp else None

    # -- signals from the control plane (DPS-04 … DPS-08) -----------------

    async def prepare(self, caller: str, msg: DataFlowPrepareMessage) -> dict[str, Any]:
        self._check_type(msg.transfer_type)
        flow = self._new_flow(caller, msg, FlowRole.CONSUMER)
        # A pull consumer has nothing to provision: the address arrives with
        # `/started`.
        flow.transition(FlowState.PREPARED)
        await self.store.create(flow)
        return status_message(flow.id, flow.state.value)

    async def start(self, caller: str, msg: DataFlowStartMessage) -> dict[str, Any]:
        self._check_type(msg.transfer_type)
        if msg.dataAddress is not None:
            raise BadSignal("a pull transfer's /start must not carry a data address")
        flow = self._new_flow(caller, msg, FlowRole.PROVIDER)
        flow.transition(FlowState.STARTED)
        address = self._issue(flow)
        await self.store.create(flow)  # one insert: the flow and its token together
        return status_message(flow.id, flow.state.value, data_address=address)

    async def started(self, caller: str, flow_id: str, msg: DataFlowStartedNotificationMessage) -> None:
        async with self.store.locked(flow_id) as flow:
            flow = self._own(caller, flow_id, flow)
            if flow.role is not FlowRole.CONSUMER:
                raise FlowConflict("/started is a consumer-side signal")
            if msg.dataAddress is None:
                raise BadSignal("a pull transfer's /started must carry the data address")
            flow.transition(FlowState.STARTED)
            flow.data_address = _without_credentials(msg.dataAddress)

    async def suspend(self, caller: str, flow_id: str, msg: DataFlowSuspendMessage) -> None:
        async with self.store.locked(flow_id) as flow:
            flow = self._own(caller, flow_id, flow)
            flow.transition(FlowState.SUSPENDED)
            flow.token_id = None
        logger.info("data flow %s suspended: %s", flow_id, msg.reason or "-")

    async def resume(self, caller: str, flow_id: str, msg: DataFlowResumeMessage) -> dict[str, Any]:
        async with self.store.locked(flow_id) as flow:
            flow = self._own(caller, flow_id, flow)
            if flow.state is not FlowState.SUSPENDED:
                raise FlowConflict(f"data flow {flow.id} is {flow.state.value}, not SUSPENDED")
            flow.transition(FlowState.STARTED)
            if flow.role is FlowRole.PROVIDER:
                # EDC 0.18.0 sends back the address this data plane issued (it
                # became the "data address owner" at /start). It is ignored: the
                # token in it died with the suspension.
                address = self._issue(flow)
                return status_message(flow.id, flow.state.value, data_address=address)
            if msg.dataAddress is not None:
                flow.data_address = _without_credentials(msg.dataAddress)
            return status_message(flow.id, flow.state.value)

    async def terminate(self, caller: str, flow_id: str, msg: DataFlowTerminateMessage) -> None:
        async with self.store.locked(flow_id) as flow:
            flow = self._own(caller, flow_id, flow)
            flow.token_id = None
            if flow.state is FlowState.TERMINATED:
                return  # already there; terminal states do not move
            flow.transition(FlowState.TERMINATED)
        logger.info("data flow %s terminated: %s", flow_id, msg.reason or "-")

    async def completed(self, caller: str, flow_id: str) -> None:
        async with self.store.locked(flow_id) as flow:
            flow = self._own(caller, flow_id, flow)
            flow.transition(FlowState.COMPLETED)
            flow.token_id = None

    async def status(self, caller: str, flow_id: str) -> dict[str, Any]:
        flow = self._own(caller, flow_id, await self.store.get(flow_id))
        return {"dataFlowId": flow.id, "state": flow.state.value}

    # -- the pull (DPS-09) ------------------------------------------------

    async def resolve_pull(self, authorization: Optional[str]) -> tuple[DataFlow, str]:
        """The flow a pull token stands for, and the consumer it was issued to."""
        token = (authorization or "").strip()
        if token[:7].lower() == "bearer ":
            token = token[7:].strip()
        if not token:
            raise PullRefused(401, "a pull needs the token from the data address")
        try:
            claims = self.tokens.verify(token)
        except InvalidToken as exc:
            logger.info("pull refused, token invalid: %s", exc)
            raise PullRefused(401, "pull token is not valid") from exc
        flow = await self.store.by_token(claims["jti"])
        if flow is None:
            # Retired by suspend, terminate, complete or resume, or never issued.
            raise PullRefused(401, "pull token is not live")
        if flow.state is not FlowState.STARTED:
            raise PullRefused(403, f"data flow is {flow.state.value}")
        if claims["aud"] != flow.counter_party_id or claims["iss"] != flow.participant_id:
            raise PullRefused(401, "pull token does not match its data flow")
        return flow, claims["aud"]

    # -- internals ----------------------------------------------------------

    def _check_type(self, transfer_type: str) -> None:
        if transfer_type not in self.transfer_types:
            raise BadSignal(f"transfer type {transfer_type!r} is not supported")

    @staticmethod
    def _new_flow(
        caller: str, msg: DataFlowPrepareMessage | DataFlowStartMessage, role: FlowRole
    ) -> DataFlow:
        return DataFlow(
            id=msg.flow_id,
            role=role,
            participant_id=msg.participantId,
            counter_party_id=msg.counterPartyId,
            agreement_id=msg.agreementId,
            dataset_id=msg.datasetId,
            transfer_type=msg.transfer_type,
            caller=caller,
            dataspace_context=msg.dataspaceContext,
            callback_address=msg.callbackAddress,
            claims=msg.claims or {},
            labels=msg.labels or [],
            metadata=msg.metadata or {},
        )

    @staticmethod
    def _own(caller: str, flow_id: str, flow: Optional[DataFlow]) -> DataFlow:
        # Another control plane's flow reads as absent: its existence is not
        # this caller's business.
        if flow is None or flow.caller != caller:
            raise UnknownFlow(f"data flow {flow_id} not found")
        return flow

    def _issue(self, flow: DataFlow) -> DataAddress:
        """A new token for the flow. Setting `token_id` retires the previous one
        once the flow is saved."""
        token, token_id = self.tokens.issue(
            issuer=flow.participant_id, audience=flow.counter_party_id
        )
        flow.token_id = token_id
        address = http_pull_address(self.pull_endpoint, token)
        # Recorded without the credential: the token is only ever in the response.
        flow.data_address = _without_credentials(address)
        return address


#: Endpoint property names that carry a credential, in EDC's namespace or bare
#: (the DSP spec's examples use bare names).
_CREDENTIAL_NAMES = frozenset({EDR_AUTHORIZATION, "authorization"})


def _without_credentials(address: DataAddress) -> dict[str, Any]:
    """The address as recorded: never with a token in it, ours or a provider's."""
    wire = address.wire()
    wire["endpointProperties"] = [
        p for p in wire.get("endpointProperties", []) if p["name"] not in _CREDENTIAL_NAMES
    ]
    return wire
