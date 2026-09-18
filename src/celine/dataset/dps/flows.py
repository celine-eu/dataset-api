"""The data flow state machine (DPS "Data Flow State Machine") and its storage contract.

States and transitions are the spec's. `PREPARING` → `PREPARED` and
`STARTING` → `STARTED` are the only ones that may be asynchronous. This data
plane answers both synchronously, so it never enters the `…ING` states, but they
are valid for a data plane that needs them.

Storage is a `DataFlowRepository`. The service needs one guarantee from it:
`locked(flow_id)` serialises changes to a flow across every worker, so two
signals for the same flow cannot both read `SUSPENDED` and both resume it. The
production repository is `store.SqlDataFlowRepository`, in the catalogue
database. `InMemoryDataFlowRepository` below serves the unit tests.
"""
from __future__ import annotations

import asyncio
import copy
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, AsyncIterator, Optional, Protocol


class FlowState(str, Enum):
    INITIALIZED = "INITIALIZED"
    PREPARING = "PREPARING"
    PREPARED = "PREPARED"
    STARTING = "STARTING"
    STARTED = "STARTED"
    SUSPENDED = "SUSPENDED"
    COMPLETED = "COMPLETED"
    TERMINATED = "TERMINATED"


TERMINAL = frozenset({FlowState.COMPLETED, FlowState.TERMINATED})

_ALLOWED: dict[FlowState, frozenset[FlowState]] = {
    FlowState.INITIALIZED: frozenset(
        {FlowState.PREPARING, FlowState.PREPARED, FlowState.STARTING, FlowState.STARTED}
    ),
    FlowState.PREPARING: frozenset({FlowState.PREPARED}),
    FlowState.PREPARED: frozenset({FlowState.STARTING, FlowState.STARTED}),
    FlowState.STARTING: frozenset({FlowState.STARTED}),
    FlowState.STARTED: frozenset({FlowState.SUSPENDED, FlowState.COMPLETED}),
    FlowState.SUSPENDED: frozenset({FlowState.STARTED}),
}


class FlowRole(str, Enum):
    #: Created by `/start`: this data plane serves the data.
    PROVIDER = "PROVIDER"
    #: Created by `/prepare`: this data plane receives the data.
    CONSUMER = "CONSUMER"


class FlowError(Exception):
    status_code = 400

    def __init__(self, detail: str):
        super().__init__(detail)
        self.detail = detail


class BadSignal(FlowError):
    status_code = 400


class UnknownFlow(FlowError):
    status_code = 404


class FlowConflict(FlowError):
    status_code = 409


def _now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class DataFlow:
    id: str
    role: FlowRole
    participant_id: str
    counter_party_id: str
    agreement_id: str
    dataset_id: str
    transfer_type: str
    #: The authenticated control plane that created the flow. Only it may
    #: signal the flow again.
    caller: str
    state: FlowState = FlowState.INITIALIZED
    dataspace_context: Optional[str] = None
    callback_address: Optional[str] = None
    claims: dict[str, Any] = field(default_factory=dict)
    labels: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    #: Provider: the address handed out, without its credential.
    #: Consumer: the address received.
    data_address: Optional[dict[str, Any]] = None
    #: `jti` of the one pull token currently valid for this flow. Replacing or
    #: clearing it retires the previous token.
    token_id: Optional[str] = None
    created_at: datetime = field(default_factory=_now)
    updated_at: datetime = field(default_factory=_now)

    def transition(self, to: FlowState) -> None:
        if self.state in TERMINAL:
            raise FlowConflict(f"data flow {self.id} is {self.state.value}")
        if to is FlowState.TERMINATED or to in _ALLOWED.get(self.state, frozenset()):
            self.state = to
            self.updated_at = _now()
            return
        raise FlowConflict(
            f"data flow {self.id} cannot go from {self.state.value} to {to.value}"
        )


@dataclass
class ControlPlane:
    """A spec control-plane registration.

    Its `authorization` profile is **not kept**: this data plane authenticates
    its callbacks with its own OIDC client, and the profile may carry a secret.
    """

    controlplane_id: str
    endpoint: str
    registered_by: str


class DataFlowRepository(Protocol):
    async def create(self, flow: DataFlow) -> None:
        """Insert. Raises `FlowConflict` if the id or the token id exists."""
        ...

    async def get(self, flow_id: str) -> Optional[DataFlow]: ...

    async def by_token(self, token_id: str) -> Optional[DataFlow]: ...

    def locked(self, flow_id: str) -> AbstractAsyncContextManager[Optional[DataFlow]]:
        """Yield the flow (or None) under an exclusive lock and persist it on a
        clean exit. An exception discards the changes."""
        ...

    async def put_control_plane(self, cp: ControlPlane) -> bool:
        """Register or replace. Returns False, changing nothing, when another
        caller holds the id."""
        ...

    async def delete_control_plane(self, controlplane_id: str, caller: str) -> bool:
        """Returns False when the caller did not register that id."""
        ...

    async def control_plane_registered_by(self, caller: str) -> Optional[ControlPlane]: ...


class InMemoryDataFlowRepository:
    """One process, no persistence. For unit tests.

    It hands out copies, as a database would, so a test cannot pass only because
    the service changed a shared object without saving it.
    """

    def __init__(self) -> None:
        self._flows: dict[str, DataFlow] = {}
        self._control_planes: dict[str, ControlPlane] = {}
        self._lock = asyncio.Lock()

    async def create(self, flow: DataFlow) -> None:
        async with self._lock:
            if flow.id in self._flows:
                raise FlowConflict(f"data flow {flow.id} already exists")
            self._check_token_unique(flow)
            self._flows[flow.id] = copy.deepcopy(flow)

    async def get(self, flow_id: str) -> Optional[DataFlow]:
        return copy.deepcopy(self._flows.get(flow_id))

    async def by_token(self, token_id: str) -> Optional[DataFlow]:
        for flow in self._flows.values():
            if flow.token_id == token_id:
                return copy.deepcopy(flow)
        return None

    @asynccontextmanager
    async def locked(self, flow_id: str) -> AsyncIterator[Optional[DataFlow]]:
        async with self._lock:
            flow = copy.deepcopy(self._flows.get(flow_id))
            yield flow
            if flow is not None:
                self._check_token_unique(flow)
                self._flows[flow.id] = copy.deepcopy(flow)

    async def put_control_plane(self, cp: ControlPlane) -> bool:
        existing = self._control_planes.get(cp.controlplane_id)
        if existing is not None and existing.registered_by != cp.registered_by:
            return False
        self._control_planes[cp.controlplane_id] = copy.deepcopy(cp)
        return True

    async def delete_control_plane(self, controlplane_id: str, caller: str) -> bool:
        existing = self._control_planes.get(controlplane_id)
        if existing is None or existing.registered_by != caller:
            return False
        del self._control_planes[controlplane_id]
        return True

    async def control_plane_registered_by(self, caller: str) -> Optional[ControlPlane]:
        for cp in self._control_planes.values():
            if cp.registered_by == caller:
                return copy.deepcopy(cp)
        return None

    def _check_token_unique(self, flow: DataFlow) -> None:
        if flow.token_id and any(
            f.token_id == flow.token_id and f.id != flow.id for f in self._flows.values()
        ):
            raise FlowConflict("token id already bound to another data flow")
