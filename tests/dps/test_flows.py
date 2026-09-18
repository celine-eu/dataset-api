"""DPS-07 — the data flow state machine; DPS-13 — the repository contract, in memory."""
from __future__ import annotations

import pytest

from celine.dataset.dps.flows import (
    ControlPlane,
    DataFlow,
    FlowConflict,
    FlowRole,
    FlowState,
    InMemoryDataFlowRepository,
)


def _flow(flow_id: str = "f1", state: FlowState = FlowState.INITIALIZED) -> DataFlow:
    return DataFlow(
        id=flow_id,
        role=FlowRole.PROVIDER,
        participant_id="p",
        counter_party_id="c",
        agreement_id="a",
        dataset_id="d",
        transfer_type="HttpData-PULL",
        caller="cp",
        state=state,
    )


@pytest.mark.parametrize(
    ("start", "to"),
    [
        (FlowState.INITIALIZED, FlowState.PREPARED),
        (FlowState.INITIALIZED, FlowState.STARTED),
        (FlowState.PREPARING, FlowState.PREPARED),
        (FlowState.PREPARED, FlowState.STARTED),
        (FlowState.STARTING, FlowState.STARTED),
        (FlowState.STARTED, FlowState.SUSPENDED),
        (FlowState.STARTED, FlowState.COMPLETED),
        (FlowState.SUSPENDED, FlowState.STARTED),
        (FlowState.PREPARED, FlowState.TERMINATED),
        (FlowState.SUSPENDED, FlowState.TERMINATED),
    ],
)
def test_spec_transitions_are_allowed(start: FlowState, to: FlowState) -> None:
    flow = _flow(state=start)
    flow.transition(to)
    assert flow.state is to


@pytest.mark.parametrize(
    ("start", "to"),
    [
        (FlowState.PREPARED, FlowState.SUSPENDED),
        (FlowState.SUSPENDED, FlowState.COMPLETED),
        (FlowState.STARTED, FlowState.PREPARED),
        (FlowState.COMPLETED, FlowState.STARTED),
        (FlowState.COMPLETED, FlowState.TERMINATED),
        (FlowState.TERMINATED, FlowState.STARTED),
    ],
)
def test_other_transitions_conflict(start: FlowState, to: FlowState) -> None:
    flow = _flow(state=start)
    with pytest.raises(FlowConflict):
        flow.transition(to)
    assert flow.state is start


async def test_a_flow_id_is_created_once() -> None:
    store = InMemoryDataFlowRepository()
    await store.create(_flow())
    with pytest.raises(FlowConflict):
        await store.create(_flow())


async def test_locked_persists_on_exit_and_by_token_follows() -> None:
    store = InMemoryDataFlowRepository()
    await store.create(_flow())
    async with store.locked("f1") as flow:
        flow.token_id = "t1"
    assert (await store.by_token("t1")).id == "f1"
    async with store.locked("f1") as flow:
        flow.token_id = "t2"
    assert await store.by_token("t1") is None
    assert (await store.by_token("t2")).id == "f1"


async def test_an_exception_discards_the_change() -> None:
    store = InMemoryDataFlowRepository()
    await store.create(_flow(state=FlowState.STARTED))
    with pytest.raises(FlowConflict):
        async with store.locked("f1") as flow:
            flow.transition(FlowState.SUSPENDED)
            raise FlowConflict("abort")
    assert (await store.get("f1")).state is FlowState.STARTED


async def test_reads_are_copies() -> None:
    store = InMemoryDataFlowRepository()
    await store.create(_flow())
    (await store.get("f1")).state = FlowState.TERMINATED
    assert (await store.get("f1")).state is FlowState.INITIALIZED


async def test_a_token_id_belongs_to_one_flow() -> None:
    store = InMemoryDataFlowRepository()
    await store.create(_flow("f1"))
    await store.create(_flow("f2"))
    async with store.locked("f1") as flow:
        flow.token_id = "t"
    with pytest.raises(FlowConflict):
        async with store.locked("f2") as flow:
            flow.token_id = "t"


async def test_a_missing_flow_is_yielded_as_none() -> None:
    async with InMemoryDataFlowRepository().locked("nope") as flow:
        assert flow is None


async def test_control_plane_registrations_belong_to_their_caller() -> None:
    store = InMemoryDataFlowRepository()
    assert await store.put_control_plane(ControlPlane("cp", "https://a", "caller-a"))
    assert not await store.put_control_plane(ControlPlane("cp", "https://b", "caller-b"))
    assert (await store.control_plane_registered_by("caller-a")).endpoint == "https://a"
    assert not await store.delete_control_plane("cp", "caller-b")
    assert await store.delete_control_plane("cp", "caller-a")
    assert await store.control_plane_registered_by("caller-a") is None
