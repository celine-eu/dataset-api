"""DPS-13 — flows live in the catalogue database: shared by workers, kept across restarts.

Integration: PostgreSQL, the test run's catalogue schema. "A worker" is a
`DataPlane` with its own engine and connection pool, which is what a separate
uvicorn worker has. The last test uses real separate processes.
"""
from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import textwrap

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from celine.dataset.core.config import get_settings
from celine.dataset.dps.flows import ControlPlane, FlowConflict, FlowState
from celine.dataset.dps.messages import (
    EDR_AUTHORIZATION,
    DataFlowResumeMessage,
    DataFlowStartMessage,
    DataFlowSuspendMessage,
)
from celine.dataset.dps.service import PullRefused
from celine.dataset.dps.store import SqlDataFlowRepository
from celine.dataset.dps.tokens import TokenIssuer

from .conftest import CONSUMER, edc_start_message, make_dataplane

CP = "cp-a"


def _pem() -> str:
    return ec.generate_private_key(ec.SECP256R1()).private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
    ).decode()


def _url() -> str:
    return get_settings().database_url.replace("postgresql+psycopg", "postgresql+asyncpg")


@pytest.fixture()
def key() -> str:
    return _pem()


@pytest.fixture()
async def worker(dps_tables, key):
    """A factory of workers: separate engines, one shared signing key."""
    engines = []

    def _worker():
        engine = create_async_engine(_url())
        engines.append(engine)
        store = SqlDataFlowRepository(async_sessionmaker(bind=engine, expire_on_commit=False))
        return make_dataplane(store, TokenIssuer(key))

    yield _worker
    for engine in engines:
        await engine.dispose()


def _token(status: dict) -> str:
    props = {p["name"]: p["value"] for p in status["dataAddress"]["endpointProperties"]}
    return props[EDR_AUTHORIZATION]


async def _start(dp, flow_id: str) -> str:
    return _token(await dp.start(CP, DataFlowStartMessage.model_validate(edc_start_message(flow_id))))


# -- the repository contract, in SQL ---------------------------------------


async def test_a_flow_round_trips(sql_dataplane) -> None:
    await _start(sql_dataplane, "s-1")
    flow = await sql_dataplane.store.get("s-1")
    assert flow.state is FlowState.STARTED
    assert flow.counter_party_id == CONSUMER
    assert flow.claims == {"sub": CONSUMER}
    assert flow.labels == [] and flow.metadata == {}
    assert flow.token_id and flow.data_address["endpoint"]
    assert flow.created_at.tzinfo is not None


async def test_a_duplicate_id_conflicts(sql_dataplane) -> None:
    await _start(sql_dataplane, "s-2")
    with pytest.raises(FlowConflict):
        await _start(sql_dataplane, "s-2")


async def test_a_token_id_is_unique_in_the_table(sql_dataplane) -> None:
    await _start(sql_dataplane, "s-3a")
    await _start(sql_dataplane, "s-3b")
    taken = (await sql_dataplane.store.get("s-3a")).token_id
    with pytest.raises(FlowConflict):
        async with sql_dataplane.store.locked("s-3b") as flow:
            flow.token_id = taken
    assert (await sql_dataplane.store.get("s-3b")).token_id != taken


async def test_an_exception_rolls_the_change_back(sql_dataplane) -> None:
    await _start(sql_dataplane, "s-4")
    with pytest.raises(RuntimeError):
        async with sql_dataplane.store.locked("s-4") as flow:
            flow.transition(FlowState.SUSPENDED)
            raise RuntimeError("abort")
    assert (await sql_dataplane.store.get("s-4")).state is FlowState.STARTED


async def test_control_plane_ownership_is_enforced_by_the_statement(sql_store_factory) -> None:
    store = sql_store_factory()
    assert await store.put_control_plane(ControlPlane("cp-x", "https://a", "caller-a"))
    assert await store.put_control_plane(ControlPlane("cp-x", "https://a2", "caller-a"))
    assert not await store.put_control_plane(ControlPlane("cp-x", "https://b", "caller-b"))
    assert (await store.control_plane_registered_by("caller-a")).endpoint == "https://a2"
    assert not await store.delete_control_plane("cp-x", "caller-b")
    assert await store.delete_control_plane("cp-x", "caller-a")
    assert await store.control_plane_registered_by("caller-a") is None


async def test_no_authorization_profile_is_stored(dps_tables) -> None:
    async with dps_tables.connect() as conn:
        cols = (
            await conn.execute(
                text(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema = :s AND table_name = 'dps_control_planes'"
                ),
                {"s": get_settings().catalogue_schema},
            )
        ).scalars().all()
    assert set(cols) == {"controlplane_id", "endpoint", "registered_by", "updated_at"}


# -- multiple workers ------------------------------------------------------


async def test_a_token_issued_by_one_worker_is_served_by_another(worker) -> None:
    a, b = worker(), worker()
    token = await _start(a, "w-1")
    flow, consumer = await b.resolve_pull(token)
    assert flow.id == "w-1" and consumer == CONSUMER


async def test_a_suspension_on_one_worker_stops_the_pull_on_another(worker) -> None:
    a, b = worker(), worker()
    token = await _start(a, "w-2")
    await b.suspend(CP, "w-2", DataFlowSuspendMessage())
    with pytest.raises(PullRefused) as exc:
        await a.resolve_pull(token)
    assert exc.value.status_code == 401

    new = _token(await a.resume(CP, "w-2", DataFlowResumeMessage()))
    assert (await b.resolve_pull(new))[0].id == "w-2"
    with pytest.raises(PullRefused):
        await b.resolve_pull(token)


async def test_concurrent_resumes_on_two_workers_resume_once(worker) -> None:
    """`SELECT … FOR UPDATE`: the second reads the first's result, not the old state."""
    a, b = worker(), worker()
    await _start(a, "w-3")
    await a.suspend(CP, "w-3", DataFlowSuspendMessage())

    results = await asyncio.gather(
        a.resume(CP, "w-3", DataFlowResumeMessage()),
        b.resume(CP, "w-3", DataFlowResumeMessage()),
        return_exceptions=True,
    )
    ok = [r for r in results if isinstance(r, dict)]
    conflicts = [r for r in results if isinstance(r, FlowConflict)]
    assert len(ok) == 1 and len(conflicts) == 1, results
    # The one token handed out is the one that works.
    assert (await b.resolve_pull(_token(ok[0])))[0].id == "w-3"


async def test_a_flow_created_twice_at_once_exists_once(worker) -> None:
    a, b = worker(), worker()
    results = await asyncio.gather(_start(a, "w-4"), _start(b, "w-4"), return_exceptions=True)
    assert sum(isinstance(r, str) for r in results) == 1
    assert sum(isinstance(r, FlowConflict) for r in results) == 1


# -- restart ---------------------------------------------------------------


async def test_a_restart_keeps_flows_and_tokens(dps_tables, key) -> None:
    first = create_async_engine(_url())
    before = make_dataplane(
        SqlDataFlowRepository(async_sessionmaker(bind=first)), TokenIssuer(key)
    )
    token = await _start(before, "r-1")
    await first.dispose()  # the process is gone

    second = create_async_engine(_url())
    try:
        after = make_dataplane(
            SqlDataFlowRepository(async_sessionmaker(bind=second)), TokenIssuer(key)
        )
        assert (await after.resolve_pull(token))[0].id == "r-1"
        assert (await after.status(CP, "r-1"))["state"] == "STARTED"
    finally:
        await second.dispose()


# -- separate processes ----------------------------------------------------

_CHILD = textwrap.dedent(
    """
    import asyncio, json, sys
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
    from celine.dataset.core.config import get_settings
    from celine.dataset.dps.messages import DataFlowStartMessage, DataFlowSuspendMessage
    from celine.dataset.dps.service import DataPlane, PullRefused
    from celine.dataset.dps.store import SqlDataFlowRepository
    from celine.dataset.dps.tokens import TokenIssuer

    async def main(action, arg, key):
        engine = create_async_engine(
            get_settings().database_url.replace("postgresql+psycopg", "postgresql+asyncpg"))
        dp = DataPlane(dataplane_id="dp", signaling_endpoint="s", pull_endpoint="https://p",
                       transfer_types=["HttpData-PULL"], tokens=TokenIssuer(key),
                       store=SqlDataFlowRepository(async_sessionmaker(bind=engine)))
        try:
            if action == "start":
                out = await dp.start("cp-a", DataFlowStartMessage.model_validate(json.loads(arg)))
            elif action == "pull":
                try:
                    flow, consumer = await dp.resolve_pull(arg)
                    out = {"flow": flow.id, "consumer": consumer}
                except PullRefused as exc:
                    out = {"refused": exc.status_code}
            else:
                await dp.suspend("cp-a", arg, DataFlowSuspendMessage())
                out = {"suspended": arg}
            print(json.dumps(out))
        finally:
            await engine.dispose()

    asyncio.run(main(sys.argv[1], sys.argv[2], sys.stdin.read()))
    """
)


def _child(action: str, arg: str, key: str) -> dict:
    env = {**os.environ, "CATALOGUE_SCHEMA": get_settings().catalogue_schema}
    proc = subprocess.run(
        [sys.executable, "-c", _CHILD, action, arg],
        input=key, capture_output=True, text=True, env=env, timeout=60,
    )
    assert proc.returncode == 0, proc.stderr
    return json.loads(proc.stdout.strip().splitlines()[-1])


async def test_separate_processes_share_flows(dps_tables, key) -> None:
    """Start in one process, pull in a second, suspend in a third, pull again."""
    started = await asyncio.to_thread(
        _child, "start", json.dumps(edc_start_message("p-1")), key
    )
    token = _token(started)

    assert await asyncio.to_thread(_child, "pull", token, key) == {
        "flow": "p-1", "consumer": CONSUMER,
    }
    await asyncio.to_thread(_child, "suspend", "p-1", key)
    assert await asyncio.to_thread(_child, "pull", token, key) == {"refused": 401}
