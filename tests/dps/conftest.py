"""Fixtures for the DPS data plane (`docs/dps-data-plane.md`).

Signalling authentication is replaced by a stand-in that reads the caller from
the bearer (`Bearer cp-a` → `cp-a`), except in `test_signaling_auth.py`, which
exercises the real dependency.

`dataplane` is the unit fixture: an in-memory repository and no database.
`sql_dataplane` is the integration fixture: the real repository, over the DPS
tables created in the test run's catalogue schema.
"""
from __future__ import annotations

import asyncio
from typing import Any, Optional

import pytest
from fastapi import FastAPI, Header, HTTPException
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import async_sessionmaker

from celine.dataset.dps.api import get_dataplane, public_router, signaling_router
from celine.dataset.dps.auth import signaling_caller
from celine.dataset.dps.flows import InMemoryDataFlowRepository
from celine.dataset.dps.service import DataPlane
from celine.dataset.dps.store import SqlDataFlowRepository
from celine.dataset.dps.store import metadata as dps_metadata
from celine.dataset.dps.tokens import TokenIssuer

PROVIDER = "did:web:provider.example.org"
CONSUMER = "did:web:consumer.example.org"
PULL_ENDPOINT = "https://data.example.org/dps/public"
SIGNALING_ENDPOINT = "https://dataplane.example.org/dps/v1/dataflows"


def edc_start_message(flow_id: str = "tp-1", **overrides: Any) -> dict[str, Any]:
    """What EDC 0.18.0's `DataPlaneSignalingFlowController.start` sends for a pull.

    Serialised from `org.eclipse.edc.signaling.domain.DataFlowStartMessage`'s
    getters; `dataAddress` is `NON_NULL` and absent on a pull.
    """
    body = {
        "messageId": "m-1",
        "participantId": PROVIDER,
        "counterPartyId": CONSUMER,
        "dataspaceContext": "dataspace-protocol-http:2025-1",
        "processId": flow_id,
        "agreementId": "agreement-1",
        "datasetId": "example_dataset",
        "callbackAddress": "https://cp.example.org/api/signaling",
        "transferType": "HttpData-PULL",
        "labels": None,
        "metadata": None,
        "claims": {"sub": CONSUMER},
    }
    body.update(overrides)
    return body


def edc_prepare_message(flow_id: str = "tp-c1", **overrides: Any) -> dict[str, Any]:
    """EDC 0.18.0's prepare: sent by the consumer's control plane."""
    body = edc_start_message(flow_id)
    body.update(participantId=CONSUMER, counterPartyId=PROVIDER)
    body.update(overrides)
    return body


def run(coro):
    """Await from a synchronous test (the in-memory repository only)."""
    return asyncio.run(coro)


def make_dataplane(store, tokens: TokenIssuer) -> DataPlane:
    return DataPlane(
        dataplane_id="dp-test",
        signaling_endpoint=SIGNALING_ENDPOINT,
        pull_endpoint=PULL_ENDPOINT,
        transfer_types=["HttpData-PULL"],
        tokens=tokens,
        store=store,
    )


def bearer(caller: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {caller}"}


async def _caller_from_header(authorization: Optional[str] = Header(default=None)) -> str:
    if not authorization:
        raise HTTPException(401, "no token")
    return authorization.removeprefix("Bearer ").strip()


@pytest.fixture()
def tokens() -> TokenIssuer:
    return TokenIssuer(key_id="test-key", ephemeral=True)


@pytest.fixture()
def dataplane(tokens) -> DataPlane:
    return make_dataplane(InMemoryDataFlowRepository(), tokens)


@pytest.fixture()
async def dps_tables(test_engine):
    """The DPS tables, in the schema `test_engine` creates and drops."""
    async with test_engine.begin() as conn:
        await conn.run_sync(dps_metadata.create_all)
    return test_engine


@pytest.fixture()
def sql_store_factory(dps_tables):
    """A new repository per call, each with its own session factory."""

    def _make() -> SqlDataFlowRepository:
        return SqlDataFlowRepository(async_sessionmaker(bind=dps_tables, expire_on_commit=False))

    return _make


@pytest.fixture()
def sql_dataplane(sql_store_factory, tokens) -> DataPlane:
    return make_dataplane(sql_store_factory(), tokens)


@pytest.fixture()
def dps_app(dataplane: DataPlane) -> FastAPI:
    app = FastAPI()
    app.include_router(signaling_router)
    app.include_router(public_router)
    app.dependency_overrides[get_dataplane] = lambda: dataplane
    app.dependency_overrides[signaling_caller] = _caller_from_header
    return app


@pytest.fixture()
def signaling(dps_app: FastAPI) -> TestClient:
    return TestClient(dps_app)
