"""A stand-in ds-connector for the two-instance e2e — several participants, one process.

It answers the three `/internal/*` endpoints this service asks a connector for,
under a per-participant path prefix so one process can play several connectors:

    /{participant}/internal/edr-jwks
    /{participant}/internal/dataplane/authorize
    /{participant}/internal/audit/query

It is **not** a model of ds. It holds no agreements and decides nothing: every
`authorize` is an allow for the dataset ids it is handed, because what the e2e is
proving is *which connector was asked*, not what it answered. The one thing it
reproduces faithfully is the key set, because that is what makes a token from one
participant fail at another participant's data plane.

Configuration is `E2E_PARTICIPANTS`, JSON: `{"<name>": "<EC private key PEM>"}`.
"""
from __future__ import annotations

import json
import os
from typing import Any

import jwt
from cryptography.hazmat.primitives import serialization
from fastapi import FastAPI, Request

app = FastAPI()

_PARTICIPANTS: dict[str, Any] = {
    name: serialization.load_pem_private_key(pem.encode(), password=None)
    for name, pem in json.loads(os.environ.get("E2E_PARTICIPANTS", "{}")).items()
}

#: Everything asked of every participant, in order. The e2e reads it back to say
#: which connector served which request.
CALLS: list[dict[str, Any]] = []


@app.get("/{participant}/internal/edr-jwks")
async def edr_jwks(participant: str) -> dict[str, Any]:
    CALLS.append({"participant": participant, "call": "edr-jwks"})
    key = _PARTICIPANTS[participant].public_key()
    return {"keys": [jwt.algorithms.ECAlgorithm.to_jwk(key, as_dict=True)]}


@app.post("/{participant}/internal/dataplane/authorize")
async def authorize(participant: str, request: Request) -> dict[str, Any]:
    body = await request.json()
    CALLS.append({"participant": participant, "call": "authorize", "body": body})
    return {
        "decision": "allow",
        "datasets": [
            {"dataset_id": dataset_id, "row_filter": None}
            for dataset_id in body.get("dataset_ids", [])
        ],
    }


@app.post("/{participant}/internal/audit/query")
async def audit(participant: str, request: Request) -> dict[str, Any]:
    body = await request.json()
    CALLS.append({"participant": participant, "call": "audit", "body": body})
    return {}


@app.get("/_calls")
async def calls() -> list[dict[str, Any]]:
    """What this stub was asked, for the e2e to assert on."""
    return CALLS
