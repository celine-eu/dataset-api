"""The deployment topology, proved: one dataset-api per participant.

The maintainer, 2026-09-18: *"having multiple dataset-api is the way, and worth
proving in an e2e"*. This is that proof, and it is deliberately a **process**
e2e rather than an in-process one, because the constraint being demonstrated is
a process-global: `core/config.py` caches one `Settings` per interpreter and
`create_app(settings_override=…)` calls `configure()`, which replaces it. Two
instances with two connectors therefore cannot exist in one Python process at
all — which is itself the reason the practicality half of this work (one
instance, several connectors, `connector_internal_urls`) is worth having for
tests and validation.

What runs:

- one stub connector process playing both participants' `/internal/*` APIs under
  two path prefixes, each with its own EDR signing key (`stub_connector.py`);
- **instance A** — its own catalogue schema, its own dataset, `A`'s connector;
- **instance B** — likewise for `B`;
- **instance AB** — one instance, both catalogues, both connectors through
  `CONNECTOR_INTERNAL_URLS`. The practicality half, in the same run as the
  topology it does not replace.

Then, over real HTTP:

1. A's token to A serves rows; the decision and the disclosure land at A's
   connector.
2. **A's token to B is refused** — B's connector publishes a different key set,
   which is the property that makes the topology a boundary rather than a
   convention.
3. Both tokens to AB serve rows, each reaching its own connector.

Preconditions: PostgreSQL on the server `DATABASE_URL` names (the suite creates
its own `*_test` databases there, `tests/testdb.py`), `uv`, and three free
localhost ports. No EDC and no ds: this proves
*this repository's* seam, and the connector behind it is a stub. An e2e over a
real EDC transfer belongs in the deployment, not here — it needs two connectors,
two EDC runtimes and a negotiated agreement.

**These three were `xfail(strict=True)` when this module was written**, and not
because of anything in it: `POST /query` depends on `get_optional_user`, which
FastAPI resolves *before* the route body and which validated any
`Authorization: Bearer …` against Keycloak. An EDR token is signed by EDC's
vault key, so Keycloak's key set can never contain its `kid`, and every request
carrying a real EDR token was refused `401 Token validation failed` before the
dataspace path was entered — at *any* instance, with or without the connector
map.

The dependency now returns `None` for a request in dataspace mode
(`security/edr.py::dataspace_mode`), and the strict markers did their job: they
failed as XPASS the moment the gate opened, and were removed here. So these
assertions are live, and a regression in that dependency fails this module
rather than being absorbed by it. Nothing here is stubbed on the auth path.

Run with `DATASET_API_E2E=1 uv run pytest tests/e2e -q`.
"""
from __future__ import annotations

import json
import os
import socket
import subprocess
import time
from pathlib import Path

import httpx
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec

from celine.dataset.dps.tokens import TokenIssuer

from ..conftest import PROTECTED_DATABASE_URLS
from ..testdb import assert_disposable

REPO = Path(__file__).resolve().parents[2]

PARTICIPANT_A = "did:web:provider-a.example.org"
PARTICIPANT_B = "did:web:provider-b.example.org"
CONSUMER = "did:web:consumer.example.org"

#: Path segments for the stub, since a DID is not a URL path.
SEGMENT = {PARTICIPANT_A: "a", PARTICIPANT_B: "b"}

#: A **database** each, not a schema each. `CATALOGUE_SCHEMA` cannot vary: the
#: migrations hardcode `dataset_api` (`alembic/versions/11ab075cfa8f_.py` and
#: every one after it), so the setting moves the ORM's view of the table and not
#: the table. Two databases is also the truer shape — it is the warehouse, the
#: half this work deliberately left process-global.
#: Named `*_test` so the suite's guard (`tests/testdb.py`) admits dropping them.
DATABASE = {PARTICIPANT_A: "e2e_dataset_api_a_test", PARTICIPANT_B: "e2e_dataset_api_b_test"}
DATASET = {PARTICIPANT_A: "ds_a_readings", PARTICIPANT_B: "ds_b_readings"}


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def _pem(key: ec.EllipticCurvePrivateKey) -> str:
    return key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ).decode()


def _wait(url: str, process: subprocess.Popen, timeout: float = 45.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise AssertionError(f"{url} exited with {process.returncode}")
        try:
            if httpx.get(url, timeout=2.0).status_code < 500:
                return
        except httpx.HTTPError:
            time.sleep(0.4)
    raise AssertionError(f"{url} did not come up")


def _run(cmd: list[str], env: dict[str, str]) -> subprocess.Popen:
    return subprocess.Popen(
        cmd, cwd=REPO, env={**os.environ, **env},
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )


@pytest.fixture(scope="module")
def database_url(test_database_url) -> str:
    """The suite's own database: only the server is used, to create the two below."""
    return test_database_url


@pytest.fixture(scope="module")
def keys() -> dict[str, ec.EllipticCurvePrivateKey]:
    return {
        PARTICIPANT_A: ec.generate_private_key(ec.SECP256R1()),
        PARTICIPANT_B: ec.generate_private_key(ec.SECP256R1()),
    }


@pytest.fixture(scope="module")
def connector(keys) -> str:
    """One process, both participants' connectors."""
    port = _free_port()
    process = _run(
        ["uv", "run", "uvicorn", "tests.e2e.stub_connector:app",
         "--host", "127.0.0.1", "--port", str(port)],
        {
            "E2E_PARTICIPANTS": json.dumps(
                {SEGMENT[did]: _pem(key) for did, key in keys.items()}
            )
        },
    )
    base = f"http://127.0.0.1:{port}"
    try:
        _wait(f"{base}/_calls", process)
        yield base
    finally:
        process.terminate()
        process.wait(timeout=10)


def _database_url(database_url: str, name: str) -> str:
    return database_url.rsplit("/", 1)[0] + "/" + name


@pytest.fixture(scope="module")
def catalogues(database_url) -> dict[str, str]:
    """A database per participant, each migrated and holding its own dataset."""
    import sqlalchemy

    admin = sqlalchemy.create_engine(database_url, isolation_level="AUTOCOMMIT")
    urls = {did: _database_url(database_url, name) for did, name in DATABASE.items()}
    for url in urls.values():
        assert_disposable(url, protected=PROTECTED_DATABASE_URLS)
    try:
        for did, name in DATABASE.items():
            with admin.connect() as conn:
                conn.execute(sqlalchemy.text(f'DROP DATABASE IF EXISTS "{name}"'))
                conn.execute(sqlalchemy.text(f'CREATE DATABASE "{name}"'))
            migrate = _run(
                ["uv", "run", "alembic", "upgrade", "head"],
                {"DATABASE_URL": urls[did]},
            )
            out, _ = migrate.communicate(timeout=300)
            assert migrate.returncode == 0, out

            engine = sqlalchemy.create_engine(urls[did])
            with engine.begin() as conn:
                conn.execute(sqlalchemy.text("CREATE SCHEMA IF NOT EXISTS gold"))
                conn.execute(sqlalchemy.text("CREATE TABLE gold.readings (id INTEGER)"))
                conn.execute(sqlalchemy.text("INSERT INTO gold.readings VALUES (1)"))
                conn.execute(
                    sqlalchemy.text(
                        "INSERT INTO dataset_api.datasets_entries "
                        "(dataset_id, title, backend_type, backend_config, expose, "
                        " dataspace_expose, access_level) "
                        "VALUES (:id, :id, 'postgres', :cfg, true, true, 'open')"
                    ),
                    {"id": DATASET[did], "cfg": json.dumps({"table": "gold.readings"})},
                )
            engine.dispose()
        yield urls
    finally:
        for name in DATABASE.values():
            with admin.connect() as conn:
                conn.execute(sqlalchemy.text(f'DROP DATABASE IF EXISTS "{name}"'))
        admin.dispose()


def _instance(*, env: dict[str, str], database_url: str):
    port = _free_port()
    process = _run(
        ["uv", "run", "uvicorn", "celine.dataset.main:create_app", "--factory",
         "--host", "127.0.0.1", "--port", str(port)],
        {
            "EDR_ENABLED": "true",
            "DATABASE_URL": database_url,
            "DATASETS_DATABASE_URL": database_url,
            "POLICIES_CHECK_ENABLED": "false",
            **env,
        },
    )
    base = f"http://127.0.0.1:{port}"
    _wait(f"{base}/docs", process)
    return base, process


@pytest.fixture(scope="module")
def instances(connector, catalogues) -> dict[str, str]:
    """A, B, and the one instance that serves both."""
    started: list[subprocess.Popen] = []
    bases: dict[str, str] = {}
    try:
        for did in (PARTICIPANT_A, PARTICIPANT_B):
            base, process = _instance(
                env={"CONNECTOR_INTERNAL_URL": f"{connector}/{SEGMENT[did]}"},
                database_url=catalogues[did],
            )
            started.append(process)
            bases[did] = base

        # The practicality half: one instance, both connectors. It carries A's
        # catalogue and A's warehouse — a single instance still has exactly one
        # of each, which is precisely why it does not replace the two above.
        base, process = _instance(
            env={
                "CONNECTOR_INTERNAL_URL": f"{connector}/{SEGMENT[PARTICIPANT_A]}",
                "CONNECTOR_INTERNAL_URLS": json.dumps(
                    {
                        PARTICIPANT_A: f"{connector}/{SEGMENT[PARTICIPANT_A]}",
                        PARTICIPANT_B: f"{connector}/{SEGMENT[PARTICIPANT_B]}",
                    }
                ),
            },
            database_url=catalogues[PARTICIPANT_A],
        )
        started.append(process)
        bases["both"] = base
        yield bases
    finally:
        for process in started:
            process.terminate()
            process.wait(timeout=10)


def _token(keys, participant: str) -> str:
    issued, _ = TokenIssuer(_pem(keys[participant])).issue(
        issuer=participant, audience=CONSUMER
    )
    return issued


def _query(base: str, token: str, dataset: str) -> httpx.Response:
    return httpx.post(
        f"{base}/query",
        json={"sql": f"SELECT * FROM {dataset}", "limit": 10},
        headers={
            "Authorization": f"Bearer {token}",
            "Edc-Contract-Agreement-Id": "agr-e2e",
            "Edc-Transfer-Process-Id": "tr-e2e",
        },
        timeout=30.0,
    )


def _calls(connector: str) -> list[dict]:
    return httpx.get(f"{connector}/_calls", timeout=10.0).json()


def test_a_participants_own_instance_serves_its_dataset(
    instances, connector, keys
) -> None:
    response = _query(
        instances[PARTICIPANT_A], _token(keys, PARTICIPANT_A), DATASET[PARTICIPANT_A]
    )
    assert response.status_code == 200, response.text
    served = {(c["participant"], c["call"]) for c in _calls(connector)}
    assert ("a", "authorize") in served
    assert ("a", "audit") in served
    assert not any(c["participant"] == "b" for c in _calls(connector))


def test_one_participants_token_is_refused_at_anothers_instance(
    instances, connector, keys
) -> None:
    """The boundary the topology draws.

    B's instance verifies against B's connector's key set, which cannot verify a
    token A's EDC signed. This is what makes "one instance per participant" a
    boundary rather than a convention, and it is unchanged by the map: an
    instance still only serves the participants it was configured for.

    The **reason** is asserted, not just the status. While `get_optional_user`
    still refused the EDR token, this request was also a 401 several steps
    earlier, so a status-only assertion would have passed while proving nothing
    about the key sets. It is the reason, not the status, that says the token
    reached verification at all.
    """
    response = _query(
        instances[PARTICIPANT_B], _token(keys, PARTICIPANT_A), DATASET[PARTICIPANT_B]
    )
    assert response.status_code == 401, response.text
    assert response.json()["detail"] == "EDR token is not valid", response.text


def test_one_instance_can_resolve_both_connectors(instances, connector, keys) -> None:
    """The practicality the maintainer asked for, in the same run.

    Same process, two providers, two control planes — the thing that halves the
    setup cost of a validation pass.
    """
    first = _query(
        instances["both"], _token(keys, PARTICIPANT_A), DATASET[PARTICIPANT_A]
    )
    assert first.status_code == 200, first.text

    before = len(_calls(connector))
    second = _query(
        instances["both"], _token(keys, PARTICIPANT_B), DATASET[PARTICIPANT_A]
    )
    # B holds no agreement over A's dataset in a real deployment; the stub
    # allows, so what this asserts is *which* connector was asked.
    assert second.status_code == 200, second.text
    assert [c["participant"] for c in _calls(connector)[before:]].count("b") >= 1
