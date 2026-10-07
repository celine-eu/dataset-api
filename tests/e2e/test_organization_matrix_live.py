"""The organization matrix, against a running dataset-api (GS-06, RF-11).

`tests/routes/test_organization_scoping.py` proves the predicate in process. This
proves it end to end: real Keycloak tokens, a real catalogue imported from the
pipelines' governance, and a warehouse holding two communities' rows. It is
written for a hardened instance (`CELINE_ENV=staging`, e.g. celine-dev
`task prodlike -- up dataset-api`), and is skipped unless it is told where
everything is. Nothing here names a real community:

    ORG_E2E_API_URL            the dataset-api, e.g. http://127.0.0.1:18001
    ORG_E2E_COMMUNITY          a community whose warehouse rows exist
    ORG_E2E_OTHER              a second community with rows of its own
    ORG_E2E_VIEWER_TOKEN       a `viewers` user of ORG_E2E_COMMUNITY, owning no meter
    ORG_E2E_MANAGER_TOKEN      a `managers` user of ORG_E2E_COMMUNITY, owning no meter
    ORG_E2E_OTHER_VIEWER_TOKEN a `viewers` user of ORG_E2E_OTHER only
    ORG_E2E_SERVICE_TOKEN      a service holding `dataset.query`

and, optionally, the dataset lists (comma-separated table names in the gold
schema; the defaults are the public pipelines' REC datasets):

    ORG_E2E_SCHEMA             default `datasets.ds_dev_gold`
    ORG_E2E_COMMUNITY_DATASETS community grain: `organization_match` on the column
    ORG_E2E_DEVICE_DATASETS    device grain: `rec_registry` on `device_id`
    ORG_E2E_COLUMN             the community column, default `community_id`
    ORG_E2E_RETIRED_COLUMN     the column it replaced, default `rec_id`

The e2e layer also needs `DATASET_API_E2E=1` (`conftest.py`).
"""
from __future__ import annotations

import os

import httpx
import pytest

ENV = ("ORG_E2E_API_URL", "ORG_E2E_COMMUNITY", "ORG_E2E_OTHER", "ORG_E2E_VIEWER_TOKEN",
       "ORG_E2E_MANAGER_TOKEN", "ORG_E2E_OTHER_VIEWER_TOKEN", "ORG_E2E_SERVICE_TOKEN")

pytestmark = pytest.mark.skipif(
    any(not os.environ.get(k) for k in ENV), reason="needs a live stack: " + ", ".join(ENV)
)

COMMUNITY_DATASETS = (
    "rec_virtual_consumption_15m,rec_virtual_consumption_hourly,rec_measurements_15m,"
    "rec_flexibility_windows_community,rec_co2_savings_community"
)
DEVICE_DATASETS = (
    "meters_data_15m,meters_data_15m_missing_intervals,rec_virtual_consumption_per_device_15m,"
    "rec_settlement_1h,rec_participant_points,rec_gamification_summary,"
    "rec_flexibility_windows,rec_anti_gaming_flags,rec_device_membership"
)


def _env(key: str, default: str | None = None) -> str:
    value = os.environ.get(key, default)
    assert value is not None, key
    return value


def _list(key: str, default: str) -> list[str]:
    return [name.strip() for name in _env(key, default).split(",") if name.strip()]


def _dataset(table: str) -> str:
    return f"{_env('ORG_E2E_SCHEMA', 'datasets.ds_dev_gold')}.{table}"


def _column() -> str:
    return _env("ORG_E2E_COLUMN", "community_id")


def _query(token_key: str, sql: str) -> httpx.Response:
    return httpx.post(
        f"{_env('ORG_E2E_API_URL')}/query",
        json={"sql": sql, "limit": 1000},
        headers={"Authorization": f"Bearer {_env(token_key)}"},
        timeout=120,
    )


def _rows_per_community(token_key: str, table: str) -> dict[str, int]:
    column = _column()
    resp = _query(token_key, f"SELECT {column} AS c, count(*) AS n FROM {_dataset(table)} GROUP BY {column}")
    assert resp.status_code == 200, f"{table}: {resp.status_code} {resp.text}"
    return {row["c"]: row["n"] for row in resp.json()["items"]}


def _count(token_key: str, table: str) -> int:
    resp = _query(token_key, f"SELECT count(*) AS n FROM {_dataset(table)}")
    assert resp.status_code == 200, f"{table}: {resp.status_code} {resp.text}"
    return resp.json()["items"][0]["n"]


COMMUNITY = pytest.mark.parametrize("table", _list("ORG_E2E_COMMUNITY_DATASETS", COMMUNITY_DATASETS))
DEVICE = pytest.mark.parametrize("table", _list("ORG_E2E_DEVICE_DATASETS", DEVICE_DATASETS))


# A matrix that passes because the warehouse holds one community proves nothing.
@COMMUNITY
def test_the_service_sees_both_communities(table):
    seen = _rows_per_community("ORG_E2E_SERVICE_TOKEN", table)
    assert seen.get(_env("ORG_E2E_COMMUNITY")), f"{table}: no rows of the community"
    assert seen.get(_env("ORG_E2E_OTHER")), f"{table}: no rows of the other community"


# @verifies GS-06
@COMMUNITY
@pytest.mark.parametrize(
    "token_key, own, other",
    [
        ("ORG_E2E_VIEWER_TOKEN", "ORG_E2E_COMMUNITY", "ORG_E2E_OTHER"),
        ("ORG_E2E_MANAGER_TOKEN", "ORG_E2E_COMMUNITY", "ORG_E2E_OTHER"),
        ("ORG_E2E_OTHER_VIEWER_TOKEN", "ORG_E2E_OTHER", "ORG_E2E_COMMUNITY"),
    ],
)
def test_an_organization_reader_reads_only_its_own_community(table, token_key, own, other):
    seen = _rows_per_community(token_key, table)
    assert seen.get(_env(own)), f"{table}: {token_key} reads none of its own rows"
    assert set(seen) == {_env(own)}, f"{table}: {token_key} reads {sorted(seen)}"
    assert _env(other) not in seen


# @verifies RF-11
@DEVICE
@pytest.mark.parametrize("token_key", ["ORG_E2E_MANAGER_TOKEN", "ORG_E2E_VIEWER_TOKEN"])
def test_a_reader_without_a_meter_reads_no_device_rows(table, token_key):
    assert _count(token_key, table) == 0


@COMMUNITY
def test_the_retired_column_is_gone(table):
    resp = httpx.get(
        f"{_env('ORG_E2E_API_URL')}/catalogue/{_dataset(table)}/schema",
        headers={"Authorization": f"Bearer {_env('ORG_E2E_SERVICE_TOKEN')}"},
        timeout=60,
    )
    assert resp.status_code == 200, resp.text
    body = resp.text
    assert f'"{_column()}"' in body
    assert f'"{_env("ORG_E2E_RETIRED_COLUMN", "rec_id")}"' not in body
