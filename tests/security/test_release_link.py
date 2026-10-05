"""GS-05 — a dataspace read carries its release link.

The connector answers `POST /internal/dataplane/authorize` with a `decision_ref`
for an allow. This data plane carries it, opaque, into two places: the read's
`celine.audit` record and the `QueryExecuted` disclosure it posts back, each
beside the agreement id. A connector older than the field sends none, and that is
served as before. A disclosure that cannot be posted is an `ERROR`, not a warning.

The full pull path is held in `tests/dps/test_pull.py`; these hold the pieces.
"""
from __future__ import annotations

import json
import logging
from types import SimpleNamespace
from typing import Any, Self

import httpx as real_httpx
import pytest

from celine.dataset.core.config import get_settings
from celine.dataset.security import edr as edr_mod
from celine.dataset.security.audit import QUERY, ReadAudit
from celine.dataset.security.edr import (
    EDRRequestContext,
    audit_query,
    authorize_dataplane,
)

CONNECTOR = "http://connector.example.org:30001"
CONSUMER = "did:web:consumer.example.org"
#: A connector's reference: 64 hex characters, opaque to this service.
REF = "ab" * 32


class _Response:
    def __init__(self, payload: Any) -> None:
        self._payload = payload
        self.status_code = 200

    def json(self) -> Any:
        return self._payload

    def raise_for_status(self) -> None:
        return None


class _Connector:
    """Stands in for `httpx.AsyncClient`: answers every POST with `answer`, or
    raises `fail` when set, and records what it was sent."""

    def __init__(self, answer: Any = None, fail: Exception | None = None) -> None:
        self.answer = answer
        self.fail = fail
        self.posts: list[tuple[str, dict]] = []

    def factory(self, *args: Any, **kwargs: Any) -> Self:
        return self

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc: object) -> None:
        return None

    async def post(self, url: str, json: dict | None = None, **kwargs: Any) -> _Response:
        self.posts.append((url, json or {}))
        if self.fail is not None:
            raise self.fail
        return _Response(self.answer)


@pytest.fixture()
def connector(monkeypatch):
    settings = get_settings()
    monkeypatch.setattr(settings, "connector_internal_url", CONNECTOR)
    monkeypatch.setattr(settings, "connector_internal_urls", {})

    def install(**kwargs: Any) -> _Connector:
        double = _Connector(**kwargs)
        monkeypatch.setattr(
            edr_mod,
            "httpx",
            SimpleNamespace(
                AsyncClient=double.factory,
                HTTPError=real_httpx.HTTPError,
                HTTPStatusError=real_httpx.HTTPStatusError,
                RequestError=real_httpx.RequestError,
            ),
        )
        return double

    return install


async def _decide(connector, answer: dict):
    connector(answer=answer)
    return await authorize_dataplane(
        context=EDRRequestContext(agreement_id="agr-1", consumer_id=CONSUMER),
        dataset_ids=["datasets.example.readings"],
    )


# @verifies GS-05
async def test_the_decision_ref_of_an_allow_is_read_as_received(connector) -> None:
    decision = await _decide(
        connector, {"decision": "allow", "datasets": [], "decision_ref": REF}
    )
    assert decision.allowed
    assert decision.decision_ref == REF


# @verifies GS-05
async def test_a_connector_older_than_the_field_is_served_without_one(connector) -> None:
    decision = await _decide(connector, {"decision": "allow", "datasets": []})
    assert decision.allowed
    assert decision.decision_ref is None


# @verifies GS-05
async def test_a_deny_carries_no_decision_ref(connector) -> None:
    decision = await _decide(
        connector, {"decision": "deny", "reason": "consent_missing", "decision_ref": None}
    )
    assert not decision.allowed
    assert decision.decision_ref is None


# @verifies GS-05
async def test_a_decision_ref_that_is_not_a_string_is_dropped_not_coerced(connector) -> None:
    decision = await _decide(
        connector, {"decision": "allow", "datasets": [], "decision_ref": 12345}
    )
    assert decision.allowed
    assert decision.decision_ref is None


async def _disclose(**kwargs: Any) -> None:
    await audit_query(
        dataset_id="datasets.example.readings",
        consumer_id=CONSUMER,
        agreement_id="agr-1",
        transfer_id="tr-1",
        row_count=3,
        **kwargs,
    )


# @verifies GS-05
async def test_the_disclosure_carries_the_decision_ref_and_the_agreement(connector) -> None:
    double = connector(answer={})
    await _disclose(decision_ref=REF)

    [(url, payload)] = double.posts
    assert url == f"{CONNECTOR}/internal/audit/query"
    assert (payload["agreement_id"], payload["decision_ref"]) == ("agr-1", REF)


# @verifies GS-05
async def test_without_a_decision_ref_the_disclosure_is_the_one_it_always_was(
    connector,
) -> None:
    """Never invented and never sent as `null`: an older connector, which may
    refuse a field it does not know, sees the request it always saw."""
    double = connector(answer={})
    await _disclose()

    [(_, payload)] = double.posts
    assert "decision_ref" not in payload
    assert payload["agreement_id"] == "agr-1"


# @verifies GS-05
async def test_a_disclosure_that_cannot_be_posted_is_an_error(connector, caplog) -> None:
    connector(fail=real_httpx.ConnectError("connection refused"))
    caplog.set_level(logging.DEBUG, logger=edr_mod.logger.name)

    await _disclose(decision_ref=REF)  # best-effort: does not raise

    [record] = [
        r for r in caplog.records if "QueryExecuted disclosure not recorded" in r.getMessage()
    ]
    assert record.levelno == logging.ERROR


def _records(caplog) -> list[dict]:
    return [json.loads(r.getMessage()) for r in caplog.records if r.name == "celine.audit"]


# @verifies GS-05
def test_a_dataspace_read_record_carries_the_release_link(caplog) -> None:
    caplog.set_level(logging.INFO, logger="celine.audit")
    with ReadAudit(QUERY) as audit:
        audit.datasets = ["datasets.example.a", "datasets.example.b"]
        audit.agreement_id = "agr-1"
        audit.decision_ref = REF

    records = _records(caplog)
    assert [r["resource"] for r in records] == ["datasets.example.a", "datasets.example.b"]
    for record in records:
        assert (record["agreement_id"], record["decision_ref"]) == ("agr-1", REF)
    # The attribute a structured handler reads says the same as the line.
    for raw in (r for r in caplog.records if r.name == "celine.audit"):
        assert raw.audit["decision_ref"] == REF


# @verifies GS-05
def test_a_refused_dataspace_read_names_its_agreement_and_no_decision(caplog) -> None:
    from celine.dataset.security.audit import Refused

    caplog.set_level(logging.INFO, logger="celine.audit")
    with pytest.raises(Refused), ReadAudit(QUERY) as audit:
        audit.datasets = ["datasets.example.a"]
        audit.agreement_id = "agr-1"
        raise Refused(403, "Refused by ds: consent_missing", reason="ds_refused")

    [record] = _records(caplog)
    assert (record["event"], record["reason"]) == ("denied", "ds_refused")
    assert (record["agreement_id"], record["decision_ref"]) == ("agr-1", None)


# @verifies GS-05
def test_a_read_off_the_dataspace_path_carries_neither_field(caplog) -> None:
    caplog.set_level(logging.INFO, logger="celine.audit")
    with ReadAudit(QUERY) as audit:
        audit.datasets = ["datasets.example.a"]

    [record] = _records(caplog)
    assert "agreement_id" not in record
    assert "decision_ref" not in record


# @verifies GS-05
def test_the_link_does_not_outlive_its_records(caplog) -> None:
    """Records written after a dataspace read — by any other code on the audit
    logger — are not stamped with its agreement."""
    from celine.sdk.audit import audit_access

    caplog.set_level(logging.INFO, logger="celine.audit")
    with ReadAudit(QUERY) as audit:
        audit.agreement_id = "agr-1"
    audit_access("catalogue.import", service="dataset-api")

    later = _records(caplog)[-1]
    assert later["action"] == "catalogue.import"
    assert "agreement_id" not in later


# @verifies GS-05
def test_an_asserted_agreement_reaches_the_record_without_control_characters(caplog) -> None:
    caplog.set_level(logging.INFO, logger="celine.audit")
    with ReadAudit(QUERY) as audit:
        audit.agreement_id = "agr-1\n{\"forged\":true}" + "x" * 400

    [record] = _records(caplog)
    assert "\n" not in record["agreement_id"]
    assert len(record["agreement_id"]) == 256
