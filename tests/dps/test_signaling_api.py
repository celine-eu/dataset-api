"""DPS-04 … DPS-08, DPS-10, DPS-11 — the signalling endpoints as EDC calls them."""
from __future__ import annotations

from celine.dataset.dps.messages import EDR_AUTHORIZATION, HTTP_ENDPOINT_TYPE
from celine.dataset.dps.service import DataPlane

from .conftest import (
    CONSUMER,
    PROVIDER,
    PULL_ENDPOINT,
    SIGNALING_ENDPOINT,
    bearer,
    edc_prepare_message,
    edc_start_message,
    run,
)

BASE = "/dps/v1/dataflows"
CP = bearer("cp-a")


def _token(body: dict) -> str:
    props = {p["name"]: p["value"] for p in body["dataAddress"]["endpointProperties"]}
    return props[EDR_AUTHORIZATION]


def _status(signaling, flow_id: str, caller=CP) -> str:
    r = signaling.get(f"{BASE}/{flow_id}/status", headers=caller)
    assert r.status_code == 200, r.text
    assert r.json()["dataFlowId"] == flow_id
    return r.json()["state"]


# -- DPS-05 start --------------------------------------------------------


def test_start_answers_started_with_the_pull_address(signaling, dataplane: DataPlane) -> None:
    r = signaling.post(f"{BASE}/start", json=edc_start_message("tp-1"), headers=CP)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["state"] == "STARTED"
    assert body["dataFlowId"] == "tp-1"
    assert body["dataAddress"]["endpoint"] == PULL_ENDPOINT
    assert body["dataAddress"]["endpointType"] == HTTP_ENDPOINT_TYPE

    # DPS-06: the token is ours and names the consumer.
    claims = dataplane.tokens.verify(_token(body))
    assert claims["aud"] == CONSUMER and claims["iss"] == PROVIDER
    assert _status(signaling, "tp-1") == "STARTED"

    # The stored record does not keep the credential.
    recorded = run(dataplane.store.get("tp-1")).data_address
    assert EDR_AUTHORIZATION not in {p["name"] for p in recorded["endpointProperties"]}


def test_start_twice_conflicts(signaling) -> None:
    assert signaling.post(f"{BASE}/start", json=edc_start_message("tp-2"), headers=CP).status_code == 200
    assert signaling.post(f"{BASE}/start", json=edc_start_message("tp-2"), headers=CP).status_code == 409


def test_start_with_an_unsupported_type_is_refused(signaling) -> None:
    r = signaling.post(
        f"{BASE}/start", json=edc_start_message("tp-3", transferType="HttpData-PUSH"), headers=CP
    )
    assert r.status_code == 400
    assert _missing(signaling, "tp-3")


def test_a_pull_start_with_a_data_address_is_refused(signaling) -> None:
    body = edc_start_message("tp-4", dataAddress={"endpointType": "x", "endpoint": "https://x"})
    assert signaling.post(f"{BASE}/start", json=body, headers=CP).status_code == 400
    assert _missing(signaling, "tp-4")


def test_a_message_without_its_flow_id_is_refused(signaling) -> None:
    body = edc_start_message()
    del body["processId"]
    assert signaling.post(f"{BASE}/start", json=body, headers=CP).status_code == 422


def test_start_accepts_the_rc4_spelling(signaling) -> None:
    body = edc_start_message()
    del body["processId"], body["transferType"]
    body.update(dataFlowId="tp-rc4", profile="HttpData-PULL")
    r = signaling.post(f"{BASE}/start", json=body, headers=CP)
    assert r.status_code == 200 and r.json()["dataFlowId"] == "tp-rc4"


# -- DPS-04 prepare, and the consumer side of DPS-07 ----------------------


def test_prepare_then_started_records_the_providers_address(signaling, dataplane) -> None:
    r = signaling.post(f"{BASE}/prepare", json=edc_prepare_message("c-1"), headers=CP)
    assert r.status_code == 200, r.text
    assert r.json()["state"] == "PREPARED"
    assert "dataAddress" not in r.json()

    # A provider's /start answer, as the consumer control plane forwards it.
    address = {
        "@type": "DataAddress",
        "endpointType": HTTP_ENDPOINT_TYPE,
        "endpoint": "https://provider.example.org/dps/public",
        "endpointProperties": [{"@type": "EndpointProperty", "name": EDR_AUTHORIZATION, "value": "t"}],
    }
    r = signaling.post(f"{BASE}/c-1/started", json={"messageId": "m", "dataAddress": address}, headers=CP)
    assert r.status_code == 200, r.text
    assert _status(signaling, "c-1") == "STARTED"
    recorded = run(dataplane.store.get("c-1")).data_address
    # Recorded, but never with the provider's token in it (DPS-13).
    assert recorded["endpoint"] == address["endpoint"]
    assert recorded["endpointProperties"] == []


def test_started_without_an_address_is_refused(signaling) -> None:
    signaling.post(f"{BASE}/prepare", json=edc_prepare_message("c-2"), headers=CP)
    assert signaling.post(f"{BASE}/c-2/started", json={"messageId": "m"}, headers=CP).status_code == 400
    assert _status(signaling, "c-2") == "PREPARED"


def test_started_is_not_a_provider_signal(signaling) -> None:
    signaling.post(f"{BASE}/start", json=edc_start_message("tp-5"), headers=CP)
    r = signaling.post(
        f"{BASE}/tp-5/started",
        json={"dataAddress": {"endpointType": "x", "endpoint": "https://x"}},
        headers=CP,
    )
    assert r.status_code == 409


def test_prepare_with_an_unsupported_type_is_refused(signaling) -> None:
    body = edc_prepare_message("c-3", transferType="AmazonS3-PUSH")
    assert signaling.post(f"{BASE}/prepare", json=body, headers=CP).status_code == 400


# -- DPS-07 provider transitions ------------------------------------------


def test_suspend_resume_issues_a_new_token(signaling, dataplane) -> None:
    first = signaling.post(f"{BASE}/start", json=edc_start_message("tp-6"), headers=CP).json()
    old = _token(first)

    assert signaling.post(f"{BASE}/tp-6/suspend", json={"messageId": "m", "reason": "r"}, headers=CP).status_code == 200
    assert _status(signaling, "tp-6") == "SUSPENDED"
    assert run(dataplane.store.by_token(dataplane.tokens.verify(old)["jti"])) is None

    # EDC 0.18.0 sends back the address it holds; it is ignored.
    r = signaling.post(f"{BASE}/tp-6/resume", json={"messageId": "m", "dataAddress": first["dataAddress"]}, headers=CP)
    assert r.status_code == 200, r.text
    assert r.json()["state"] == "STARTED"
    new = _token(r.json())
    assert new != old
    assert run(dataplane.store.by_token(dataplane.tokens.verify(new)["jti"])).id == "tp-6"
    assert run(dataplane.store.by_token(dataplane.tokens.verify(old)["jti"])) is None


def test_resume_of_a_running_flow_conflicts(signaling) -> None:
    signaling.post(f"{BASE}/start", json=edc_start_message("tp-7"), headers=CP)
    assert signaling.post(f"{BASE}/tp-7/resume", json={}, headers=CP).status_code == 409


def test_terminate_retires_the_token_and_is_idempotent(signaling, dataplane) -> None:
    body = signaling.post(f"{BASE}/start", json=edc_start_message("tp-8"), headers=CP).json()
    jti = dataplane.tokens.verify(_token(body))["jti"]

    assert signaling.post(f"{BASE}/tp-8/terminate", json={"messageId": "m"}, headers=CP).status_code == 200
    assert _status(signaling, "tp-8") == "TERMINATED"
    assert run(dataplane.store.by_token(jti)) is None
    assert signaling.post(f"{BASE}/tp-8/terminate", json={"messageId": "m"}, headers=CP).status_code == 200
    # Terminal states do not move.
    assert signaling.post(f"{BASE}/tp-8/suspend", json={}, headers=CP).status_code == 409


def test_completed_retires_the_token_and_is_final(signaling, dataplane) -> None:
    body = signaling.post(f"{BASE}/start", json=edc_start_message("tp-9"), headers=CP).json()
    jti = dataplane.tokens.verify(_token(body))["jti"]

    # EDC sends `{}` with a JSON content type.
    assert signaling.post(f"{BASE}/tp-9/completed", json={}, headers=CP).status_code == 200
    assert _status(signaling, "tp-9") == "COMPLETED"
    assert run(dataplane.store.by_token(jti)) is None
    assert signaling.post(f"{BASE}/tp-9/terminate", json={}, headers=CP).status_code == 409


def test_an_unknown_flow_is_404(signaling) -> None:
    assert signaling.post(f"{BASE}/nope/suspend", json={}, headers=CP).status_code == 404
    assert signaling.get(f"{BASE}/nope/status", headers=CP).status_code == 404


def test_another_callers_flow_reads_as_absent(signaling) -> None:
    signaling.post(f"{BASE}/start", json=edc_start_message("tp-10"), headers=CP)
    other = bearer("cp-b")
    assert signaling.get(f"{BASE}/tp-10/status", headers=other).status_code == 404
    assert signaling.post(f"{BASE}/tp-10/terminate", json={}, headers=other).status_code == 404
    assert _status(signaling, "tp-10") == "STARTED"


# -- DPS-10, DPS-11, DPS-12 ------------------------------------------------


def test_the_registration_message_is_edc_shaped(signaling) -> None:
    r = signaling.get("/dps/registration", headers=CP)
    assert r.status_code == 200
    assert r.json() == {
        "dataplaneId": "dp-test",
        "endpoint": SIGNALING_ENDPOINT,
        "transferTypes": ["HttpData-PULL"],
        "labels": [],
    }
    assert "authorization" not in r.json()


def test_control_plane_registration_supplies_the_callback_base(signaling, dataplane) -> None:
    reg = {"controlplaneId": "cp-1", "endpoint": "https://cp.example.org/api/signaling",
           "authorization": {"type": "oauth2_client_credentials"}}
    assert signaling.put("/dps/v1/controlplanes", json=reg, headers=CP).status_code == 200

    # RC4: no callbackAddress in the message.
    body = edc_start_message("tp-11")
    del body["callbackAddress"]
    signaling.post(f"{BASE}/start", json=body, headers=CP)
    assert run(dataplane.callback_base(run(dataplane.store.get("tp-11")))) == reg["endpoint"]

    # EDC 0.18.0 sends one, and it wins.
    signaling.post(f"{BASE}/start", json=edc_start_message("tp-12"), headers=CP)
    assert run(dataplane.callback_base(run(dataplane.store.get("tp-12")))) == "https://cp.example.org/api/signaling"

    # Another caller cannot take over or delete the registration.
    assert signaling.put("/dps/v1/controlplanes", json=reg, headers=bearer("cp-b")).status_code == 409
    assert signaling.delete("/dps/v1/controlplanes/cp-1", headers=bearer("cp-b")).status_code == 404
    assert signaling.delete("/dps/v1/controlplanes/cp-1", headers=CP).status_code == 204
    assert run(dataplane.callback_base(run(dataplane.store.get("tp-11")))) is None


def test_a_push_data_plane_cannot_be_configured() -> None:
    import pytest

    from celine.dataset.dps.flows import InMemoryDataFlowRepository
    from celine.dataset.dps.tokens import TokenIssuer

    with pytest.raises(ValueError):
        DataPlane(
            dataplane_id="x",
            signaling_endpoint="s",
            pull_endpoint="p",
            transfer_types=["HttpData-PUSH"],
            tokens=TokenIssuer(ephemeral=True),
            store=InMemoryDataFlowRepository(),
        )


def _missing(signaling, flow_id: str) -> bool:
    return signaling.get(f"{BASE}/{flow_id}/status", headers=CP).status_code == 404
