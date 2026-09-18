"""DPS-03 — messages in either spec spelling; DPS-05's data address shape."""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from celine.dataset.dps.messages import (
    EDR_AUTH_TYPE,
    EDR_AUTHORIZATION,
    EDR_ENDPOINT,
    HTTP_ENDPOINT_TYPE,
    DataAddress,
    DataFlowStartMessage,
    http_pull_address,
    is_pull,
    status_message,
)

from .conftest import CONSUMER, edc_start_message


def test_edc_0_18_rc2_names_are_read() -> None:
    msg = DataFlowStartMessage.model_validate(edc_start_message("tp-9"))
    assert msg.flow_id == "tp-9"
    assert msg.transfer_type == "HttpData-PULL"
    assert msg.counterPartyId == CONSUMER
    assert msg.labels is None and msg.metadata is None  # EDC sends nulls


def test_rc4_names_are_read() -> None:
    body = edc_start_message()
    del body["processId"], body["transferType"], body["callbackAddress"]
    body.update(dataFlowId="tp-rc4", profile="https://w3id.org/dspace-sig/profile/http-pull")
    msg = DataFlowStartMessage.model_validate(body)
    assert msg.flow_id == "tp-rc4"
    assert msg.transfer_type.endswith("/http-pull")
    assert msg.callbackAddress is None


def test_unknown_fields_are_ignored() -> None:
    msg = DataFlowStartMessage.model_validate({**edc_start_message(), "futureField": 1})
    assert not hasattr(msg, "futureField")


@pytest.mark.parametrize(
    "missing", ["processId", "participantId", "counterPartyId", "agreementId", "datasetId", "transferType"]
)
def test_a_deciding_fact_is_required(missing: str) -> None:
    body = edc_start_message()
    del body[missing]
    with pytest.raises(ValidationError):
        DataFlowStartMessage.model_validate(body)


def test_a_dsp_data_address_round_trips_as_edc_serialises_it() -> None:
    # `org.eclipse.edc.signaling.domain.DspDataAddress` with its `@type` fields.
    wire = {
        "@type": "DataAddress",
        "endpointType": HTTP_ENDPOINT_TYPE,
        "endpoint": "https://x.example.org",
        "endpointProperties": [
            {"@type": "EndpointProperty", "name": EDR_AUTHORIZATION, "value": "t"}
        ],
    }
    address = DataAddress.model_validate(wire)
    assert address.property(EDR_AUTHORIZATION) == "t"
    assert address.wire() == wire


def test_the_pull_address_carries_edc_named_properties() -> None:
    wire = http_pull_address("https://data.example.org/dps/public", "tok").wire()
    assert wire["@type"] == "DataAddress"
    assert wire["endpointType"] == HTTP_ENDPOINT_TYPE
    assert wire["endpoint"] == "https://data.example.org/dps/public"
    props = {p["name"]: p["value"] for p in wire["endpointProperties"]}
    assert props[EDR_ENDPOINT] == wire["endpoint"]
    assert props[EDR_AUTHORIZATION] == "tok"
    assert props[EDR_AUTH_TYPE] == "bearer"
    assert all(p["@type"] == "EndpointProperty" for p in wire["endpointProperties"])


def test_a_status_message_always_has_a_state() -> None:
    body = status_message("f", "STARTED")
    assert body["state"] == "STARTED" and body["dataFlowId"] == "f" and body["messageId"]
    assert "dataAddress" not in body and "error" not in body


@pytest.mark.parametrize(
    ("transfer_type", "pull"),
    [
        ("HttpData-PULL", True),
        ("HttpData-PUSH", False),
        ("HttpData-PULL-HttpData", True),
        ("https://w3id.org/dspace-sig/profile/http-pull", True),
        ("https://w3id.org/dspace-sig/profile/s3-push", False),
    ],
)
def test_pull_is_recognised_in_both_spellings(transfer_type: str, pull: bool) -> None:
    assert is_pull(transfer_type) is pull
