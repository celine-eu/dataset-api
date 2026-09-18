"""The Data Plane Signaling wire shapes.

**Tolerant reader.** EDC 0.18.0 implements the protocol as it stood at v1.0-RC2
(`processId`, `transferType`); the spec has since renamed them (`dataFlowId` in
RC4, `profile` from RC3). Both spellings are accepted, so the data plane keeps
working across that rename. Unknown fields are ignored: these messages describe a
transfer, they are not authorization evidence — the decision is still ds's.

**Conservative writer.** A status message always carries `state`: EDC's
`DataFlowStatusMessageToDataFlowResponseTransformer` calls `state.endsWith("ING")`
and fails on a missing one.
"""
from __future__ import annotations

import uuid
from typing import Any, Optional

from pydantic import AliasChoices, BaseModel, ConfigDict, Field

#: `endpointType` of an HTTP pull endpoint, as the DSP spec and EDC's
#: `Endpoint.url` spell it.
HTTP_ENDPOINT_TYPE = "https://w3id.org/idsa/v4.1/HTTP"

#: EDC's own namespace. Its data plane names EDR properties in it
#: (`DataPlaneAuthorizationServiceImpl`), and a consumer's management API
#: compacts them to `endpoint` / `authorization` — the names ds reads today.
EDC_NAMESPACE = "https://w3id.org/edc/v0.0.1/ns/"
EDR_ENDPOINT = EDC_NAMESPACE + "endpoint"
EDR_ENDPOINT_TYPE = EDC_NAMESPACE + "endpointType"
EDR_AUTHORIZATION = EDC_NAMESPACE + "authorization"
EDR_AUTH_TYPE = EDC_NAMESPACE + "authType"


class _Tolerant(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)


class EndpointProperty(_Tolerant):
    type: str = Field(default="EndpointProperty", alias="@type")
    name: str
    # A string on every property but EDC's `responseChannel`, which nests a
    # data address.
    value: Any = None


class DataAddress(_Tolerant):
    """A DSP `DataAddress`, as EDC's `DspDataAddress` serialises it."""

    type: str = Field(default="DataAddress", alias="@type")
    endpointType: Optional[str] = None
    endpoint: Optional[str] = None
    endpointProperties: list[EndpointProperty] = Field(default_factory=list)

    def property(self, name: str) -> Any:
        for prop in self.endpointProperties:
            if prop.name == name:
                return prop.value
        return None

    def wire(self) -> dict[str, Any]:
        return self.model_dump(by_alias=True, exclude_none=True)


def http_pull_address(endpoint: str, token: str) -> DataAddress:
    """The pull data address: where to call, and the bearer to call it with.

    The endpoint is carried twice — as `endpoint` for DSP, and as `edc:endpoint`
    because EDC's `DspDataAddressToDataAddressTransformer` and the DSP
    serializer carry properties, not the `endpoint` field, to the consumer.
    """
    return DataAddress(
        endpointType=HTTP_ENDPOINT_TYPE,
        endpoint=endpoint,
        endpointProperties=[
            EndpointProperty(name=EDR_ENDPOINT, value=endpoint),
            EndpointProperty(name=EDR_ENDPOINT_TYPE, value=HTTP_ENDPOINT_TYPE),
            EndpointProperty(name=EDR_AUTHORIZATION, value=token),
            EndpointProperty(name=EDR_AUTH_TYPE, value="bearer"),
        ],
    )


class _FlowMessage(_Tolerant):
    """Fields shared by `DataFlowPrepareMessage` and `DataFlowStartMessage`."""

    messageId: Optional[str] = None
    participantId: str
    counterPartyId: str
    dataspaceContext: Optional[str] = None
    flow_id: str = Field(validation_alias=AliasChoices("dataFlowId", "processId"))
    agreementId: str
    datasetId: str
    callbackAddress: Optional[str] = None
    transfer_type: str = Field(validation_alias=AliasChoices("profile", "transferType"))
    claims: Optional[dict[str, Any]] = None
    labels: Optional[list[str]] = None
    metadata: Optional[dict[str, Any]] = None


class DataFlowPrepareMessage(_FlowMessage):
    pass


class DataFlowStartMessage(_FlowMessage):
    dataAddress: Optional[DataAddress] = None


class DataFlowStartedNotificationMessage(_Tolerant):
    messageId: Optional[str] = None
    dataAddress: Optional[DataAddress] = None


class DataFlowSuspendMessage(_Tolerant):
    messageId: Optional[str] = None
    reason: Optional[str] = None


class DataFlowResumeMessage(_Tolerant):
    messageId: Optional[str] = None
    dataAddress: Optional[DataAddress] = None


class DataFlowTerminateMessage(_Tolerant):
    messageId: Optional[str] = None
    reason: Optional[str] = None


class ControlPlaneRegistrationMessage(_Tolerant):
    controlplaneId: str
    endpoint: str
    authorization: Optional[dict[str, Any]] = None


def status_message(
    flow_id: str,
    state: str,
    *,
    data_address: Optional[DataAddress] = None,
    error: Optional[str] = None,
) -> dict[str, Any]:
    """A `DataFlowStatusMessage`."""
    body: dict[str, Any] = {
        "messageId": str(uuid.uuid4()),
        "dataFlowId": flow_id,
        "state": state,
    }
    if data_address is not None:
        body["dataAddress"] = data_address.wire()
    if error is not None:
        body["error"] = error
    return body


def is_pull(transfer_type: str) -> bool:
    """`HttpData-PULL` (EDC) or `…/http-pull` (a spec profile)."""
    parts = transfer_type.rsplit("/", 1)[-1].replace("_", "-").split("-")
    return any(p.upper() == "PULL" for p in parts)
