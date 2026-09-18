# DPS data plane ("EDC mode")

**Status: prototype.** It is off by default and not yet driven by a connector end to end.

The Eclipse Dataspace Components (EDC) connector deprecated its own data plane and moved
endpoint data reference (EDR) handling to data planes that speak
[Data Plane Signaling](https://github.com/eclipse-dataplane-signaling/dataplane-signaling)
(DPS). With `DPS_ENABLED=true`, dataset-api is such a data plane for its governed query
API:

1. a control plane signals it to start a pull transfer;
2. it answers with a data address that carries a pull token;
3. the consumer calls the query API with that token;
4. rows flow only while the transfer's data flow is `STARTED`.

### Two query endpoints, on purpose

`POST /query` and `POST /dps/public/query` stay separate. This was decided by the
maintainer on 2026-09-17.

- `/query` serves API clients with their own tokens (user or service). With
  `EDR_ENABLED`, the `Edc-Contract-Agreement-Id` header switches it to the legacy EDR path
  (see [governance-security.md](governance-security.md)). This mode changes neither.
- `/dps/public/query` serves only holders of a DPS pull token.

Keeping the two concerns apart means neither endpoint has to guess which kind of
credential it was given.

Code: `src/celine/dataset/dps/`. Tests: `tests/dps/`. Each clause below names its tests.

## Compatibility

| Counterpart | Version followed |
|---|---|
| EDC control plane | 0.18.0 (`data-protocols/data-plane-signaling`), which implements DPS **v1.0-RC2** |
| DPS specification | v1.0-RC2, and the renames of RC3/RC4 are accepted on input |
| Reference data plane layout | `org.eclipse.dataplane-core:dataplane-sdk` (`/v1/dataflows`, `/v1/controlplanes`) |

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `DPS_ENABLED` | `false` | mount the routes below |
| `DPS_DATAPLANE_ID` | `dataset-api` | the id this data plane registers under |
| `DPS_SIGNALING_URL` | `http://localhost:8001` | base URL the control plane reaches; the registered endpoint is this + `/dps/v1/dataflows` |
| `DPS_PUBLIC_URL` | `http://localhost:8001` | base URL consumers reach; the data address endpoint is this + `/dps/public` |
| `DPS_TRANSFER_TYPES` | `["HttpData-PULL"]` | accepted and registered transfer types; pull only |
| `DPS_LABELS` | `[]` | registration labels |
| `DPS_CONTROL_PLANE_CLIENTS` | `[]` | OIDC client ids allowed to signal; empty admits nobody |
| `DPS_TOKEN_SIGNING_KEY` | — (required when enabled) | PEM EC P-256 private key for pull tokens. It must be the same on every worker and kept across restarts |
| `DPS_TOKEN_KEY_ID` | unset | `kid` stamped on pull tokens |

The pull path calls ds exactly as the legacy EDR path does, so it also needs
`CONNECTOR_INTERNAL_URL` and the OIDC client settings. The flows are stored in the
catalogue database (`DATABASE_URL`), so run `alembic upgrade head` before enabling the
mode.

## Clauses

### DPS-01 — Off unless enabled

With `DPS_ENABLED` unset or false, none of the routes below is mounted. With it set, a
missing `DPS_TOKEN_SIGNING_KEY` fails the start-up.
Tests: `tests/dps/test_routes_mounting.py`.

### DPS-02 — Only admitted control planes may signal

Every route under `/dps` except `/dps/public` requires `Authorization: Bearer <token>`.

- The token is validated with the service's OIDC settings: issuer, audience and signature.
- The caller is the token's `azp`, or else its `client_id`. It must be listed in
  `DPS_CONTROL_PLANE_CLIENTS`.
- A missing or invalid token gets `401`. A valid token from a client not in the list gets
  `403`.

This matches EDC's `oauth2_client_credentials` signalling authorisation profile.
Tests: `tests/dps/test_signaling_auth.py`.

### DPS-03 — Messages in either spec spelling

The data flow id is read from `dataFlowId` or `processId`. The transfer type is read from
`profile` or `transferType`. Unknown fields are ignored.

The facts a decision rests on are required: flow id, `participantId`, `counterPartyId`,
`agreementId`, `datasetId` and transfer type. A message missing one of them is refused
with `422`.
Tests: `tests/dps/test_messages.py`.

### DPS-04 — Prepare

`POST /dps/v1/dataflows/prepare` with a `DataFlowPrepareMessage`. This is the consumer
side of a pull.

- A transfer type not in `DPS_TRANSFER_TYPES` gets `400`.
- An existing flow id gets `409`.
- Otherwise the flow is created in `PREPARED` and the answer is `200` with a
  `DataFlowStatusMessage` that has no data address.

Tests: `tests/dps/test_signaling_api.py`.

### DPS-05 — Start

`POST /dps/v1/dataflows/start` with a `DataFlowStartMessage`. This is the provider side
of a pull.

- A transfer type not in `DPS_TRANSFER_TYPES` gets `400`.
- A message that carries a `dataAddress` gets `400`: the spec forbids one on a pull start.
- An existing flow id gets `409`.
- Otherwise the flow is created in `STARTED` and the answer is `200` with a
  `DataFlowStatusMessage`. Its `dataAddress` has:
  - `endpointType`: `https://w3id.org/idsa/v4.1/HTTP`
  - `endpoint`: `{DPS_PUBLIC_URL}/dps/public`
  - endpoint properties in EDC's namespace: `endpoint`, `endpointType`, `authorization`
    (the pull token) and `authType` = `bearer`.

The client calls `{endpoint}/query`.
Tests: `tests/dps/test_signaling_api.py`.

### DPS-06 — The pull token

The pull token is an ES256 JWT with these claims:

- `jti`
- `aud`: the message's `counterPartyId`
- `iss` and `sub`: both the message's `participantId`
- `iat`

It has no `exp`. The agreement, dataset and flow are not in the token: they are held
against the `jti`. A flow has at most one live token.

The token is signed with `DPS_TOKEN_SIGNING_KEY`. Without that key nothing is signed.
Tests: `tests/dps/test_tokens.py`, `tests/dps/test_signaling_api.py`.

### DPS-07 — Transitions

All of these are `POST /dps/v1/dataflows/{id}/<signal>`.

- `suspend`: `STARTED` → `SUSPENDED`. The live token is retired.
- `resume`: `SUSPENDED` → `STARTED`.
  - A provider flow answers with a new data address and a new token. The old token stays
    dead, and a data address in the message is ignored.
  - A consumer flow records the data address it is given, if any, without its credential.
- `terminate`: any non-terminal state → `TERMINATED`. The token is retired. Terminating
  an already `TERMINATED` flow succeeds and changes nothing.
- `completed`: `STARTED` → `COMPLETED`. The token is retired.
- `started`: consumer flows only, `PREPARED` → `STARTED`. The message must carry a data
  address, which is recorded without its credential.

Errors:

- Any other transition, including one out of `COMPLETED`, gets `409`.
- An unknown flow gets `404`. So does a flow created by a different caller.

Tests: `tests/dps/test_flows.py`, `tests/dps/test_signaling_api.py`.

### DPS-08 — Status

`GET /dps/v1/dataflows/{id}/status` answers `{"dataFlowId", "state"}`.
Tests: `tests/dps/test_signaling_api.py`.

### DPS-09 — The pull

`POST /dps/public/query` takes the same body as `POST /query`. It is a separate endpoint
by decision (see *Two query endpoints* above). The token goes in
`Authorization`, with or without the `Bearer` prefix.

Refusals:

- `401`: no token, a token not signed by this data plane, or a token that is not the
  flow's live one.
- `403`: the flow is not `STARTED`.
- `403`: an `Edc-Contract-Agreement-Id` header that differs from the flow's agreement.

Otherwise the request becomes a dataspace request:

- the agreement is the flow's;
- the consumer is the token's `aud`;
- the purpose comes from `Edc-Purpose`, and the transfer id from `Edc-Transfer-Process-Id`.

From there it follows the legacy EDR path unchanged: the `dataspace_expose` gate, then
ds's `POST /internal/dataplane/authorize`, then row filters, then the disclosure audit.
Tests: `tests/dps/test_pull.py` (PostgreSQL).

### DPS-10 — Registration message

`GET /dps/registration` (guarded as in DPS-02) returns `dataplaneId`, `endpoint`
(`{DPS_SIGNALING_URL}/dps/v1/dataflows`), `transferTypes` and `labels`. This is the body
of EDC's `PUT /v4/dataplanes` (or `/v5beta/participants/{id}/dataplanes`), without the
`authorization` profile. The registrar adds that profile, because it holds the control
plane's credentials. This service never registers itself.
Tests: `tests/dps/test_signaling_api.py`.

### DPS-11 — Control plane registration

- `PUT /dps/v1/controlplanes` with `{controlplaneId, endpoint, authorization?}` registers
  or replaces a control plane. The `authorization` profile is accepted but not stored.
- A `controlplaneId` registered by another caller gets `409`.
- `DELETE /dps/v1/controlplanes/{id}` answers `204`, or `404` when the caller did not
  register that id.

Tests: `tests/dps/test_signaling_api.py`.

### DPS-12 — Callbacks to the control plane

A notification is `POST {base}/transfers/{id}/dataflow/{prepared|started|completed|errored}`
with a `DataFlowStatusMessage` and this service's bearer token.

The base is the message's `callbackAddress`. If there is none, it is the endpoint the same
caller registered under DPS-11. With neither, the notification fails, and so does a
non-2xx answer.

The synchronous flows above send no callbacks.
Tests: `tests/dps/test_callbacks.py`, `tests/dps/test_signaling_api.py`.

### DPS-13 — Flows live in the catalogue database

Data flows and control-plane registrations are rows in the catalogue schema:
`dps_data_flows` and `dps_control_planes`. They are created by alembic revision
`d7a2e5f19c40`.

- **Workers and restarts.** Every worker reads and writes the same rows, and a flow
  outlives a restart. With the shared signing key (DPS-06), a pull token issued by one
  worker is served by any other, before or after a restart.
- **Serialised signals.** Signals for one flow are serialised with `SELECT … FOR UPDATE`.
  Of two concurrent `resume` signals, one succeeds and the other gets `409`. Of two
  concurrent `start` signals with the same id, one gets `409`.
- **One token per flow.** `token_id` is unique, so a token stands for at most one flow.
- **No stored credentials.** Neither the handed-out token nor a control plane's
  `authorization` profile is stored.

Tests: `tests/dps/test_flows.py` (the repository contract, in memory) and
`tests/dps/test_store.py` (PostgreSQL: several workers, a restart, and separate
processes).

## Not covered yet

- **Push transfers and asynchronous transitions.**
- **An end-to-end pull through an EDC control plane.** It needs a connector built with the
  DPS modules instead of the legacy in-process signalling.
