# Governance & Security

This document defines the governance model (access levels, identity), the security posture (validation, isolation), and the authorization flow.

---

## Security Goals

- prevent data exfiltration by escaping the catalogue
- prevent side effects (no writes, no DDL)
- ensure authorization decisions are explainable and consistent
- keep resource usage bounded and predictable

---

## Access Levels

Each dataset declares an `access_level`:

| Access level | Intended exposure | Authentication | Authorization |
|---|---|---|---|
| `open` | public catalogue + read access | not required | not required |
| `internal` | internal audiences | required | required (policy) |
| `restricted` | limited audiences | required | required (policy) |
| `secret` | nobody | — | omitted from every catalogue surface; `/query` answers `403 Dataset not available` |

Notes:
- `open` does not mean “unsafe”; SQL hardening and limits still apply.
- `restricted` should be used when access depends on groups/roles/attributes.
- A dataset that states no `access_level` is `internal`; the import stores it as such.
- A level the import cannot read is refused there (`422`). A stored level nobody can
  read answers like `secret`: `403 Dataset not available`, indistinguishable from an
  unexposed dataset.
- With `POLICIES_CHECK_ENABLED=false`, any authenticated caller may read `internal`
  and `restricted` datasets.

---

## Identity Model

### JWT Inputs

The API receives a JWT from the OIDC provider (`CELINE_OIDC_*` settings) and
normalizes its claims into an authenticated user:

- `sub`
- `username` (`preferred_username`, falling back to `email`) and `email`
- `groups` (realm and organization groups)
- `roles` (realm roles and the client roles of the configured client)
- `scopes`
- `issuer`, `audiences`, and the raw `claims`

### Users vs Service Accounts

Service accounts (client credentials) are treated as first-class identities. A
token is a service account's when its `preferred_username` starts with
`service-account-` or it carries `gty=client-credentials`. Services are authorized
by **scopes**, users by **groups**.

You should avoid “god tokens” unless policy explicitly supports it.

---

## Authorization

The Dataset API is the Policy Enforcement Point. Policies are evaluated
**in-process** by the celine-sdk policy engine (regorus); there is no OPA server.
The engine loads every `.rego` file under `POLICIES_DIR` (default `./policies`,
optional data in `POLICIES_DATA_DIR`) once per process, so a policy change needs a
restart.

### Decision flow

1. API normalizes JWT → identity
2. API collects dataset metadata and the action (`read`)
3. API builds the policy input
4. the engine evaluates `data.<POLICIES_PACKAGE>.allow` and `.reason` (default
   package `celine.dataset`, `policies/celine/dataset.rego`)
5. API enforces the result: a deny is `403` with the policy's `reason`; an engine
   that cannot load or evaluate is `503`

Decisions are cached in memory when `POLICIES_CACHE_ENABLED` is true (the default).

### Policy input shape

```json
{
  "subject": {
    "id": "service-account-dt-app",
    "type": "service",
    "groups": ["ops"],
    "scopes": ["dataset.query"],
    "claims": {"…": "raw JWT claims"}
  },
  "resource": {
    "type": "dataset",
    "id": "datasets.gold.example",
    "attributes": {
      "access_level": "internal",
      "backend_type": "postgres",
      "namespace": "gold",
      "governance": {"…": "lineage governance facet, _-prefixed keys dropped"}
    }
  },
  "action": {"name": "read", "context": {}},
  "environment": {"timestamp": 1760000000.0, "source_service": "dataset-api"}
}
```

`subject.type` is `user`, `service` or `anonymous` (`id: "anonymous"`). Roles reach
the policy only inside `claims`; tags are not sent.

### Shipped policy

`policies/celine/dataset.rego` implements:

| Access level | Service (by scope) | User (by group) |
|---|---|---|
| `open` | never reaches the policy | never reaches the policy |
| `internal` | `dataset.query` | `admins`, `managers` or `viewers` |
| `restricted` | `dataset.admin` | `admins` |

Scopes use `.` as separator; `dataset.admin` matches every `dataset.*` scope.
Custom rules can read `resource.attributes.governance` and `namespace`.

---

## SQL Hardening (Non-negotiable)

All SQL is validated using an AST parser and an allowlist.

### Allowed
- `SELECT` queries only
- explicit projections and filters
- pagination enforced server-side: `limit` defaults to 100 and is capped at 10 000;
  every statement runs under `QUERY_STATEMENT_TIMEOUT_MS` (default 5000 ms)
- references only to catalogued datasets

### Rejected
- DDL: `CREATE`, `ALTER`, `DROP`, …
- DML: `INSERT`, `UPDATE`, `DELETE`, `MERGE`, …
- multiple statements / statement chaining
- functions not on allowlist ([query-engine.md](query-engine.md#function-and-expression-allowlist)
  says what is admitted and on what basis)
- table references that resolve to no catalogue entry. A reference resolves by
  exact id or, for a two-part `schema.table` name, by a literal id suffix, and is
  then governed as that entry; no match, or more than one, is a `400`

---

## Defense-in-depth Execution Pipeline

1. Reject an invalid bearer token up front
2. SQL parse + validation
3. Resolve every referenced table to a catalogue entry
4. Dataspace path only: every dataset must be `dataspace_expose`, then ds authorizes
   the query as a whole
5. Refuse datasets that are unexposed, `secret`, or carry an unreadable level
   (`403 Dataset not available`)
6. Map logical ids to physical tables: `backend_config.table`, or for postgres the
   table the id names; a backend with no SQL table is a `400`. No dataset skips
   the steps below
7. Ordinary path: authentication check required by the access level, then policy
8. Apply row filters, execute with limits and timeout
9. Dataspace path only: record the disclosure with ds
10. Return paginated results only

If any step fails → request fails.

---

## Catalogue administration

`POST /admin/catalogue` overwrites and deletes catalogue entries, including the
`expose` and `access_level` every other gate reads. It requires a valid token with
the `dataset.admin` scope, or a user in the `admins` group: `401` without a token,
`403` without either. `svc-dataset-api` holds the scope, so an import job
authenticates as the service with its own client credentials.

---

## Dataspace (EDR) path

With `EDR_ENABLED=true`, a request carrying `Edc-Contract-Agreement-Id` is a
dataspace request, and the ordinary path is never a fallback for it:

- the bearer is not validated against the OIDC provider; it is an EDR token,
  verified against the key set the provider's ds-connector publishes at
  `GET /internal/edr-jwks`. The consumer is the verified `aud`, the provider the
  verified `iss`
- the connector is `CONNECTOR_INTERNAL_URLS[iss]` when that map is set (an unlisted
  provider is `401`), otherwise `CONNECTOR_INTERNAL_URL` (unset is `503`)
- every dataset in the query must be offered to the dataspace (`dataspace_expose`),
  else `403` naming the withheld datasets
- one `POST /internal/dataplane/authorize` covers the whole query; a deny is `403`
  (*"Refused by ds: …"*), ds unreachable or erroring is `502`
- the row filter arrives in ds's decision and is applied here or nothing is served
  ([dataspace-row-filters.md](dataspace-row-filters.md))
- the disclosure is recorded with `POST /internal/audit/query`, naming the
  consenting subjects by DID, never by principal or key (RF-10 in
  [dataspace-row-filters.md](dataspace-row-filters.md))

Calls to ds authenticate with this service's OIDC client-credentials token. The DPS
pull path reuses the same enforcement; see [dps-data-plane.md](dps-data-plane.md).

---

## Auditability & Observability

Logged:
- subject
- dataset ids referenced
- allow/deny decision and reason
- validation errors (sanitized)

The query engine never logs a statement's literals: wherever it logs SQL it logs the
statement's shape, every literal replaced by `?` (QE-03 in
[query-engine.md](query-engine.md#logging)). On a dataspace request the completed SQL is
withheld from the log entirely: its predicate is a literal list of the consenting subjects
(RF-08 in [dataspace-row-filters.md](dataspace-row-filters.md)).

---

## Row-level filtering

A dataset may declare `row_filters` in governance, and a handler turns each one into
a predicate. On a request arriving through the dataspace the filter is not read from
governance here — it arrives inside ds's decision, with the consenting subjects named
in it, and this service applies it or serves nothing. That path has its own
specification, with clauses and tests:
[dataspace-row-filters.md](dataspace-row-filters.md).

**A service account is not narrowed by `rec_registry`** on the normal API path: it
is not a registry member, and once the policy has admitted it the whole table is
served. A service that relays such rows to a person narrows them itself or forwards
the person's token. The order the handler decides in is in
[dataspace-row-filters.md](dataspace-row-filters.md#rec_registry-on-the-normal-api-path).

---

## Configuration

| Variable | Default | Purpose |
|---|---|---|
| `POLICIES_CHECK_ENABLED` | `true` | evaluate policies; `false` admits any authenticated caller |
| `POLICIES_DIR` | `./policies` | `.rego` files loaded at start |
| `POLICIES_DATA_DIR` | unset | policy data |
| `POLICIES_PACKAGE` | `celine.dataset` | package whose `allow`/`reason` are evaluated |
| `POLICIES_CACHE_ENABLED` | `true` | cache decisions in memory |
| `EDR_ENABLED` | `false` | accept dataspace (EDR) requests |
| `CONNECTOR_INTERNAL_URL` | unset | the ds-connector, single-connector deployments |
| `CONNECTOR_INTERNAL_URLS` | `{}` | JSON map provider (`iss`) → ds-connector URL |
| `CELINE_OIDC_*` | — | OIDC provider and client settings (celine-sdk) |
