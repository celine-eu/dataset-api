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
- `roles`: the realm roles (`realm_access.roles`), the platform level
- `groups`: the groups the caller holds inside its organizations
  (`organization.<alias>.groups`), the organization level — which organization
  each is held in stays in the raw claims, where the row filter reads it
- `scopes`
- `issuer`, `audiences`, and the raw `claims`

### Users vs Service Accounts

Service accounts (client credentials) are treated as first-class identities. A
token is a service account's when its `preferred_username` starts with
`service-account-` or it carries `gty=client-credentials`. Services are authorized
by **scopes**, users by the **platform role** or their **organization groups**.

### Two levels, never mixed

A person's grants come from exactly two places, read apart
(`src/celine/dataset/security/groups.py`):

- **Platform.** The realm role `platform-admin` is the only platform-wide grant.
  Its holder skips every row filter, reads `restricted` datasets and may write the
  catalogue.
- **Organization.** `admins`, `managers` and `viewers` held inside an organization
  read `internal` datasets (an organization's `admins` reads what its `managers`
  read; `editors` grants nothing here), and only rows that belong to that
  organization: the dataset's row filter decides which (GS-06), and an `internal`
  dataset that declares no row filter is not readable through an organization at
  all (GS-07).

**A realm group grants nothing.** The top-level `groups` claim is not read, so a
token that still carries `/admins` there is not an administrator. Neither are the
retired realm roles `admin`, `manager`, `editor` and `viewer`, a client role, or
an organization group *named* `platform-admin`.

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
    "roles": [],
    "groups": [],
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
      "governance": {"…": "lineage governance facet, _-prefixed keys dropped"},
      "row_scoped": true
    }
  },
  "action": {"name": "read", "context": {}},
  "environment": {"timestamp": 1760000000.0, "source_service": "dataset-api"}
}
```

`subject.type` is `user`, `service` or `anonymous` (`id: "anonymous"`).
`subject.roles` carries the realm roles and `subject.groups` the organization
groups; neither ever carries a realm group. `subject.groups` says *which* groups,
not *where*: it is the coarse gate, and the row filter scopes by organization.
`resource.attributes.row_scoped` is true when the dataset declares any row
filter (`member_wide` included). A policy reads the platform level as
`"platform-admin" in input.subject.roles` and never from `groups` or `claims`.
Tags are not sent.

### Shipped policy

`policies/celine/dataset.rego` implements:

| Access level | Service (by scope) | User |
|---|---|---|
| `open` | never reaches the policy | never reaches the policy |
| `internal` | `dataset.query` | the `platform-admin` role; or `admins`, `managers` or `viewers` in an organization, when the dataset is `row_scoped` |
| `restricted` | `dataset.admin` | the `platform-admin` role |

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
the `dataset.admin` scope, or a user holding the `platform-admin` realm role: `401`
without a token,
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
- the read's audit record and that disclosure both carry the agreement id and the
  connector's `decision_ref` (GS-05); a disclosure that cannot be posted is logged at
  `ERROR` and does not fail the read

Calls to ds authenticate with this service's OIDC client-credentials token. The DPS
pull path reuses the same enforcement; see [dps-data-plane.md](dps-data-plane.md).

---

## Auditability & Observability

Logged:
- subject
- dataset ids referenced
- allow/deny decision and reason
- validation errors (sanitized)

### Access audit

Who read which dataset, and who was refused, is written as an **audit record**: one
JSON object per line on the logger `celine.audit`, in the shape every CELINE service
shares (`celine.sdk.audit`). A log pipeline selects the trail by logger name. Every
record carries `event` (`access` or `denied`), `service` (`dataset-api`), the caller
(`sub`, `client_id`, `service_account`), `action`, `method`, `route` (the route
template, never the raw path or query string), `resource`, `outcome` (`allowed`,
`denied`, `error`), `reason`, `request_id` (`X-Request-ID` / `X-Correlation-ID`),
`trace_id` (from `traceparent`) and `ts`. `access` records are `INFO`, `denied` ones
`WARNING`; the audit logger stays at `INFO` whatever `LOG_LEVEL` says.

The caller is named by `sub` and client id only, never by email, username or name.
`resource` holds catalogue dataset ids, which are platform identifiers. A `reason` is
a short code, never an error detail: a detail can quote the caller's statement.

#### GS-01 — A data read is recorded with the caller and each dataset read

Every request that reads rows writes **one record per dataset it read**, after the
outcome is known:

| route | `action` |
|---|---|
| `POST /query` | `dataset.query` |
| `POST /dps/public/query` (DPS pull) | `dataset.query` |
| `POST /catalogue/{id}/conformance` (the sample it reads) | `dataset.conformance` |

Each record's `resource` is one catalogue id: a join of three datasets writes three
records. The records of one request share its `request_id` — the caller's
`X-Request-ID` (or `X-Correlation-ID`) when it sent a usable one, otherwise one the
service mints. When no dataset resolved, the request writes one record with
`resource` null. On the user path the caller is the verified token's
`sub` and `azp`; an anonymous read of an `open` dataset has no caller. On the
dataspace path (EDR or DPS pull) the caller is the consumer participant — the
verified token's `aud`, never a header — as both `sub` and `client_id`.

A request that fails for a reason other than a refusal (GS-02) — a statement that
does not parse, a database timeout — is `access` with `outcome: error` and
`reason: "http <status>"`, or the exception's class name.

Catalogue metadata (`/catalogue`, `/catalogue/{id}`, its schema and vocabulary, the
HTML views) describes exposed datasets and returns no rows; it is not recorded.
Neither is `/health`.

#### GS-02 — A refusal is recorded with the caller and a reason code

Every refusal writes a `denied` record carrying the caller, whenever the caller is
known from a token that verified. As in GS-01, a refusal of a statement that
resolved to several datasets writes one `denied` record per dataset, under one
`request_id`; a refusal before any dataset resolved (`unknown_dataset`,
`sql_refused`, …) writes one, with `resource` null:

| reason | refusal |
|---|---|
| `policy` | the policy engine denied the read (`403`) |
| `auth_required` | the dataset, or its row filter, needs a login and none was given (`401`) |
| `not_available` | unexposed, `secret`, or an unreadable access level (`403`) |
| `unknown_dataset` | a table reference resolves to no catalogue entry — including a name outside the scope of the CTE it matches (QE-05) (`400`) |
| `ambiguous_dataset` | a two-part reference matches more than one entry (`400`) |
| `scope_unresolved` | the statement's scopes could not be resolved (QE-05) (`400`) |
| `sql_refused` | the SQL guard refused a statement kind, function, construct or operation, a comment, stacked statements, or a tautology inside `OR` (`400`) |
| `row_filter_unresolved` | a row filter could not be resolved for the caller (`403`) |
| `not_offered` | dataspace: a dataset is not `dataspace_expose` (`403`) |
| `ds_refused` | dataspace: ds refused the query (`403`) |
| `row_filter_unenforceable` | dataspace: ds asked for a row filter this instance cannot apply (`403`) |
| `pull_token` | DPS pull: no token, or one that is not valid or not live (`401`) |
| `flow_not_started` | DPS pull: a live token on a flow that is not `STARTED` (`403`) |
| `agreement_mismatch` | DPS pull: the agreement header names another agreement (`403`) |
| `http <status>` | any other `401` or `403` |

A presented bearer token that does not verify is refused before any route runs; it
is recorded with `action: "auth.token"`, `reason: "invalid_token"` and the route it
was sent to, and **no** caller: nothing in a token that failed verification is
trusted. DPS signalling refusals are recorded with `action: "dps.signal"` and
`no_token`, `invalid_token`, or `client_not_admitted` (which names the verified
client).

#### GS-03 — A catalogue import is recorded

`POST /admin/catalogue` rewrites the fields every gate reads. A completed import is
recorded as `catalogue.import`; a caller refused for lacking the `dataset.admin`
scope and the `platform-admin` role is recorded as `denied` with
`not_catalogue_admin` and its `sub`.

Tests: `tests/routes/test_access_audit.py`, the audit cases in
`tests/dps/test_pull.py`, `tests/dps/test_signaling_auth.py` and
`tests/routes/test_catalogue_conformance.py`.

#### GS-04 — The API documentation is served under dev only

Swagger UI (`/docs`), ReDoc (`/redoc`) and `/openapi.json` are served only under
`CELINE_ENV=dev`. Anywhere else they answer `404`, unless `CELINE_PUBLIC_DOCS` is
`true` (also `1`, `yes`, `on`). The schema is still built in process, so tooling
that reads `app.openapi()` is unaffected. Tests: `tests/security/test_api_docs.py`.

#### GS-05 — A dataspace read carries its release link

On the dataspace path (EDR or DPS pull) every record of the request also carries
`agreement_id` and `decision_ref`, and the `QueryExecuted` disclosure posted to the
connector (`POST /internal/audit/query`) carries the same pair, so the log line and
the provenance event of one read can be matched:

- `agreement_id` is the agreement the request was judged under: the
  `Edc-Contract-Agreement-Id` header on the EDR path, the signalled flow's agreement
  on a DPS pull (from the moment the flow is known, a refusal included).
- `decision_ref` is the connector's reference for the allow that served the read,
  read from the `POST /internal/dataplane/authorize` response and **opaque** here:
  never parsed, recomputed or invented. It is `null` on a refusal, and when the
  connector sent none — a connector older than the field is served as before, with
  no failure. The disclosure sends it only when it was received.

A record off the dataspace path carries neither field. A disclosure that cannot be
posted does not fail the read; it is logged at `ERROR`.

Tests: `tests/security/test_release_link.py`.

#### GS-06 — An organization's reader reads only its own organization's rows

A person reads an `internal` dataset through an organization when they hold
`admins`, `managers` or `viewers` inside it. Which rows is the dataset's row
filter (`binds: organization`, GS-08): `organization_match` with `column` serves the rows whose column holds the
alias of an organization the caller reads in, and nothing else. The column holds
organization aliases **by convention** (a REC's `community_id` is its organization's
alias); no mapping is applied, so a value in another vocabulary matches nobody.
With `org_type` alone it serves every row to a reader in any organization of that
type (`dso`), and with both it does both. A caller who reads in no matching
organization gets no rows. A caller in two organizations reads both; a token still
naming an organization the person has left reads it until the token is reissued,
and a reissued token is never answered from the previous token's cached plan.

The platform administrator and a service admitted by scope are not narrowed by
`organization_match`. A dataspace (delegated) request is refused by it: an
organization is not a consenting subject.

Tests: `tests/routes/test_organization_scoping.py`,
`tests/routes/test_platform_admin_role.py`.

#### GS-07 — An `internal` dataset that declares no row filter is closed to organizations

An `internal` dataset whose governance declares no row filter is readable by the
`platform-admin` role and by a service holding the scope, and by no organization's
group: nobody decided whose rows it holds. The refusal is `403` with reason
`internal dataset without a row filter - platform-admin or services only`
(audited as `policy`). A dataset meant for every member of every organization —
weather, public building data — says so in governance with the `member_wide` row
filter (`binds: organization`), which narrows nothing.

Tests: `tests/routes/test_organization_scoping.py`,
`tests/routes/test_platform_admin_role.py`.

#### GS-08 — A row filter's `binds` agrees with its handler

Every governance row filter says what it narrows rows to (celine-utils REQ-0010):
`binds: person` (the default when absent) or `binds: organization`. Each built-in
handler declares the one it is (`HANDLER_BINDS`): `direct_user_match`,
`rec_registry`, `subject_key_match`, `http_in_list` and `table_pointer` bind a
person; `organization_match` and `member_wide` bind an organization.
`POST /admin/catalogue` refuses with `422`, naming the dataset and the filter, a
`binds` that contradicts its handler or that is not one of the two values. A handler
from another package is checked for a readable value only.

It matters outside this service: ds gates a dataset on consent only for a filter
binding a person, and puts only that filter on the wire with the consenting
subjects. A person filter marked `organization` would un-gate personal data; an
organization filter left unmarked over-gates a contract-only dataset.

Tests: `tests/routes/test_import_row_filter_binds.py`.

#### GS-09 — A row filter names a column its table has

`POST /admin/catalogue` refuses with `422` a row filter whose `args.column` is not a
column of the dataset's physical table, naming the dataset, the filter, the column and
the table; every such filter of the import is named at once. A filter that names no
column (`member_wide`, `organization_match` with `org_type` alone) is not checked, and
neither is an entry whose table does not exist (it is skipped, as before).

A refused import changes nothing: no entry is created or updated, and no stale entry is
removed. Every entry is checked before the first write.

Without it, a filter on a missing column answers `400` at query time, and only to the
callers it narrows: a service and the platform administrator emit no predicate, so the
service paths stay green while every organization reader or person is refused. A
governance release that renames a filter column must therefore reach the catalogue in
the same step as the table's migration, and the import is what holds that order.

Tests: `tests/routes/test_import_row_filter_column.py`.

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

Whose rows an organization's reader gets is GS-06; an `internal` dataset with no
row filter at all is GS-07; what a filter binds is GS-08; the column it names is GS-09.

**A service account is not narrowed by `rec_registry`** or `organization_match` on the normal API path: it
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
| `CELINE_PUBLIC_DOCS` | unset | serve `/docs`, `/redoc`, `/openapi.json` outside dev (GS-04) |
| `CELINE_AUDIT_PSEUDONYM_KEY` | unset | keys the pseudonym the audit writes for a personal identifier that reaches a record (celine-sdk) |
