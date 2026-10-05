# CELINE Dataset API

Provides a secure, lineage-aware, metadata-rich interface to catalogued datasets: PostgreSQL tables are queryable, and object storage and filesystem backends can be catalogued. Exposes a DCAT-AP 3.0 compatible catalogue, a governed SQL query interface, and OpenLineage-integrated provenance, designed to support Digital Twins, analytical applications, and DSSC-aligned dataspace participants.

---

## Core capabilities

### DCAT-AP 3.0 catalogue

Public catalogue endpoint returning `application/ld+json` responses conforming to DCAT-AP 3.0.

- `GET /catalogue` — full catalogue as a `dcat:Catalog` node with embedded `dcat:Dataset` and `dcat:Distribution` nodes
- `GET /catalogue/{id}` — single dataset by ID
- `POST /catalogue/search` — filtered search by `q`, `access_level`, `keywords`

Each dataset includes `dct:publisher` and `odrl:hasPolicy` on its distribution, and `dcat:theme`, `dct:language`, `dct:spatial`, `dct:accrualPeriodicity`, `dct:conformsTo`, `dct:temporal` when governance provides them. Publisher URI is derived from `governance.yaml`; the fallback is `settings.catalog_uri`.

The `downloadURL` is only present on distributions with `access_level: open`. All other distributions require negotiating access through a dataspace connector.

### Governed query API

SQL `SELECT` queries over exposed datasets with strict validation, server-side pagination, hard row caps, and row-level filters.

- `POST /query` — accepts `{"sql": "SELECT ...", "limit": 50, "offset": 0, "skip_count": false}`
- Validates SQL (SELECT-only, table allowlist, function allowlist)
- Supports spatial PostGIS functions: `ST_Intersects`, `ST_Within`, `ST_Contains`, `ST_Transform`, `ST_Distance`, `ST_SetSRID`, `ST_GeomFromGeoJSON`, `ST_Point`, `ST_XMin/YMin/XMax/YMax`, `ST_Extent`, and `ST_AsGeoJSON`/`ST_Simplify` for a shape at a bounded size (QE-01)
- Admits a unary minus on a numeric literal only, so `ST_Point(-0.05, 45.1)` parses (QE-02)
- Supports `IN` clauses with tuples, string/numeric/date functions, aggregates
- Enforces `LIMIT`/`OFFSET` server-side, after the statement's top-level `ORDER BY` (carried onto the page query where its keys name selected columns; see QE-04 in the query engine docs)
- Configurable query timeout via `QUERY_STATEMENT_TIMEOUT_MS` (default 5000ms)
- `skip_count: true` skips the `COUNT(*)` query to avoid full table scans
- Applies row-level filter plans from governance handlers (`direct_user_match`, `rec_registry`, `subject_key_match`, `http_in_list`, `table_pointer`)

### EDR-gated query path (dataspace integration)

When queries arrive through the EDC data plane (via an Endpoint Data Reference), the API detects the EDR context via the `Edc-Contract-Agreement-Id` header and switches to a dataspace-specific enforcement path.

Enabled by:
```env
EDR_ENABLED=true
CONNECTOR_INTERNAL_URL=http://ds-connector:30001
```

EDR query flow:

1. `Edc-Contract-Agreement-Id` selects dataspace mode; the ordinary path is never a fallback for it
2. The `Authorization` header is **not** validated against Keycloak: an EDR token is signed with the provider EDC's vault key, which Keycloak's key set can never contain. The request reaches the route with no user identity — the state an unauthenticated request already reaches, so asserting the header costs a caller its identity and grants it nothing
3. Verifies the EDR token's signature against the key set ds publishes at `GET /internal/edr-jwks`. **This service is the EDR endpoint** — upstream EDC removed the data-plane proxy, so nothing validated the token before it arrived
4. Takes the consumer from the verified `aud` and the provider from the verified `iss`; neither comes from a header
5. Refuses the whole query (403) if any referenced dataset is not offered to the dataspace (`dataspace.expose` in governance → `dataspace_expose`); the dataset must also be `expose: true`
6. Calls `ds-connector POST /internal/dataplane/authorize` once for the whole query — agreement validity, the agreement↔consumer binding, purpose and the consented subjects are all ds's to answer, and it returns the verdict *and* the row-filter spec
7. Applies that spec through the row-filter handlers, then records the disclosure with `POST /internal/audit/query`
8. Skips the Keycloak/policy path entirely for datasets covered by the decision

#### One instance, several connectors

The connector is resolved **per request**, from the provider in the EDR token, so
one instance can be the data plane of more than one participant:

```env
EDR_ENABLED=true
CONNECTOR_INTERNAL_URL=http://connector-a:30001
CONNECTOR_INTERNAL_URLS={"did:web:a.example.org":"http://connector-a:30001","did:web:b.example.org":"http://connector-b:30001"}
```

Leave `CONNECTOR_INTERNAL_URLS` unset for a single-connector deployment; nothing
changes. Once it is set it is authoritative — a provider it does not name is
refused rather than sent to `CONNECTOR_INTERNAL_URL`, because asking a control
plane that has never heard of the agreement produces a denial that reads as a
consent problem. **Listing any connector means listing them all**, including the
one already at `CONNECTOR_INTERNAL_URL`.

This is a convenience for testing and validation, **not** a replacement for one
instance per participant: the warehouse is still one engine per process
(`DATASETS_DATABASE_URL`), and a catalogue entry names a table and never a
connection — so a participant whose data lives in its own database still needs
its own instance.

### DPS data plane ("EDC mode", prototype)

With `DPS_ENABLED=true`, dataset-api is also a [Data Plane Signaling](https://github.com/eclipse-dataplane-signaling/dataplane-signaling) data plane, which is where EDC moved EDR handling.

- An admitted control plane signals pull transfers at `/dps/v1/dataflows/*`.
- The `/start` answer carries a data address with a pull token issued by this service.
- `POST /dps/public/query` serves rows for that token only while the data flow is `STARTED`.
- Past the token, a pull takes the same path as an EDR query: ds's decision, row filters and the audit.

Flows are stored in the catalogue database, so any worker can serve a pull and flows survive a restart. The legacy path above and `POST /query` are unchanged; the two query endpoints are separate on purpose. Specification and configuration: [docs/dps-data-plane.md](docs/dps-data-plane.md).

### Governance and disclosure model

Access levels:
- `open` — no authentication required; `downloadURL` exposed in DCAT
- `internal` — JWT required; services need the `dataset.query` scope, users the `platform-admin` realm role, or `managers`/`viewers` inside an organization; ODRL carries `ds:accessScope eq "dataspaces.query"`
- `restricted` — JWT required; only the `dataset.admin` scope or the `platform-admin` realm role (`policies/celine/dataset.rego`); ODRL carries `ds:accessScope eq "dataspaces.query"` and `ds:consentStatus eq "active"`
- `secret` — omitted from every catalogue surface; not queryable (`403 Dataset not available`, as for an unexposed dataset)

An entry that states no level is `internal`, and the import stores it as such; an unknown level is refused at import.

Row-level filtering via the pluggable governance handler registry. Five built-in handlers are supported:
- `direct_user_match` — filter by user column
- `rec_registry` — lookup via REC registry
- `subject_key_match` — filter by the typed data keys (`pod:…`) the consent carried, for a holder whose rows are keyed by something only the organisation that collected the consent can name
- `http_in_list` — HTTP-based allow list
- `table_pointer` — table-based lookup

Further handlers can be registered through `ROW_FILTERS_MODULES` or the `celine.dataset.row_filters` entry-point group (see `celine.dataset.ext`).

Holders of the `platform-admin` realm role bypass row filters entirely; no group does, at either level (see [Identity Model](docs/governance-security.md#identity-model)). Service accounts bypass the `rec_registry` filter when they query on their own behalf — never when a dataspace decision delegates the query to them.

On a dataspace request the filter arrives from ds whole, naming the consenting subjects as `principals` and as `keys`; a filter this service cannot apply serves no rows. Specification: [docs/dataspace-row-filters.md](docs/dataspace-row-filters.md).

Governance overrides are supported via `governance.<app_name>.yaml` files merged with the base `governance.yaml`.

### Lineage and provenance

- Lineage read from Marquez by `dataset-cli export openlineage` (`MARQUEZ_URL` or `--marquez-url`)
- Namespace-based dataset grouping
- Governance facets embedded in lineage events (`rowFilters`, `accessLevel`, `classification`, `medallion`, `consentRequired`; legacy `userFilterColumn` still read)
- Provenance surfaced in catalogue metadata

### Schema and metadata introspection

- JSON Schema (2020-12) generated from physical tables
- Column-level metadata for UI and clients

---

## API surface

- `GET /catalogue` — DCAT-AP catalogue (`application/ld+json`)
- `GET /catalogue/{id}` — single dataset (HTML for browsers)
- `GET /catalogue/{id}/schema` — JSON Schema of the dataset's rows
- `GET /catalogue/{id}/vocabulary` — the dataset's ontology vocabulary
- `POST /catalogue/{id}/conformance` — SHACL conformance check, when `CONFORMANCE_ENABLED=true`
- `POST /catalogue/search` — filtered search
- `GET /` — HTML catalogue view
- `POST /query` — governed SQL query; EDR-gated when `EDR_ENABLED=true`
- `POST /admin/catalogue` — catalogue import; requires the `dataset.admin` scope or the `platform-admin` realm role
- `/dps/v1/dataflows/*`, `PUT /dps/v1/controlplanes`, `DELETE /dps/v1/controlplanes/{id}`, `GET /dps/registration` — DPS signalling, when `DPS_ENABLED=true`
- `POST /dps/public/query` — governed SQL query for a DPS pull token, when `DPS_ENABLED=true`
- `GET /health`
- `/docs`, `/redoc`, `/openapi.json` — OpenAPI UI and schema, under `CELINE_ENV=dev` only unless `CELINE_PUBLIC_DOCS=true`

---

## CLI

The CLI is the primary control plane for the Dataset API:

```bash
dataset-cli --help
```

Main commands:
- `export openlineage` — extract lineage from Marquez
- `export governance` — export governance rules to dataset entries
- `export postgres` — generate catalogue YAML from PostgreSQL schema introspection
- `import catalogue` — validate and import dataset catalogue (authenticates with `--token`/`DATASET_API_TOKEN`, else the `CELINE_OIDC_CLIENT_ID`/`_SECRET` client credentials)
- `row-filter add|remove|list` — manage row filters in exported YAML files

The `export governance` command reads `governance.yaml` files (plus `governance.<app>.yaml` overlays) and writes catalogue YAML for `import catalogue`; it needs neither a database nor Marquez. `expose` lists the dataset in the catalogue and makes it queryable; `dataspace.expose` offers it into the dataspace (required for EDR/DPS requests). An unset `expose` falls back to `dataspace.expose`; offered-but-unlisted is refused.

---

## governance.yaml integration

Dataset-api reads governance rules resolved by `celine.governance.GovernanceResolver` (from `celine-utils>=2.0`). The following extended blocks are supported:

`dcat:` block — DCAT-AP metadata:
- `publisher_uri` — overrides the settings-level fallback
- `themes` — `dcat:theme` URIs (EU Publications Office vocabulary)
- `language_uris` — `dct:language` URIs
- `spatial_uris` — `dct:spatial` URIs
- `accrual_periodicity` — `dct:accrualPeriodicity` URI
- `conforms_to` — `dct:conformsTo` URI
- `temporal` — `dct:temporal` with `start` and `end` dates

`dataspace:` block — access control and ODRL hints:
- `expose` — offer the dataset to dataspace consumers
- `consent_required` (or any `row_filters`) — adds `ds:consentStatus eq active` constraint
- `contract_required`, `odrl_action`, `purpose` — carried in the governance facet for dataspace consumers; not rendered into the ODRL offer, which always uses `odrl:use`
- `medallion` — data quality level (inferred from the name when unset)

`expose: true` on the source entry (top-level, not under `dataspace:`) makes the dataset visible in the catalogue.

---

## Documentation

- [Architecture overview](docs/architecture.md)
- [Catalogue Management](docs/catalogue-management.md)
- [CLI operations](docs/cli-operations.md)
- [Governance and security](docs/governance-security.md)
- [Query engine](docs/query-engine.md)
- [Dataspace row filters](docs/dataspace-row-filters.md)
- [DPS data plane](docs/dps-data-plane.md)

Published at [celine-eu.github.io](https://celine-eu.github.io/projects/dataset-api/).

---

## Development and contribution

- Python >= 3.12
- Async SQLAlchemy
- Pydantic v2
- FastAPI + httpx
- sqlglot-based SQL validation

### Running the tests

```bash
uv sync
uv run pytest          # or: task test
```

`pyproject.toml` requires `celine-sdk>=1.21.0`, the first release whose registry
self-service calls raise `RecRegistryApiError` with the registry's `code` (RF-12 in
[docs/dataspace-row-filters.md](docs/dataspace-row-filters.md)); `uv.lock` pins it.

The API, governance and DPS tests need a real PostgreSQL with PostGIS. **They never use
the database `DATABASE_URL` names**, because their fixtures drop and recreate the
catalogue schema on every test:

- by default they use `DATABASE_URL` with `_test` appended to the database name, on the
  same server (`.../datasets` → `.../datasets_test`), and create it on first run;
- `TEST_DATABASE_URL` overrides that, and must name a database ending in `_test`.

The suite refuses to run destructive setup against any database whose name does not end
in `_test`, or which is the one `DATABASE_URL` or `DATASETS_DATABASE_URL` points at
(`tests/testdb.py`). The SQL parser tests need no database. The process e2e in
`tests/e2e` runs with `DATASET_API_E2E=1` and creates, then drops, two more `*_test`
databases on the same server.

**One test database per server, so run one suite at a time.** Every run on a server uses
the same `*_test` database and its fixtures drop and recreate the catalogue schema on every
test, so two runs at once (two checkouts, or a run beside an agent's) fail each other's
tests at random. Give a concurrent run its own `TEST_DATABASE_URL` (a name ending in
`_test`, such as `.../datasets_b_test`; it is created on first use).

Before opening a PR:
- validate all YAML definitions
- add tests for new API behaviour
- include migrations for schema changes
- keep docs in sync with API behaviour

---

## License

Copyright © 2025 Spindox Labs

Licensed under the Apache License, Version 2.0.
