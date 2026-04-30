# CELINE Dataset API

Provides a secure, lineage-aware, metadata-rich interface to heterogeneous datasets (PostgreSQL, object storage, filesystem). Exposes a DCAT-AP 3.0 compatible catalogue, a governed SQL query interface, and OpenLineage-integrated provenance, designed to support Digital Twins, analytical applications, and DSSC-aligned dataspace participants.

---

## Core capabilities

### DCAT-AP 3.0 catalogue

Public catalogue endpoint returning `application/ld+json` responses conforming to DCAT-AP 3.0.

- `GET /catalogue` — full catalogue as a `dcat:Catalog` node with embedded `dcat:Dataset` and `dcat:Distribution` nodes
- `GET /catalogue/{id}` — single dataset by ID
- `POST /catalogue/search` — filtered search by `q`, `access_level`, `keywords`

Each dataset includes `dct:publisher`, `dcat:theme`, `dct:language`, `dct:spatial`, `dct:accrualPeriodicity`, and `odrl:hasPolicy` on every distribution. Publisher URI is derived from `governance.yaml`; the fallback is `settings.catalog_uri`.

The `downloadURL` is only present on distributions with `access_level: open`. All other distributions require negotiating access through a dataspace connector.

### Governed query API

SQL `SELECT` queries over exposed datasets with strict validation, server-side pagination, hard row caps, and row-level filters.

- `POST /query` — accepts `{"sql": "SELECT ...", "limit": 50, "offset": 0, "skip_count": false}`
- Validates SQL (SELECT-only, table allowlist, function allowlist)
- Supports spatial PostGIS functions: `ST_Intersects`, `ST_Within`, `ST_Contains`, `ST_Transform`, `ST_Distance`, `ST_SetSRID`, `ST_GeomFromGeoJSON`, `ST_Point`, `ST_XMin/YMin/XMax/YMax`, `ST_Extent`
- Supports `IN` clauses with tuples, string/numeric/date functions, aggregates
- Enforces `LIMIT`/`OFFSET` server-side
- Configurable query timeout via `QUERY_STATEMENT_TIMEOUT_MS` (default 5000ms)
- `skip_count: true` skips the `COUNT(*)` query to avoid full table scans
- Applies row-level filter plans from governance handlers (`direct_user_match`, `rec_registry`, `http_in_list`, `table_pointer`)

### EDR-gated query path (dataspace integration)

When queries arrive through the EDC data plane (via an Endpoint Data Reference), the API detects the EDR context via the `Edc-Contract-Agreement-Id` header and switches to a dataspace-specific enforcement path.

Enabled by:
```env
EDR_ENABLED=true
CONNECTOR_INTERNAL_URL=http://ds-connector:30001
```

EDR query flow:

1. Detects `Edc-Contract-Agreement-Id` and `Edc-Bpn` headers
2. Calls `ds-connector GET /internal/agreements/{id}/status` — checks the agreement is active
3. If the dataset has a `user_filter_column`, calls `ds-connector GET /internal/consent/check` — retrieves the list of subject IDs the consumer has consent for
4. Injects an SQL `IN (subject_ids)` predicate or a deny plan into the row filter pipeline
5. Skips the Keycloak/OPA path entirely — the EDC data plane already validated the EDR JWT

This path requires no JWT re-validation by dataset-api since the EDC data plane validates the bearer token before proxying.

### Governance and disclosure model

Access levels:
- `open` — no authentication required; `downloadURL` exposed in DCAT
- `internal` — JWT required; `ds:accessScope eq "dataspaces.query"` constraint in ODRL
- `restricted` — JWT + contract required; `ds:contractRequired eq "true"` in ODRL
- `secret` — not exposed in catalogue or EDC

Row-level filtering via the pluggable governance handler registry. Four built-in handlers are supported:
- `direct_user_match` — filter by user column
- `rec_registry` — lookup via REC registry
- `http_in_list` — HTTP-based allow list
- `table_pointer` — table-based lookup

Users in the `admins` group bypass row filters entirely. Service accounts bypass the `rec_registry` filter.

Governance overrides are supported via `governance.<app_name>.yaml` files merged with the base `governance.yaml`.

### Lineage and provenance

- OpenLineage ingestion via Marquez
- Namespace-based dataset grouping
- Governance facets embedded in lineage events (`userFilterColumn`, `medallion`, `classification`)
- Provenance surfaced in catalogue metadata

### Schema and metadata introspection

- JSON Schema (2020-12) generated from physical tables
- Column-level metadata for UI and clients

---

## API surface

- `GET /catalogue` — DCAT-AP catalogue (`application/ld+json`)
- `GET /catalogue/{id}` — single dataset
- `POST /catalogue/search` — filtered search
- `POST /query` — governed SQL query; EDR-gated when `EDR_ENABLED=true`
- `POST /admin/catalogue` — catalogue import
- `GET /health`

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
- `import catalogue` — validate and import dataset catalogue
- `row-filter add|remove|list` — manage row filters in exported YAML files

The `export governance` command reads `governance.yaml` files and propagates `dcat:` and `dataspace:` blocks to `DatasetEntry` records. The `expose: true` field on a source entry controls whether the dataset is visible in the catalogue and registered in EDC.

---

## governance.yaml integration

Dataset-api reads governance rules resolved by `celine-utils` `GovernanceResolver`. The following extended blocks are supported:

`dcat:` block — DCAT-AP metadata:
- `publisher_uri` — overrides the settings-level fallback
- `themes` — `dcat:theme` URIs (EU Publications Office vocabulary)
- `language_uris` — `dct:language` URIs
- `spatial_uris` — `dct:spatial` URIs
- `accrual_periodicity` — `dct:accrualPeriodicity` URI
- `conforms_to` — `dct:conformsTo` URI
- `temporal` — `dct:temporal` with `start` and `end` dates

`dataspace:` block — access control and ODRL hints:
- `contract_required` — adds `ds:contractRequired` constraint to ODRL
- `consent_required` — adds `ds:consentStatus eq active` constraint
- `odrl_action` — default action for the ODRL offer
- `purpose` — purpose values for ODRL purpose constraints
- `medallion` — data quality level

`expose: true` on the source entry (top-level, not under `dataspace:`) makes the dataset visible in the catalogue.

---

## Documentation

- [Architecture overview](https://celine-eu.github.io/projects/dataset-api/docs/architecture)
- [Catalogue Management](https://celine-eu.github.io/projects/dataset-api/docs/catalogue-management)
- [CLI operations](https://celine-eu.github.io/projects/dataset-api/docs/cli-operations)
- [Governance and security](https://celine-eu.github.io/projects/dataset-api/docs/governance-security)
- [Query engine](https://celine-eu.github.io/projects/dataset-api/docs/query-engine)

---

## Development and contribution

- Python >= 3.12
- Async SQLAlchemy
- Pydantic v2
- FastAPI + httpx
- sqlglot-based SQL validation

Before opening a PR:
- validate all YAML definitions
- add tests for new API behaviour
- include migrations for schema changes
- keep docs in sync with API behaviour

---

## License

Copyright © 2025 Spindox Labs

Licensed under the Apache License, Version 2.0.
