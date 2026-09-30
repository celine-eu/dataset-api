# Architecture & Core Concepts

This document explains **what the Dataset API is**, the **core domain model**, and how the major subsystems fit together.

---

## What the Dataset API is

The Dataset API is a **governed, read-only data access layer** that:

- publishes a **dataset catalogue** (DCAT-AP compatible)
- exposes a **restricted SQL query interface** over *catalogued* datasets
- provides **schema and metadata introspection** for clients and UIs
- integrates with **OpenLineage** to keep provenance and trust up to date
- acts as the **data plane** of a dataspace connector, serving contracted consumers
  through an EDR or a DPS pull token

The API is not an ingestion tool. Pipelines produce data; the Dataset API governs and serves it.

---

## System Context

### Actors

- **Producers (pipelines)**: create/refresh physical tables, declare `governance.yaml`, and emit lineage (OpenLineage)
- **Operators**: manage catalogue definitions via CLI and keep the Rego policies correct
- **Consumers (apps/DTs/BI)**: discover datasets, fetch schemas, run governed queries
- **Dataspace consumers**: query under a contract agreement, via an EDR token on
  `/query` or a DPS pull token on `/dps/public/query`

### External Dependencies

- **Physical storage**: PostgreSQL — a catalogue database (`DATABASE_URL`, schema
  `dataset_api`) and the warehouse the datasets live in (`DATASETS_DATABASE_URL`)
- **Authorization**: Rego policies in `POLICIES_DIR`, evaluated in-process
  (celine-sdk policy engine)
- **Identity provider**: OIDC, issues JWTs (users + service accounts)
- **Lineage backend**: Marquez, read by the CLI (`export openlineage`)
- **ds-connector(s)**: decide dataspace requests (`/internal/dataplane/authorize`),
  publish the EDR key set, and record disclosures
- **EDC control plane**: signals DPS data flows at `/dps/v1/*` (optional)
- **REC registry**: resolves a member's meters for the `rec_registry` row filter

---

## High-level Architecture

```
            +------------------------+
            | Pipelines (ETL/dbt/..) |
            +-----------+------------+
                        |
            governance.yaml / OpenLineage
                        v
                  +-----------+
                  | dataset-  |  export + import catalogue
                  | cli       |
                  +-----+-----+
                        |
                        v
+---------+     +---------------------+      +--------------------+
| Clients | --> |     Dataset API     | <--> | ds-connector(s)    |
| (apps)  |     |                     |      | authorize / audit  |
+---------+     | - Catalogue         |      +--------------------+
                | - Query Engine      |
+---------+     | - Row filters       |      +--------------------+
| Dataspace| -->| - Policy (Rego)     | <--> | EDC control plane  |
| consumer |    | - DPS data plane    |      | (DPS signalling)   |
+---------+     +----------+----------+      +--------------------+
                           |
                           v
                   +---------------+
                   | PostgreSQL    |
                   | tables/views  |
                   +---------------+
```

Subsystems:
- **Catalogue**: `/catalogue` (JSON-LD, and HTML for browsers),
  `/catalogue/{id}/schema`, `/catalogue/{id}/vocabulary`, optional
  `/catalogue/{id}/conformance`
- **Query engine**: `POST /query` ([query-engine.md](query-engine.md))
- **Row-filter registry**: pluggable handlers
  ([dataspace-row-filters.md](dataspace-row-filters.md))
- **DPS data plane**: optional ([dps-data-plane.md](dps-data-plane.md))

One instance can serve as the data plane of several connectors: the connector is
resolved per request from the token's issuer (`CONNECTOR_INTERNAL_URLS`).

---

## Core Domain Model

### Dataset

A **dataset** is a governed contract over a physical data asset.

**Dataset identity**
- `dataset_id` (stable string; often namespace-qualified)

**Dataset governance**
- `access_level`: `open` | `internal` | `restricted` | `secret`
- `expose` (listed in the catalogue, queryable) and `dataspace_expose` (offered to
  dataspace consumers)
- ownership / stewardship fields
- classification, tags, retention hints
- row filters (`lineage.facets.governance.rowFilters`)

**Dataset physical mapping**
- resolved storage reference (e.g., Postgres table/view)
- schema and column metadata derived from reflection

### Namespace

The namespace is the dataset's OpenLineage namespace (`lineage.namespace`). It
selects datasets on import (`--ns`), is rendered as `dct:isPartOf`, and is passed
to the policy. Exposure is decided by `expose` / `dataspace.expose`, not by
namespace. Medallion (`bronze`/`silver`/`gold`) is a separate governance hint,
inferred from the name when not declared, and rendered as `ds:medallion`.

### Distribution

In DCAT terms, a dataset can expose one or more **distributions** (e.g., SQL endpoint, files, API resource).
In practice:
- the API exposes a **query distribution**
- optional documentation and external references may be included

---

## Read-only Contract

Consumers cannot mutate data or catalogue state. Mutations happen only via:
- data pipelines (tables/views)
- CLI-managed catalogue imports
- admin endpoints used by CLI, which require the `dataset.admin` scope

This guarantees:
- reproducibility
- auditability
- consistent governance enforcement

---

## Catalogue vs Storage Reality

The catalogue is **validated against storage**.

- a PostgreSQL dataset is checked against the table it would be queried through
  (`backend_config.table`, or the table its id names); a missing one is skipped at
  import
- on every import, any PostgreSQL entry whose table no longer exists is deleted;
  other backends are catalogue-only, not checked and not queryable
- schema endpoints reflect what exists in storage today

The catalogue **never creates** physical data.

---

## Lifecycle of a Dataset

1. Pipeline creates/refreshes physical table/view
2. Pipeline declares governance (`governance.yaml`); lineage is emitted to Marquez
3. Operator exports catalogue YAML: from `governance.yaml` (`export governance`),
   Marquez (`export openlineage`) or PostgreSQL introspection (`export postgres`)
4. Operator curates YAML where needed (titles, descriptions, access levels, tags, docs)
5. CLI imports catalogue (create/update)
6. API lists the dataset in the catalogue (if `expose`) and offers it to dataspace
   consumers (if `dataspace.expose`)
7. Consumers query datasets under governance
8. Cleanup removes stale entries when physical assets disappear

---

## Data Integrity & Guardrails

- SQL must be validated (AST-based, allowlisted)
- dataset references must resolve to catalogued assets
- access is policy-controlled (Rego) on the ordinary path, and decided by the
  provider's ds-connector on dataspace requests
- limits and pagination protect the system from unbounded workloads

---

## Extension points

- routes: the `celine.dataset.routes` entry-point group
- row-filter handlers: the `celine.dataset.row_filters` entry-point group, or
  `ROW_FILTERS_MODULES`
- the public API for extensions: `celine.dataset.ext`
