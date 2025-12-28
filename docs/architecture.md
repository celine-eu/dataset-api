# Architecture & Core Concepts

This document explains **what the Dataset API is**, the **core domain model**, and how the major subsystems fit together.

---

## What the Dataset API is

The Dataset API is a **governed, read-only data access layer** that:

- publishes a **dataset catalogue** (DCAT-AP compatible)
- exposes a **restricted SQL query interface** over *catalogued* datasets
- provides **schema and metadata introspection** for clients and UIs
- integrates with **OpenLineage** to keep provenance and trust up to date

The API is not an ingestion tool. Pipelines produce data; the Dataset API governs and serves it.

---

## System Context

### Actors

- **Producers (pipelines)**: create/refresh physical tables and emit lineage (OpenLineage)
- **Operators**: manage catalogue definitions via CLI and ensure OPA/policy config is correct
- **Consumers (apps/DTs/BI)**: discover datasets, fetch schemas, run governed queries

### External Dependencies

- **Physical storage**: PostgreSQL (tables, views)
- **Authorization**: OPA (policy decision point)
- **Lineage backend**: Marquez (OpenLineage ingestion/query)
- **Identity provider**: issues JWTs (users + service accounts)

---

## High-level Architecture

```
            +------------------------+
            | Pipelines (ETL/dbt/..) |
            +-----------+------------+
                        |
                        | OpenLineage events
                        v
                  +-----------+
                  | Marquez   |
                  +-----+-----+
                        |
                        | export lineage + metadata
                        v
+---------+     +---------------------+      +--------------------+
| Clients | --> |     Dataset API     | <--> | OPA (Policy)       |
| (apps)  |     |                     |      | allow/deny         |
+---------+     | - Catalogue         |      +--------------------+
                | - Query Engine      |
                | - Schema API        |
                | - Metadata API      |
                +----------+----------+
                           |
                           v
                   +---------------+
                   | PostgreSQL    |
                   | tables/views  |
                   +---------------+
```

---

## Core Domain Model

### Dataset

A **dataset** is a governed contract over a physical data asset.

**Dataset identity**
- `dataset_id` (stable string; often namespace-qualified)

**Dataset governance**
- `access_level`: `open` | `internal` | `restricted`
- ownership / stewardship fields
- classification, tags, retention hints

**Dataset physical mapping**
- resolved storage reference (e.g., Postgres table/view)
- schema and column metadata derived from reflection

### Namespace

Namespaces are a first-class taxonomy for lifecycle and intent:
- `raw`: ingestion/staging
- `silver`: enriched internal
- `gold`: curated/exposed

Namespaces drive:
- catalogue selection filters
- policy rules (e.g., only `gold` exposed externally)
- operational grouping

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
- admin endpoints used by CLI

This guarantees:
- reproducibility
- auditability
- consistent governance enforcement

---

## Catalogue vs Storage Reality

The catalogue is **validated against storage**.

Expected behaviors:
- if a dataset points to a missing table/view, it should be marked invalid and/or removed during cleanup
- imports reconcile desired state (YAML) vs actual DB objects
- schema endpoints reflect what exists in storage today

The catalogue **never creates** physical data.

---

## Lifecycle of a Dataset (Conceptual)

1. Pipeline creates/refreshes physical table/view
2. Lineage is emitted to Marquez (OpenLineage)
3. Operator exports lineage-derived candidates (CLI)
4. Operator curates YAML (titles, descriptions, access levels, tags, docs)
5. CLI imports catalogue (create/update)
6. API exposes dataset in catalogue (if allowed)
7. Consumers query datasets under governance
8. Cleanup removes stale entries when physical assets disappear

---

## Data Integrity & Guardrails

- SQL must be validated (AST-based, allowlisted)
- dataset references must resolve to catalogued assets
- access is policy-controlled (OPA)
- limits and pagination protect the system from unbounded workloads

