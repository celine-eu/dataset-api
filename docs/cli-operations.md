# CLI & Operations

This document covers day-to-day workflows, the operational safety model, configuration, and troubleshooting.

---

## Operational Model

The Dataset API is immutable from the perspective of clients.
All changes happen through controlled workflows:
- pipelines change physical data
- CLI imports catalogue metadata and reconciles state
- policies are updated in OPA

This prevents “clickops” drift.

---

## CLI: Core Workflows

### 1) Validate a catalogue
Validation should happen before import.

```bash
dataset-cli validate catalogue catalogue.yaml
```

What validation should check:
- YAML schema correctness
- required fields
- duplicate dataset_ids
- invalid access_level values
- unresolved references (optional)

### 2) Import a catalogue
```bash
dataset-cli import catalogue catalogue.yaml
```

Expected output:
- created / updated counts
- per-dataset errors (if partial success supported)
- cleanup summary (if enabled)

### 3) Dry-run import
```bash
dataset-cli import catalogue catalogue.yaml --dry-run
```

Dry-run should print:
- final selected dataset_ids after filters
- planned create/update/delete actions

### 4) Export lineage candidates
```bash
dataset-cli export openlineage
```

Expected:
- fetch from lineage backend
- map lineage → dataset_ids
- produce a governance/candidate YAML for curation

---

## Filters & Selection

Use include/exclude patterns:
- `+pattern` include
- `-pattern` exclude

Example:
```bash
dataset-cli import catalogue catalogue.yaml \
  --filter +datasets.*.gold.* \
  --filter -datasets.*.gold.experimental_*
```

---

## Configuration Surfaces

Typical configuration areas:
- API base URL
- DB connection
- OPA URL
- lineage backend URL/credentials
- query limits/timeouts
- public catalogue exposure rules

Keep configuration environment-driven and documented.

---

## Troubleshooting Guide

### “Dataset not found”
- dataset_id missing from catalogue DB
- import filters excluded it
- import failed validation
- cleanup removed it due to missing physical table

### “Forbidden (403)”
- JWT missing groups/roles required by OPA policy
- service account not mapped in policy
- access_level is `restricted` and policy denies
- token missing scope used by policy

### “SQL rejected”
- non-SELECT statement
- unknown function
- references non-catalogued table
- multiple statements detected

### “Schema endpoint fails”
- physical table missing or renamed
- reflection permissions insufficient
- unsupported column type mapping

### “Lineage export is empty”
- lineage backend not receiving OpenLineage events
- wrong namespace filters
- wrong time window
- wrong Marquez project or API base URL

---

## Operational Checks (Recommended)

- periodic import dry-run in CI
- periodic cleanup run (or as part of import)
- dashboards for:
  - authorization denies
  - validation rejects
  - query latency
  - lineage ingestion lag

