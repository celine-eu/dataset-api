# CLI & Operations

This document covers day-to-day workflows, the operational safety model, configuration, and troubleshooting.

---

## Operational Model

The Dataset API is immutable from the perspective of clients.
All changes happen through controlled workflows:
- pipelines change physical data
- CLI imports catalogue metadata and reconciles state
- policies are `.rego` files in `POLICIES_DIR`, loaded at start (restart to apply
  changes)

This prevents “clickops” drift.

---

## CLI: Core Workflows

The entry point is `dataset-cli`, with three command groups: `export`, `import` and
`row-filter`. Every command takes `--help`.

### 1) Export catalogue YAML

From `governance.yaml` files (the usual path; needs neither a database nor Marquez,
and merges `governance.<app>.yaml` overlays):

```bash
dataset-cli export governance "/path/to/**/governance.yaml" -o ./data/governance \
  [--owners owners.yaml] [--backend-type postgres]
```

From PostgreSQL introspection (writes safe defaults, `access_level: internal`, to edit
before import):

```bash
dataset-cli export postgres -o ./catalogue \
  [--schema public] [--tables 'public.*' --tables '-public.tmp_*'] \
  [--namespace gold] [--expose] [--single-file] [--dry-run]
```

`--database-url` defaults to `DATABASE_URL`.

From lineage in Marquez:

```bash
dataset-cli export openlineage --ns '*' -o ./data/openlineage --expose
```

`--marquez-url` defaults to `MARQUEZ_URL`; `--ns` supports `*`, `+ns`, `-ns`.

### 2) Import a catalogue

```bash
dataset-cli import catalogue --input "./data/governance/*.yaml" \
  --api-url http://localhost:8001
```

The import authenticates: `POST /admin/catalogue` requires the `dataset.admin`
scope. The token is `--token` (or `DATASET_API_TOKEN`) when given, otherwise a
client-credentials token for `CELINE_OIDC_CLIENT_ID` / `CELINE_OIDC_CLIENT_SECRET`
(`svc-dataset-api` holds the scope). With neither, the CLI stops before sending
anything. `--dry-run` needs no token.

Validation happens here: each entry is checked against the catalogue schema
(`title` and `backend_type` required, `backend_type` one of `postgres`, `s3`, `fs`,
`quantumleap`, `context_broker`). Invalid entries are skipped with a warning;
`--strict` fails on the first one. A `dataset_id` declared differently in two inputs
is refused unless `--allow-conflicts`.

The CLI prints file, namespace and filter counts, then `Catalogue import complete.`.
The server upserts on `dataset_id`, skips entries whose PostgreSQL table does not
exist, and deletes catalogue entries whose table has disappeared. See
[catalogue-management.md](catalogue-management.md).

### 3) Dry-run import

```bash
dataset-cli import catalogue --input "./data/governance/*.yaml" \
  --api-url http://localhost:8001 --dry-run
```

Prints the dataset_ids selected after `--ns` and `--datasets`, then exits without
validating or contacting the API.

### 4) Edit row filters in exported YAML

```bash
dataset-cli row-filter add "./data/governance/*.yaml" datasets.ds_dev_gold.example \
  --handler direct_user_match --args column=user_id
dataset-cli row-filter remove "./data/governance/*.yaml" datasets.ds_dev_gold.example \
  --handler direct_user_match
dataset-cli row-filter list "./data/governance/*.yaml"
```

Handlers and their args are listed in
[dataspace-row-filters.md](dataspace-row-filters.md#handlers).

---

## Filters & Selection

Use include/exclude patterns on `dataset_id` with `--datasets` (repeatable), and on
namespace with `--ns`:
- `+pattern` (or bare) include
- `-pattern` exclude

Example:
```bash
dataset-cli import catalogue --input "./data/governance/*.yaml" \
  --api-url http://localhost:8001 \
  --datasets '+datasets.*.gold.*' \
  --datasets '-datasets.*.gold.experimental_*'
```

---

## Configuration Surfaces

Settings come from the environment, over an optional YAML file (`DATASET_CONFIG`,
else `./config.yaml`).

| Area | Variables |
|---|---|
| Databases | `DATABASE_URL` (catalogue), `DATASETS_DATABASE_URL` (warehouse), `CATALOGUE_SCHEMA` (`dataset_api`), `DB_POOL_RECYCLE_SECONDS` (300) |
| URIs | `API_BASE_URL`, `CATALOG_URI`, `DATASET_BASE_URI`, `ENTITY_BASE_URI` |
| Lineage & owners | `MARQUEZ_URL`, `OWNERS_YAML_PATH` (`./owners.yaml`) |
| Identity | `CELINE_OIDC_*` (also read from `.env`); `DATASET_API_TOKEN` for the import |
| Policies | see [governance-security.md](governance-security.md#configuration) |
| Query | `QUERY_STATEMENT_TIMEOUT_MS` (5000) |
| Row filters | `ROW_FILTERS_MODULES`, `ROW_FILTERS_CACHE_TTL` (300), `ROW_FILTERS_CACHE_MAXSIZE` (10000), `REC_REGISTRY_URL` |
| Dataspace | `EDR_ENABLED`, `CONNECTOR_INTERNAL_URL`, `CONNECTOR_INTERNAL_URLS`; `DPS_*` in [dps-data-plane.md](dps-data-plane.md) |
| Conformance | `CONFORMANCE_ENABLED` (false), `CONFORMANCE_SAMPLE_LIMIT`, `CONFORMANCE_MAX_SAMPLE` |

---

## Troubleshooting Guide

### “Dataset not found”
- dataset_id missing from catalogue DB
- import filters excluded it
- import failed validation
- its PostgreSQL table does not exist in `DATASETS_DATABASE_URL` (skipped at import)
- cleanup removed it due to missing physical table
- the catalogue hides it: `expose` is false or `access_level` is `secret`
  (`/query` answers `403` *"Dataset not available"* for an unexposed dataset)

### “Forbidden (403)”
- JWT missing groups/scopes required by the policy
- service account without the `dataset.query` (internal) or `dataset.admin`
  (restricted) scope
- access_level is `restricted` and policy denies
- dataspace path: *"Dataset not offered in the dataspace"* (no `dataspace.expose`),
  *"Refused by ds: …"*, or a row filter handler this instance cannot enforce
- dataspace path, `401` *"This data plane does not serve the provider…"*: the
  provider is missing from `CONNECTOR_INTERNAL_URLS`; `503`: no
  `CONNECTOR_INTERNAL_URL` configured

### “Import refused (401/403)”
- no token: set `--token`/`DATASET_API_TOKEN`, or `CELINE_OIDC_CLIENT_ID`/`_SECRET`
- the token lacks the `dataset.admin` scope (or the user the `platform-admin` realm role)

### “SQL rejected”
- non-SELECT statement
- unknown function — not in the allowlist; see
  [query-engine.md](query-engine.md#function-and-expression-allowlist) before adding one
- references non-catalogued table (*"Query references unknown datasets: …"*)
- multiple statements detected

### “Schema endpoint fails”
- physical table missing or renamed
- reflection permissions insufficient
- unsupported column type mapping

### “Lineage export is empty”
- lineage backend not receiving OpenLineage events
- wrong `--ns` filter
- wrong `--marquez-url` / `MARQUEZ_URL`

---

## Operational Checks (Recommended)

- periodic import dry-run in CI
- cleanup runs on every import; watch the server log's removed count
- dashboards for:
  - authorization denies
  - validation rejects
  - query latency
  - lineage ingestion lag
