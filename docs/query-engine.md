# Query Engine

This document explains the governed SQL interface: request/response semantics, validation rules, dataset resolution, and performance guardrails.

---

## Endpoints (Conceptual)

Your exact route names may differ; the engine typically exposes:
- **POST query**: run a SQL query against a dataset
- **GET schema**: fetch JSON Schema for a dataset
- **GET metadata**: column and dataset metadata for UI/clients

The README should link to the concrete API reference.

---

## Query Request Model

Typical POST body:

```json
{
  "sql": "SELECT ...",
  "limit": 100,
  "offset": 0
}
```

Notes:
- `limit`/`offset` are applied server-side.
- the engine may override `limit` with a maximum.

---

## Response Model

A typical paginated response:

```json
{
  "items": [
    {"col_a": 1, "col_b": "x"},
    {"col_a": 2, "col_b": "y"}
  ],
  "limit": 100,
  "offset": 0,
  "count": 2
}
```
---

## SQL Validation Rules

### Statement restrictions
- single statement only
- `SELECT` only

### Table restrictions
- references must map to **catalogued datasets**
- if a query contains multiple table references, each must be resolvable

### Function and expression allowlist
- only allow safe scalar functions
- block functions that can access filesystem, network, or server internals

### Projection safety
- avoid `SELECT *` if you want strict contracts (optional)
- optionally enforce explicit column selection for restricted datasets

---

## Dataset Resolution

The engine enforces a separation between:
- **logical ids**: what the client references (dataset identifiers)
- **physical names**: actual table/view names

Resolution steps:
1. parse SQL and extract table identifiers
2. map identifiers to catalogue entries
3. substitute physical references into the execution query (or bind via prepared mapping)
4. reject if any identifier cannot be mapped

This prevents “escaping” to arbitrary tables.

---

## Pagination, Limits & Timeouts

The engine must guard the storage backend.

Recommended controls:
- hard max `limit` (e.g. 1k / 10k rows)
- max offset (to prevent deep scans) or encourage keyset pagination
- statement timeout
- max query complexity (joins, subqueries, regex-like operations)

Even for `open` datasets, resource controls must remain enforced.

---

## Join Policy

Joins can be permitted, but only within controlled boundaries:

- join only catalogued datasets
- join only within allowed namespaces (e.g. silver+gold)
- reject cartesian products
- optionally limit join count (e.g. <= 3 tables)

If you want safer defaults:
- forbid joins by default, allow per-dataset tags/policy

---

## Error Semantics (Examples)

### Validation error (400)
- invalid SQL
- forbidden keyword
- unknown dataset reference

### Authorization error (403)
- identity not permitted by OPA
- missing required scopes/roles/groups

### Not found (404)
- dataset id does not exist in catalogue

### Execution error (500/502)
- database error
- upstream dependency failure (OPA/lineage)

---

## Examples

### Query a dataset
```bash
curl -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT * FROM datasets.gold.example WHERE ts >= now() - interval \'1 day\'","limit":100,"offset":0}' \
  https://host/api/dataset/datasets.gold.example/query
```

### Paginate
```bash
curl -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT id, ts, value FROM datasets.gold.example ORDER BY ts DESC","limit":100,"offset":100}' \
  https://host/api/dataset/datasets.gold.example/query
```

---

## Performance Recommendations for Clients

- always filter by time windows where possible
- request only needed columns
- prefer indexed predicates
- avoid deep offsets; paginate with stable ordering

