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

Safe means pure and read-only: the construct reaches nothing beyond the rows the
query already reads, and costs no more than an aggregate, which the statement
timeout bounds. On that basis the parser admits:

- the functions in `ALLOWED_FUNCTIONS` (`parser.py`), whether sqlglot parses them
  as a typed node (`COALESCE`, `FLOOR`, `EXTRACT`, …) or as an anonymous call. Any
  of a function's SQL names counts, so `IFNULL` is `COALESCE`
- `GREATEST`, `LEAST`, `NULLIF`
- `CASE` expressions
- window functions (`… OVER (…)`) and the ranking functions `ROW_NUMBER`, `RANK`,
  `DENSE_RANK`. The function inside a window is still checked on its own
- `PERCENTILE_CONT` / `PERCENTILE_DISC` with `WITHIN GROUP`, and `BOOL_OR` /
  `BOOL_AND`
- a `VALUES` list, typically as a CTE of constant rows

Hash and crypto functions (`MD5`, `SHA256`, …) and string concatenation (`||`)
are not admitted. No read path needs them, so derive such values in the caller.

A clause below marked **planned** describes a widening decided but not yet in
the code: the parser still rejects it. The change that lands it adds its tests
and removes the status line.

#### QE-01 — `ST_AsGeoJSON` and `ST_Simplify` are admitted

`st_asgeojson` and `st_simplify` join `ALLOWED_FUNCTIONS`, so a caller can ask
for a shape as GeoJSON at a bounded size in one statement:
`ST_AsGeoJSON(ST_Simplify(geometry, <tolerance>))`. The first reader is the
Digital Twin's boundary-shape fetcher, which draws a community's reference
areas on a map.

Both pass the test above. They read only the geometry the query already
selects: a raw `geometry` column is already answered as GeoJSON, converted after
execution, one extra database round trip per row and never simplified. Neither
reaches beyond the row. `ST_Simplify` is the reason to admit the pair: a
published reference boundary is a detailed polygon, and
simplifying in the statement bounds the payload before it leaves the database.
Its cost on a large multipolygon is CPU, which the statement timeout and
`MAX_LIMIT` bound like any aggregate. What a hostile caller gains is a
serialisation of data they could already read.
Admitting the call admits nothing about its arguments: a function nested inside
either one is checked on its own, and the other PostGIS serialisers and
simplifiers (`ST_AsText`, `ST_AsBinary`, `ST_SimplifyPreserveTopology`, …) are
still refused until a reader needs them.
Tests: `tests/api/dataset_query/sql_parser/test_accept_geojson.py` (each
function, its argument forms, nested as above; neighbours and server-reaching
functions still refused, alone and as arguments),
`tests/api/dataset_query/sql_parser/test_security_resource_abuse.py`
(`ST_Simplify` over a whole table parses; nesting without end is refused),
`tests/routes/test_boundary_query.py` (a point-in-shape lookup and a simplified
GeoJSON shape executed on PostGIS through `POST /query`; a point on the edge two
shapes share is covered by both under `ST_Intersects`, and with `limit` 1 the
statement's `ORDER BY cod_ac` answers the lowest id).

#### QE-02 — A unary minus is admitted on a numeric literal, and on nothing else

`-1`, `-0.5` and `-1e3` parse, so a coordinate west of Greenwich or south of the
equator can be written where it is used: `ST_Point(-0.05, 0.05)`. Before, every
unary minus was an unsupported `Neg` and a caller had to write `0 - 0.05`.

The widening is `exp.Neg` over a numeric `exp.Literal`, checked where the node is
met. A negated number is a constant: it reads nothing and costs nothing. A minus
over anything else is still refused with *"Unary minus is allowed only on a
numeric literal"*: a column (`-value`, also inside an admitted call such as
`ST_Point(-lon, lat)`), a parenthesised expression or literal (`-(value + 1)`,
`-(1)`), a subquery, a function call, a string (`-'1'`), a cast (`-CAST(1 AS INT)`,
and `-1::int`, which sqlglot parses as a minus over a cast), and a double
negation (`- -1`). None of those is dangerous in itself — each operand is still
checked on its own — but none is a constant number and no reader needs one, so
they stay outside the allowlist until one does. Subtraction is a different node
and unaffected: `value - -1` parses, the right operand being a negated literal.
`--1` is not a double negation at all; it opens a comment and is refused as one.
Tests: `tests/api/dataset_query/sql_parser/test_accept_negative_literals.py`
(accepted forms, including inside `ST_Point`; every refused form above),
`tests/routes/test_boundary_query.py` (negative coordinates executed on PostGIS
through `POST /query`).

### Row filters
A dataset's governance can declare row filters (`rowFilters`). They are applied to
the validated AST after physical table names are substituted, and the query is then
rendered once, as PostgreSQL. The SQL is never re-parsed from text in between: a
text round trip changes the dialect (`INTERVAL '30 minutes'` becomes
`INTERVAL '30' MINUTES`) and splits `schema.table`, so a filter keyed on the
physical table would stop matching and be dropped.

On a request arriving through the dataspace the filter comes from ds's decision
rather than from governance here, and carries the consenting subjects with it —
see [dataspace-row-filters.md](dataspace-row-filters.md).

### Logging

#### QE-03 — The query log carries a statement's shape, never its literals

A statement's literals are the caller's data, not its structure: a boundary
lookup's point is a supply address's coordinates, and the `IN` list a
`rec_registry` row filter adds names a member's sensor ids. Wherever the engine
logs a statement — the raw SQL on arrival, the SQL after table mapping and after
row filters, the statement a failed execution ran — it logs its **shape**: every
literal replaced by `?`, comments dropped, rendered as PostgreSQL
(`log_safety.sql_shape`). Text that cannot be parsed is withheld whole rather
than logged raw. No hash of the statement is logged either: coordinates and ids
are low-entropy inside a known shape, and a hash could be reversed by
enumerating candidates.

The same holds for the parser's own warnings. A syntax error is logged by its
position (description, line, column) without the context sqlglot quotes from the
statement, and a refused or tolerated tautology by its shape. Any other parser
failure (sqlglot's tokenizer error on an unterminated literal, say, whose text
quotes the statement) is logged by its type only, without message or traceback;
so is a failure to apply row filters. The `400` the
caller receives still quotes their own statement back: that goes to the caller,
not to a log.

On the dataspace path the SQL after row filters stays withheld entirely (the
predicate names the consenting subjects). The driver's own error message is
still logged at `WARNING` so a failure leaves a trace; PostgreSQL may quote a
malformed value in it (*invalid input syntax …*), never the statement. An
error that is not the driver's (a SQLAlchemy `StatementError`, say) is logged by
its type and the statement's shape, without its message or a traceback: both
repeat the statement and its bound parameters.
Tests: `tests/api/dataset_query/test_log_safety.py` (numbers, negative numbers,
strings, comments and the executor's wrapper statement masked; unparseable text
withheld; a tokenizer error in the parser logs no literal and no traceback),
`tests/routes/test_boundary_query.py` (a coordinate literal reaches no
log at any level for an executed, a failed, an unparseable and a refused
statement, nor for a count or data query failing with a non-driver error, while
the shape still does).

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

The row cap is the request's `limit`: a `LIMIT` or `OFFSET` in the top-level
statement is refused. The engine wraps the statement as
`SELECT * FROM (<statement>) AS q [ORDER BY …] LIMIT :limit OFFSET :offset`, so an
`ORDER BY` in the statement decides which rows the page holds — the Digital
Twin's boundary lookup relies on `ORDER BY cod_ac` with `limit` 1 to answer the
lowest id of several covering shapes. `tests/routes/test_boundary_query.py` pins
it (ascending and descending answer different rows).

#### QE-04 — The statement's top-level `ORDER BY` is carried onto the page query

The SQL standard does not promise that a subquery's order survives the outer
`SELECT`, and a page cut before ordering is a different page. So the engine
repeats the statement's top-level `ORDER BY` on the outer query, each key
rewritten to name a column of `q`, with its direction and `NULLS` placement as
the statement renders them. The statement keeps its own `ORDER BY` inside the
subquery. Only the top level: an `ORDER BY` in a CTE, a subquery or a window is
the statement's business. The count query is not ordered.

The outer query sees only `q`'s output columns, so a key is rewritten only where
it provably names one of them:

- a bare column that names an output column (an alias or a selected column) —
  as PostgreSQL itself reads a bare name in `ORDER BY`, so in
  `SELECT a AS b, b AS a … ORDER BY a` it is the output `a`;
- a key written exactly as a selected expression: `ORDER BY t.ts` beside
  `SELECT t.ts`, `ORDER BY SUM(value)` beside `SELECT SUM(value) AS total`;
- a position (`ORDER BY 2`) pointing at a named output column;
- with a lone `SELECT *` over one source and no join, any column reference: `q`
  then carries that source's columns under their own names.

If any key is none of these — a column or expression that is not selected, an
unnamed output (`SELECT lower(name) … ORDER BY lower(name)`), an output name
selected twice (`SELECT t.a, u.a`), a star beside other projections or over a
join (where `q` may hold one name twice and naming it would be ambiguous) — no
outer `ORDER BY` is added at all, never a partial one, which would order by a
prefix of the keys. The page query is then what it was before this clause and
PostgreSQL's plain outer `SELECT` keeps the subquery's order, as it does in
practice. No statement that ran before fails because of this clause.

Note that the statement is parsed in sqlglot's default dialect and rendered as
PostgreSQL, so an ascending key without `NULLS` is rendered `NULLS FIRST` (and a
descending one `NULLS LAST`) on both the inner and the outer query; the two
agree.
Code: `src/celine/dataset/api/dataset_query/pagination.py`.
Tests: `tests/api/dataset_query/test_outer_order_by.py` (each rewritten form,
both directions, `NULLS` and identifier case kept; each form that adds no outer
`ORDER BY`; the page query's shape), `tests/routes/test_paginated_order.py`
(executed on PostgreSQL through `POST /query`: pages in descending, alias,
position, star, aggregate and join-column order with the executed page query
carrying `ORDER BY q.…`; a column not selected and a star over a join whose
tables share column names still run, with no outer `ORDER BY`).

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

