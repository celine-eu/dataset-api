# dataset-api

Permissioned dataset interface API. Exposes governed SQL query access to PostgreSQL-backed datasets, a DCAT-AP 3.0 catalogue, and a CLI for governance/lineage import-export workflows.

## Architecture

Two-database design:
- **Catalogue DB** — stores `datasets_entries` table (metadata, access levels, DCAT fields). Managed by Alembic migrations. Schema name controlled by `CATALOGUE_SCHEMA` (default `dataset_api`).
- **Datasets DB** — holds the actual data tables. No ORM models; tables are reflected at runtime via SQLAlchemy `MetaData.reflect()` with PostGIS geometry support (`db/reflection.py`).

Entry point: `src/celine/dataset/main.py` → `create_app()` factory. Routers are discovered automatically from `routes/*.py` files exporting a `router` variable.

## Query engine

`POST /query` accepts `{"sql": "...", "limit": N, "offset": N}`. The pipeline:

1. **Parse** (`api/dataset_query/parser.py`) — sqlglot AST validation against an allowlist of expressions and functions. Rejects DML, statement stacking, comments, disallowed functions. The parser uses a Tuple/IN allowlist and depth checks.
2. **Governance** — resolves referenced tables to `DatasetEntry` records, enforces access level via OPA policies evaluated in-process (`policies/celine/dataset.rego`).
3. **Row filters** (`api/dataset_query/row_filters/`) — pluggable row-level access control. Handlers registered via `ROW_FILTERS_MODULES` setting. Built-in handlers: `direct_user_match`, `rec_registry`, `http_in_list`, `table_pointer`.
4. **Execute** (`api/dataset_query/executor.py`) — runs the rewritten SQL with `statement_timeout` guard. Limits clamped to `MAX_LIMIT=10000`.

SQL parser allowlist: when adding support for new SQL constructs, add the sqlglot `exp.*` type to `ALLOWED_EXPRESSIONS` in `parser.py`. For new SQL functions, add the lowercase name to `ALLOWED_FUNCTIONS`.

## Security model

Three layers:
1. **Authentication** — JWT via `celine-sdk` OIDC. Dependencies: `get_current_user()` (required) / `get_optional_user()` (optional).
2. **Access levels** — per-dataset: `open`, `internal`, `restricted`, `secret`. Stored in `DatasetEntry.access_level`.
3. **OPA policy** — `policies/celine/dataset.rego` evaluates subject type (user/service/anonymous), roles, groups, scopes against access level. Admin scope (`X.admin`) matches all `X.*` required scopes.

Optional EDC dataspace integration when `EDR_ENABLED=true` — checks `Edc-Contract-Agreement-Id` / `Edc-Bpn` headers.

## CLI

Installed as `dataset-cli` (pyproject.toml `[project.scripts]`).

Key commands (see `taskfile.yaml`):
- `task cli:export:governance` — extract governance metadata from `governance.yaml` files in pipelines repos into `data/governance/`
- `task cli:import:governance` — import extracted YAML into the API catalogue
- `task cli:export:openlineage` / `task cli:import:openlineage` — same for Marquez lineage data

## Development

```bash
task setup              # uv sync
task run                # uvicorn on :8001 with reload
task debug              # same with debugpy on :48001
task test               # pytest (append -- -k "name" to filter)
task alembic:migrate    # alembic upgrade head
```

Requires Python >= 3.12, `uv` as package manager, hatchling for builds.

Local PostgreSQL expected at `:15432` (credentials `postgres:securepassword123`). Settings use Pydantic Settings v2 with `.env` file support; defaults work for local dev. Cross-service refs use `host.docker.internal`.

## Key conventions

- Source layout: `src/celine/dataset/` (namespace package for cross-celine compatibility)
- Settings: single `Settings()` instance in `core/config.py`, env vars override defaults
- Tests: `pytest-asyncio` with dependency override fixtures in `tests/conftest.py`. SQL parser has dedicated security test suites (injection, fuzzing, jailbreak) under `tests/api/dataset_query/sql_parser/`
- DCAT catalogue: `api/catalogue/dcat_formatter.py` produces JSON-LD. Publisher metadata enriched from `owners.yaml`
- Versioning: `python-semantic-release`, `task release`

## File reference

| Path | Purpose |
|---|---|
| `core/config.py` | All settings and env vars |
| `api/dataset_query/parser.py` | SQL validation allowlist |
| `api/dataset_query/executor.py` | Query execution, limits, timeout |
| `api/dataset_query/row_filters/` | Row-level filter framework |
| `security/governance.py` | Access enforcement entry point |
| `security/auth.py` | JWT validation |
| `policies/celine/dataset.rego` | OPA access policy |
| `db/reflection.py` | Dynamic table introspection |
| `db/models/dataset_entry.py` | Catalogue ORM model |
| `api/catalogue/dcat_formatter.py` | DCAT-AP 3.0 serialization |
| `cli/` | CLI commands (export/import) |
| `routes/` | FastAPI routers (auto-discovered) |
