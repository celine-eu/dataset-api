# Dataset API

## Overview
The Dataset API provides a unified, lineage-aware, metadata-rich interface to datasets stored across heterogeneous backends (PostgreSQL, S3, filesystem). It exposes a DCAT-AP 3.0.0–compatible catalogue, OpenLineage-enriched metadata, and a controlled dataset exposure policy.

## Features
- DCAT-AP 3.0.0 catalogue (`/catalogue`)
- Detailed dataset metadata (`/dataset/<id>/metadata`)
- Dataset schema (`/dataset/<id>/schema`)
- Query API using SQL-like syntax (`/dataset/<id>/query`)
- Strong governance through controlled dataset exposure (`expose: true/false`)
- OpenLineage integration and provenance tracking
- YAML-based catalogue import/export
- CLI tools for validation, extraction, import, and migration
- Backend-agnostic support for PostgreSQL, S3, and filesystem data

## Architecture Summary
- **Catalogue layer** stored in PostgreSQL schema (`settings.catalogue_schema`)
- **DatasetEntry** model capturing backend config, tags, lineage, licensing, and metadata
- **DCAT builders** generate catalogue and dataset JSON-LD outputs
- **OpenLineage extractor** fetches metadata via Marquez and exports YAML
- **Importer CLI** loads YAML → validates via Pydantic → imports into catalogue DB
- **Exposure semantics** ensure only selected datasets are queryable
- **Alembic migrations** support async engines and schema scoping

## CLI Commands
### Export OpenLineage to YAML
```
dataset export openlineage --ns prod -o data/ --expose
```

### Import catalogue from YAML
```
dataset import catalogue -i data/*.yaml --api-url http://localhost:8000
```

### Validate catalogue file(s)
```
dataset validate catalogue -i data/*.yaml --strict
```

### Alembic migrations
```
uv run alembic upgrade head
uv run alembic revision --autogenerate -m "update"
```

## Backends
- **postgres** – SQL tables
- **s3** – raw objects with optional public URL
- **fs** – direct file-based datasets

## Lineage Support
The system stores structured lineage metadata from OpenLineage, including:
- namespace
- sourceName
- timestamps
- lifecycle state
- facets
- tags

Pydantic models allow flexible ingestion (`extra="allow"`).

## DCAT-AP Compliance
Each dataset includes:
- identifiers, titles, descriptions
- keywords, themes
- publisher, rights holder, license
- language & spatial coverage
- distributions (API access and raw file access)
- provenance (`prov:wasDerivedFrom`) using lineage information

## Development
### Dump all Python source files into a single file:
A provided tool gathers all `dataset/**/*.py` into one bundle while ignoring `__pycache__`.

### Supported Python tooling:
- `uv` package runner
- Typer CLI
- Pydantic v2
- SQLAlchemy (async)
- Alembic (async migrations)
- httpx (async HTTP calls)

## License

Copyright >=2025 Spindox Labs

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.


## Contributing
Ensure:
- All YAML definitions validate via the CLI
- No API endpoint accepts unvalidated data
- Alembic migrations are generated for schema changes

PRs should include tests for:
- catalogue import
- DCAT output
- lineage extraction
- backend resolution

