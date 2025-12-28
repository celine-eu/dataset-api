# Dataset API

## Overview
The Dataset API provides a **secure, lineage-aware, metadata-rich interface** to heterogeneous datasets (PostgreSQL, object storage, filesystem).  
It exposes a **DCAT-AP 3.0.0–compatible catalogue**, a **governed SQL query interface**, and **OpenLineage-integrated provenance**, designed to support Digital Twins and analytical applications.

This README gives a **high-level orientation**.  
Detailed concepts and workflows are documented in `docs/*.md` (see links below).

---

## Core Capabilities

- **Dataset catalogue (DCAT-AP 3.0.0)**  
  Public catalogue endpoint exposing datasets and distributions.

- **Governed query API**  
  SQL `SELECT` queries over exposed datasets with:
  - strict SQL validation
  - server-side pagination & limits
  - dataset-level access control (auth + OPA)

- **Strong governance & disclosure model**
  - `open`, `internal`, `restricted` access levels
  - JWT-based authentication
  - OPA policy evaluation

- **Lineage & provenance**
  - OpenLineage ingestion (Marquez)
  - Namespace-based dataset grouping
  - Provenance surfaced in catalogue & metadata

- **Schema & metadata introspection**
  - JSON Schema (2020-12) generated from physical tables
  - Column-level metadata for UI and clients

- **CLI-driven lifecycle**
  - Export lineage → YAML
  - Validate catalogue definitions
  - Import & reconcile catalogue state
  - Automatic cleanup of stale datasets

---

## API Surface (High-Level)

| Area | Description |
|-----|-------------|
| `/catalogue` | DCAT-AP catalogue (exposed datasets only) |
| `/catalogue/{dataset_id}/schema` | JSON Schema of dataset |
| `/query` | Governed SQL query endpoint |
| `/admin/catalogue` | Catalogue import (CLI-only) |
| `/health` | Health check |

Detailed endpoint semantics are described in the docs.

---

## CLI Overview

The CLI is the **primary control plane** for the Dataset API.

```bash
dataset-cli --help
```

Main commands:
- `export openlineage` – extract lineage from Marquez
- `import catalogue` – validate & import dataset catalogue
- `validate catalogue` – schema validation only
- `ontology` – ontology fetch, analysis, tree generation

---

## Documentation

Additional documentation available

- [Architecture overview](docs/architecture.md)
- [Catalogue Management](docs/catalogue-management.md)
- [CLI operations](docs/cli-operations.md)
- [Governance and security](docs/governance-security.md)
- [Query engine](docs/query-engine.md)

---

## Development & Contribution

- Python ≥ 3.11
- Async SQLAlchemy
- Pydantic v2
- FastAPI + httpx
- sqlglot-based SQL validation

Before opening a PR:
- validate all YAML definitions
- add tests for new API behavior
- include migrations for schema changes
- keep docs in sync with API behavior

---

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
