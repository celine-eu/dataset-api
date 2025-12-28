# Catalogue Management

This document covers how datasets are defined, imported, reconciled, and cleaned up.

---

## Catalogue as Code

Catalogue state is defined in YAML and treated like application config:
- version controlled
- reviewed
- validated before import

The API database stores the *result* of the import, but YAML remains the source of truth.

---

## YAML Structure (Recommended)

A compact pattern:

```yaml
defaults:
  access_level: internal
  classification: green
  tags: []
  ownership: []
  retention_days: 365

sources:
  datasets.gold.example:
    access_level: external
    title: Example dataset
    description: Curated indicator for X.
    tags: [gold, example]
    documentation_url: https://...
    source_system: Example producer
```

Key fields you typically need:
- `dataset_id`
- `title`, `description`
- `access_level`
- `tags`, `classification`
- `source_system`, `documentation_url`
- optional ownership, license, retention hints

---

## Import Semantics

Imports are **reconciling**:

- create missing dataset entries
- update metadata on existing entries
- optionally delete or disable entries that are no longer present in YAML

This makes environments reproducible.

### Create vs Update
- if dataset_id exists → update metadata and refresh schema references
- if not → create entry, then validate physical mapping

---

## Selection & Filters

To manage large catalogues, imports support dataset selection filters.

Recommended semantics:
- `+pattern` includes (glob)
- `-pattern` excludes (glob)

Example:
- include only gold: `+datasets.*.gold.*`
- exclude one: `-datasets.gold.experimental_*`

The import command should resolve the final selection list *before* applying changes.

---

## Dry Run

`--dry-run` should:
- print the selected dataset_ids after filters
- show what would be created/updated/deleted
- perform no writes

This is essential for safe ops.

---

## Physical Validation & Reflection

During import (or post-import), the system should:
- verify the referenced physical table/view exists
- reflect schema to build columns/types
- optionally generate JSON Schema artifacts

If reflection fails:
- mark dataset as invalid (or reject import for that dataset)
- surface actionable error

---

## Cleanup of Stale Entries

A robust import process includes a cleanup phase.

**Goal:** remove catalogue entries whose physical tables no longer exist.

Recommended algorithm:
1. list catalogue entries
2. for each, check existence (reflection / information_schema)
3. if missing, delete entry unless protected
4. support skip-list for datasets just imported or explicitly pinned

This addresses real-world drift when pipelines drop or rename tables.

---

## DCAT Exposure Rules

The API should only expose datasets in the public catalogue that meet both:
- configured exposure rules (e.g. namespace in {gold})
- access level compatible with anonymous viewing (typically `open`)

You can still keep internal/restricted datasets in the internal catalogue, but hide from public endpoints.

---

## Operational Tips

- keep titles/descriptions in YAML (reviewable)
- use tags to express domain/tenant scoping for OPA
- keep dataset_ids stable; rename through controlled migration
- do not overload YAML with physical implementation details unless necessary

