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


---

## Ontology conformance checking

`POST /catalogue/{dataset_id}/conformance` maps a bounded sample of a dataset's rows
through its declared mapping and validates the resulting RDF graph against the SHACL
shapes of the ontology version that mapping pins.

**Off by default.** Set `CONFORMANCE_ENABLED=true` and install the extra:

```sh
uv pip install 'dataset[conformance]'   # celine-ontologies[mapper] — adds pyshacl
```

When the setting is on and the extra is missing, the service fails at startup rather
than at the first request. When the setting is off the route is not registered at all,
so it does not appear in the OpenAPI document — "not deployed" rather than "deployed
and refusing".

### What it is

An audit, on request. It is deliberately **not**:

- a **gate** — `POST /admin/catalogue` does not validate and does not refuse an import;
- a **filter** — no row is ever dropped from `/query` because of a violation. A result
  that depended on shape conformance would be indistinguishable, to the consumer, from
  a small one;
- **stored** — there is no "last checked" column and no timestamp on the catalogue
  entry. A stored conformance claim is a claim with an expiry that nobody watches.

### What a green result asserts

That the graph produced by applying this mapping to these rows satisfies the shapes.
**Nothing about meaning.** A spec mapping `kwh` onto the wrong observed property, or
onto the right one with the wrong unit, produces a perfectly conformant graph of wrong
statements. SHACL closes the structural half of the promise `dct:conformsTo` makes; the
semantic half remains the producer's assertion and no validator recovers it.

Note also that the CELINE SHACL profile constrains CELINE classes. A mapping whose
`target_type` is a class the profile carries no shape for — `sosa:Observation` today —
conforms because there is nothing to violate. The report says which version ran; it
does not claim the version had anything to say.

### Versions

The mapping spec pins the ontology version (`profile: {name, version}`), and the check
runs against that version, not against the newest one installed. A newer ontology
release must not decide retroactively that a dataset stopped conforming. The library
packages a window of versions (v0.8–v0.10 at the time of writing); a pin outside it
fails loudly and names what is available.

`profile_version` in the request body overrides the pin — for deciding an upgrade
before making it. The report then reports `profile_pinned: false`, because a what-if is
not the dataset's own claim.

### Access

Authorised exactly like `/query`, and through the same executor: same governance and
OPA checks, same row filters. The report quotes row values back in its violation
messages, so anything weaker would be a row-level leak wearing a metadata endpoint's
clothes. 404 when the dataset is not exposed or declares no mapping, matching
`/vocabulary`.

### Reading the response

```jsonc
{
  "dataset_id": "datasets.ds_dev_gold.kpi_definitions",
  "conforms": false,
  "sample_size": 100,
  "violations": ["..."],          // capped; `violations_truncated` says when
  "violations_truncated": false,
  "profile_name": "celine",
  "profile_version": "v0.10",
  "profile_pinned": true,
  "checked_at": "2026-08-14T07:51:44+00:00"
}
```

`conforms: false` comes back as **200**. A 4xx would conflate "the check failed to run"
with "the check ran and found violations", and the second is the endpoint working. A
check that could not run at all — shapes unresolvable, stored mapping no longer a valid
spec — is 503, because neither is a finding about the data.

`sample_size: 0` conforms, over an empty graph. That is why the field is in the report.
