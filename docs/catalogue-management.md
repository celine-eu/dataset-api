# Catalogue Management

This document covers how datasets are defined, imported, reconciled, and cleaned up.

---

## Catalogue as Code

Catalogue state is defined in YAML and treated like application config:
- version controlled
- reviewed
- validated on import

The API database stores the *result* of the import, but YAML remains the source of truth.

---

## YAML Structure

Two formats are involved.

### Source: `governance.yaml`

Pipelines declare governance next to the data, as `defaults` plus rules matched by
dataset glob under `sources`, with `governance.<app>.yaml` overlays. It is parsed by
`celine.governance` (celine-utils):

```yaml
defaults:
  access_level: internal
  classification: green

sources:
  datasets.ds_dev_gold.example:
    title: Example dataset
    description: Curated indicator for X.
    expose: true
    access_level: open
    tags: [gold, example]
    documentation_url: https://example.org/docs
    dataspace:
      expose: true
```

Rule fields include `title`, `description`, `expose`, `access_level`
(`open|internal|restricted|secret`), `classification` (`green|yellow|red|pii`),
`tags`, `ownership`, `license`, `documentation_url`, `source_system`,
`retention_days`, `row_filters`, `dcat`, `ontology` and `dataspace`.

### Import format

`dataset-cli export governance` turns governance files into the format
`dataset-cli import catalogue` reads, a top-level `datasets` map keyed by
`dataset_id` (a file without it is skipped with a warning):

```yaml
datasets:
  datasets.ds_dev_gold.example:
    title: Example dataset
    description: Curated indicator for X.
    backend_type: postgres            # postgres|s3|fs|quantumleap|context_broker
    backend_config: {table: ds_dev_gold.example}
    expose: true                      # catalogue gate (default false)
    dataspace_expose: true            # offered to the dataspace (default false)
    access_level: open                # open|internal|restricted|secret
    tags: {keywords: [gold, example, "classification:green"]}
    lineage: {name: datasets.ds_dev_gold.example, facets: {governance: {}}}
    landing_page: https://example.org/docs   # from documentation_url
```

`title` and `backend_type` are required. `access_level`, when given, must be one of
`open`, `internal`, `restricted`, `secret` (any case); an entry that states none is
stored as `internal`. `classification` becomes the keyword
`classification:<value>`; `source_system` and `retention_days` live only in the
governance facet. Only `postgres` datasets are queryable; the other backend types
are catalogue entries only. See [cli-operations.md](cli-operations.md) for the
exporters (`governance`, `postgres`, `openlineage`).

---

## Import Semantics

`POST /admin/catalogue` (called by `dataset-cli import catalogue`) requires the
`dataset.admin` scope or the `platform-admin` realm role, and upserts on `dataset_id`:

- missing entries are created
- existing entries have every field overwritten
- entries absent from the input are left in place; removal happens only through
  the stale-entry cleanup below

The response is `{created, updated}`.

### Physical validation
For `postgres` datasets the table is checked by reflection **before** create or
update: `backend_config.table`, or when the entry states none, the table its id
names (`datasets.<schema>.<table>` → `<schema>.<table>`) — the same table the query
path would use. A dataset whose table does not exist is skipped (logged server-side, not
reported in the response). Column schema is not stored: `GET
/catalogue/{id}/schema` reflects it on request.

Every row filter's `args.column` must be a column of that table: an import naming a
column the table lacks is refused with `422`, listing each such filter, and changes
nothing — no create, update or cleanup ([GS-09](governance-security.md)). A filter's
`binds` must agree with its handler (GS-08).

---

## Selection & Filters

`dataset-cli import catalogue` resolves the selection before sending anything:

- `--input/-i` — file or glob, repeatable; inputs are sorted and de-duplicated
- `--ns` — namespace filter on `lineage.namespace` (default `default`); supports
  `*`, `+ns`, `-ns`
- `--datasets` — `dataset_id` globs, repeatable: `+pattern` (or bare) includes,
  `-pattern` excludes; includes are applied first, then excludes

Example:
- include only gold: `--datasets '+datasets.*.gold.*'`
- exclude one: `--datasets '-datasets.*.gold.experimental_*'`

A `dataset_id` declared differently in two inputs is refused unless
`--allow-conflicts`, which keeps the declaration from the last file in sorted order.
Invalid entries are skipped with a warning; `--strict` fails on the first one.

---

## Dry Run

`--dry-run` prints the dataset_ids selected after `--ns` and `--datasets` and exits.
It does not validate entries and does not contact the API, so it cannot say what
would be created, updated or cleaned up.

---

## Cleanup of Stale Entries

Every import ends with a cleanup, in the same transaction, over the **whole**
catalogue (not just the selection):

1. only `postgres` entries are checked, against the same table the query path uses
   (stated or derived from the id)
2. an entry whose table no longer exists is deleted
3. tables validated earlier in this import are skipped

There is no protection or pinning. The number removed is logged server-side.

This addresses real-world drift when pipelines drop or rename tables.

---

## DCAT Exposure Rules

Every catalogue surface (`GET /catalogue`, `GET /catalogue/{id}`, `POST
/catalogue/search`, `/schema`, `/vocabulary`, the HTML pages) lists each entry with
`expose: true`, except `access_level: secret`. Metadata of `internal` and
`restricted` datasets is public; access to their rows is governed separately at
`/query`. `dataspace_expose` does not affect the catalogue: it only gates the
dataspace path.

---

## Operational Tips

- keep titles/descriptions in YAML (reviewable)
- use the governance facet and the namespace for policy scoping
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

- a **gate** — `POST /admin/catalogue` does not check conformance and does not refuse an
  import over it;
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
policy checks, same row filters. The report quotes row values back in its violation
messages, so anything weaker would be a row-level leak wearing a metadata endpoint's
clothes. 404 when the dataset is not exposed or declares no mapping, matching
`/vocabulary`.

The request body is `{limit, profile_version, context}`, all optional. `limit`
defaults to `CONFORMANCE_SAMPLE_LIMIT` (100) and is capped at
`CONFORMANCE_MAX_SAMPLE` (1000). An unknown `profile_version` is a 400 naming the
available versions.

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
