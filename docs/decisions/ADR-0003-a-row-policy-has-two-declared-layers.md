# ADR-0003 — a row policy has two layers, person and organization, each declared; an undeclared one denies

**Date:** 2026-10-19
**Status:** accepted

## Context

A row filter narrowed a query, but a dataset that declared none was served whole: the
absence of a policy read as "every row". ADR-0002 closed that for organizations' readers
of `internal` datasets only. A service holding the scope, an `internal` dataset declaring
only a person filter (whose organization nobody stated), and any dataspace request on a
dataset with no consent filter were all still served every row of the table — a gap
nobody had decided, found only by reading the governance file.

Two questions are mixed in one list of filters: *which person's* rows (a consent, a
membership) and *which organization's* rows (the community, the grid operator). A dataset
can need either, both, or state that one of them narrows nothing; a single list cannot
tell "nothing narrows by organization, on purpose" from "nobody thought about it".

On the dataspace path ADR-0002 had `organization_match` refuse a delegated request, so
the organization layer could not be applied there at all: a dataset whose filters all
bound an organization was served whole through any agreement — every operator's rows to
every consumer.

## Decision

This record supersedes two points of ADR-0002, listed at the end of this section.


- **Two layers, each declared.** A filter's `binds` is its layer: `person` (the default)
  or `organization`. Every exposed dataset that is neither `open` nor `secret` declares
  both. A layer is evaluated as a whole; all its filters apply.
- **A layer can be stated without a column:** `allow_all` admits every row of the layer,
  `deny_all` none. Both require `binds`; the catalogue import refuses either without it
  (GS-08, GS-10). `deny_all` refuses the dataset with `403 row_policy_denied` rather than
  an empty page.
- **Deny by default.** A layer left undeclared on a non-`open` dataset follows
  `ROW_FILTERS_DEFAULT`: `deny` (the default) refuses with `403 row_policy_undeclared`,
  naming the layer; `allow` is report-only — the dataset is served whole, with a
  `WARNING` and an *allowed* audit record carrying the same reason, so a deployment can
  measure what `deny` would refuse before it denies.
- **After the platform-admin bypass.** The `platform-admin` realm role reads every row,
  `deny_all` included (ADR-0001 unchanged). A service holding the scope is subject to
  the same layers: its declared filters keep their service behaviour, an undeclared
  layer follows the default.
- **A declaration that cannot be read is refused, never read as none.** A malformed
  `rowFilters`, an item naming no handler, `args` that is not a mapping, a `binds`
  outside the two layers, or a handler this service lacks: `422` at import, `403
  row_filter_unenforceable` at query time (GS-11). `dataset-cli export governance` fails
  on the same findings and on a missing layer, before it writes anything.
- **`open` means any authenticated caller.** An undeclared layer on an `open` dataset
  admits every row; an anonymous caller gets `401` on every level (GS-12).
- **On a dataspace request the local declaration is evaluated, both layers** (GS-13).
  ds's filter, naming the consenting subjects, is the person layer; the organization
  layer is this service's own declaration, applied to the `organizations` in ds's
  verdict — the consumer's organizations, which ds establishes from the credential the
  consumer presented. `organization_match` reads that list **before** the service
  bypass; an empty or missing list is no rows. A delegation naming consenting subjects
  instead is still refused by it.

This supersedes, in ADR-0002:

- "An `internal` dataset that declares no row filter is readable by the `platform-admin`
  role and by services only" — under `deny` a service is refused too; it reads such a
  dataset only under `ROW_FILTERS_DEFAULT=allow`.
- "A delegated (dataspace) request is refused by [`organization_match`]" — it now
  filters by the verdict's `organizations`; only a delegation naming consenting subjects
  is refused.

The rest of ADR-0002 stands: scope by row, the column holds the organization alias,
`admins`/`managers`/`viewers` read, `binds: organization` says so, plans are cached per
organization set.

## Consequences

- **Governance must declare both layers on every exposed non-`open` dataset in the same
  deployment as `deny`**, or those datasets answer `403`. The export and
  `celine-governance validate` (celine-utils) fail on a missing layer, so the gap shows
  at development, not at a caller.
- **`allow_all` is now a decision written down.** It costs a line per layer, and a
  reviewer reads it as "this layer narrows nothing, on purpose" — which is the point.
- **Open data requires a login.** Anonymous callers lose `open` datasets.
- **An organization-bound dataset serves no rows through an agreement until ds sends
  `organizations`.** The field is optional on the wire so this service can be deployed
  first; the order of the two deployments decides when those datasets reappear.
- **This service trusts ds's `organizations`.** Whether the consumer really is that
  organization is decided by ds from the presented credential, not here.
- **The plan cache keys on the consumer's organizations too**; two agreements for two
  organizations never share a plan.
- It will be tempting to clear a `403 row_policy_undeclared` by running
  `ROW_FILTERS_DEFAULT=allow` for good, or by declaring `allow_all` on both layers
  without looking. Either serves every row again — the first silently, for every
  dataset. `allow` is a measurement step; declare the layer that matches the data.
