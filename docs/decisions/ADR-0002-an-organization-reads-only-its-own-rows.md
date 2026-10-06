# ADR-0002 — an organization reads only its own rows, through a row filter; an unscoped `internal` dataset is closed to organizations

**Date:** 2026-10-05
**Status:** accepted

## Context

ADR-0001 kept the two levels apart but left one merge: an organization's `managers` or
`viewers` read every `internal` dataset, whichever organization they were held in. Row
filters narrowed per-member data, but an `internal` dataset without one — community
aggregates, grid topology — was readable by any member of any organization. dataset-api is no
longer guaranteed to sit behind the services, so "not exposed" is not a control.

A dataset has no owning organization: one table collects several communities' rows. Whose rows
a caller may read is therefore a property of the row, not of the dataset.

## Decision

- **Scope by row, with a row filter.** `organization_match` serves the rows whose column holds
  an alias of an organization where the caller holds a reading group; `org_type` limits it to
  organizations of one type. The policy keeps a coarse gate ("a reading group somewhere") and
  never decides which organization.
- **The column holds the organization alias by convention**, as rec-registry's community key
  already does. No alias → value mapping exists, so a column in another vocabulary narrows to
  nothing rather than to the wrong organization.
- **Reading groups are `admins`, `managers` and `viewers`**: an organization's `admins` reads
  what its `managers` read. `editors` grants no read.
- **Fail closed.** An `internal` dataset that declares no row filter is readable by the
  `platform-admin` role and by services only. Data meant for every member of every
  organization declares the `member_wide` row filter, which narrows nothing but records the
  decision in governance.
- **An organization filter says so: `binds: organization`** (celine-utils REQ-0010; GS-08).
  `row_filters` stays the one place a dataset declares how its rows are narrowed, and ds keeps
  reading it as a consent signal — but only for a filter binding a person. A new governance key
  was rejected: it would split one concern across two keys a reviewer must read together.
- **Services and the platform administrator are not narrowed** by `organization_match`, as by
  `rec_registry`. A delegated (dataspace) request is refused by it.

## Consequences

- Governance must declare a row filter on every `internal` dataset members read, **in the same
  deployment** as this rule; otherwise members lose those datasets.
- A per-community aggregate needs a column naming its community before it can be scoped; until
  then it is closed to organizations or explicitly `member_wide`.
- A service that reads on its own token and relays rows to a person (celine-grid → digital
  twin) is not narrowed here; it must scope by the entity it serves or forward the person's
  token.
- A token still naming an organization the person has left reads it until reissued. Plans are
  cached per organization set, never per `sub` alone, so a reissued token is answered afresh.
- It will be tempting to "fix" a member's 403 on an aggregate by removing the dataset's
  `internal` level or adding `member_wide`. Either reopens the cross-organization read this
  decision closes; add the community column instead.
