# ADR-0001 — a platform administrator is the realm role `platform-admin`; realm groups grant nothing

**Date:** 2026-10-03
**Status:** accepted

## Context

The API decided "platform administrator" from a flat list of group names that merged the
realm `groups` claim with every organization's `organization.<alias>.groups`. Keycloak gives a
realm group and an organization's group of the same name the same path (`/admins`), so an
organization's own `admins` read as the platform's: its holder skipped every row filter, read
`restricted` datasets and could write the catalogue. Realm `managers` and `viewers` also read
`internal` datasets on every organization's behalf.

The platform settled the authority model in one realm for every service (`celine-policies`
ADR-0012, requester 2026-10-03): exactly two levels, the realm role `platform-admin` and an
organization's own groups, valid only inside that organization. Realm groups and the realm
roles `admin`, `manager`, `editor`, `viewer` are removed from the realm.

## Decision

- **The platform level is the realm role `platform-admin`, read from `realm_access.roles`.**
  Its holder skips row filters, reads `restricted` datasets and may write the catalogue
  (besides a service holding `dataset.admin`). Nothing else is a platform grant: not a realm
  group, not a retired realm role, not a client role, not an organization group named
  `platform-admin`.
- **The organization level is read only from `organization.<alias>.groups`.** `managers` and
  `viewers` read `internal` datasets, with row filters applied; an organization's `admins` and
  `editors` grant nothing here.
- **The top-level `groups` claim is not read.** A realm group still present in a token grants
  nothing.
- **Policy input keeps the levels apart:** realm roles in `input.subject.roles`, organization
  groups in `input.subject.groups`. `dataset.rego` tests `"platform-admin" in
  input.subject.roles` and never reads the platform level from `groups` or `claims`.
- No compatibility path for the old realm groups; the change ships with the realm convergence.

## Consequences

- **Operators who were in realm `/admins` lose access** until the deployment lists them as
  `platform-admin` holders. The API fails closed rather than guessing.
- **The organization is not yet matched against the dataset.** A `viewers` membership in any
  organization reads every `internal` dataset, narrowed only by row filters. Scoping that read
  to the dataset's own organization is separate work; this record does not decide it.
- **Depends on `celine-sdk` 2.0.0** (`realm_roles`, `is_platform_admin`, `Grants`, and
  `Subject.roles`, which its policy engine emits as `input.subject.roles`), which removed the
  merging helpers `extract_groups` and `realm_groups`.
- **The tempting undo** is re-reading the `groups` claim "for compatibility", or a realm group
  named `platform-admins`. Either brings back a name an organization's group can collide with.
