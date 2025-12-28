# Governance & Security

This document defines the governance model (access levels, identity), the security posture (validation, isolation), and the authorization flow.

---

## Security Goals

- prevent data exfiltration by escaping the catalogue
- prevent side effects (no writes, no DDL)
- ensure authorization decisions are explainable and consistent
- keep resource usage bounded and predictable

---

## Access Levels

Each dataset declares an `access_level`:

| Access level | Intended exposure | Authentication | Authorization |
|---|---|---|---|
| `open` | public catalogue + read access | not required | not required |
| `internal` | internal audiences | required | policy may still apply |
| `restricted` | limited audiences | required | required (OPA) |

Notes:
- `open` does not mean “unsafe”; SQL hardening and limits still apply.
- `restricted` should be used when access depends on groups/roles/attributes.

---

## Identity Model

### JWT Inputs

The API receives a JWT from an Identity Provider. The raw token can vary by IdP.
The API normalizes claims to a consistent internal representation.

Typical normalized fields:
- `sub` (subject)
- `preferred_username` or display name
- `groups` (list)
- `roles` (list)
- `scopes` (list) (if provided)

### Users vs Service Accounts

Service accounts (client credentials) are treated as first-class identities.
They should carry:
- a distinct subject (client id)
- roles/groups appropriate for their operating scope
- optionally environment scoping (tenant/community)

You should avoid “god tokens” unless policy explicitly supports it.

---

## Authorization with OPA

OPA is the Policy Decision Point (PDP). The Dataset API is the Policy Enforcement Point (PEP).

### Decision flow

1. API normalizes JWT → identity object
2. API collects dataset metadata and requested action
3. API builds OPA input document
4. API calls OPA
5. OPA returns decision (allow/deny + optional reason)
6. API enforces result (deny by default)

### Example OPA input shape (illustrative)

```json
{
  "input": {
    "action": "query",
    "dataset": {
      "dataset_id": "datasets.gold.example",
      "namespace": "gold",
      "access_level": "restricted",
      "tags": ["energy", "forecast"]
    },
    "identity": {
      "sub": "service:dt-app",
      "groups": ["community:alpha", "ops"],
      "roles": ["service"],
      "scopes": ["dataset:query"]
    },
    "request": {
      "sql_tables": ["datasets.gold.example"],
      "limit": 100,
      "offset": 0
    }
  }
}
```

Your actual field names may vary; keep them stable for policy authors.

---

## SQL Hardening (Non-negotiable)

All SQL is validated using an AST parser and an allowlist.

### Allowed
- `SELECT` queries only
- explicit projections and filters
- bounded queries (`LIMIT` required or enforced)
- references only to catalogued datasets

### Rejected
- DDL: `CREATE`, `ALTER`, `DROP`, …
- DML: `INSERT`, `UPDATE`, `DELETE`, `MERGE`, …
- multiple statements / statement chaining
- functions not on allowlist
- referencing raw physical table names to bypass catalogue
- unbounded scans (policy + query guards)

---

## Defense-in-depth Execution Pipeline

1. Authentication (if required by access level)
2. SQL parse + validation
3. Extract referenced tables/datasets
4. Resolve to catalogued mapping
5. OPA authorization (dataset + action + identity)
6. Execute in restricted context with limits/timeouts
7. Return paginated results only

If any step fails → request fails.

---

## Auditability & Observability

Log (at least):
- request id
- subject
- dataset ids referenced
- allow/deny decision
- validation errors (sanitized)
- execution time and rows returned

Do not log raw SQL without sanitization if it may contain sensitive literals.

---

## Common Policy Patterns

- **Namespace gating**: allow external access only to `gold` datasets
- **Group-based**: allow if identity belongs to `dataset:{id}:read`
- **Tenant/community scoping**: allow if identity has `community:{X}` group matching dataset tag `community:{X}`
- **Service account capabilities**: allow if role includes `service` and scope includes `dataset:query`

