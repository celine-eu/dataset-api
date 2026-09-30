# The row filter a dataspace decision carries

When a query arrives through the dataspace — the legacy EDR path or the DPS pull
endpoint — this service asks the ds connector one question and enforces one
answer. The answer is a verdict per dataset, and an *allow* may carry a **row
filter**: not permission to serve the dataset, but an instruction to serve
*these rows*.

The filter names the consenting subjects in two vocabularies, and which one a
column speaks is the handler's business:

| Field | What it holds | Who reads it |
|---|---|---|
| `handler` | the name governance declared, e.g. `rec_registry` | this service, to pick a handler |
| `args` | that handler's own arguments, uninterpreted by ds | the handler |
| `principals` | identifiers **native to this system** — usernames. Never subject DIDs | `direct_user_match`, `rec_registry` |
| `keys` | **typed data keys**, `"<type>:<value>"` — the values this holder already stores those subjects' rows under | `subject_key_match` |
| `subject_dids` | the same people as **subject DIDs**. Narrows nothing | nothing — it is what the audit disclosure reports (RF-10) |

`keys` exist because the organisation holding people's data is not always the
organisation those people belong to. A grid operator holds meter readings keyed
by supply point; the people are members of an energy community, and the
community is where they consent. The community registers the members' supply
points with the consent at the operator's connector, so the operator's data
plane matches the rows it already has and never calls the community back at
query time.

Contract: `ds.governance.dataplane` in the ds repository (`DataplaneRowFilter`).
Code: `src/celine/dataset/security/edr.py` (the wire shape),
`src/celine/dataset/api/dataset_query/row_filters/` (the handlers).
Each clause below names its tests.

## Handlers

| Handler | Reads | Resolves | Args |
|---|---|---|---|
| `direct_user_match` | `principals` | nothing — the column holds the principal itself | `column` |
| `rec_registry` | `principals` | a member to the meters they own, through the REC registry | `column`; `url` (default `REC_REGISTRY_URL`) |
| `subject_key_match` | `keys` | nothing — the column holds a data key, and `args.key_type` names which type | `column`, `key_type` |
| `http_in_list` | — | the caller's own rows; refuses a delegated request | `column`, `url`; `method` (`GET`), `headers`, `params`, `json`, `response_path` (`$`), `timeout_seconds` (5), `max_items` (2000), `empty_means_deny` (true), `forward_token` (false) |
| `table_pointer` | — | the caller's own rows; refuses a delegated request | `column`, `pointer_table`, `pointer_key_column`; `pointer_subject_column` (`user_id`) |

Args before a `;` are required. `http_in_list` formats its string args with
`{sub}`, `{username}`, `{email}` and `{token}`.

Handler names belong to the data plane, not to ds: ds passes the name through
from `governance.yaml` and never interprets it. The two ends agree through that
file, which the connector reads and this service must recognise.

### `rec_registry` on the normal API path

The same handler serves requests that do not come through the dataspace, where
`principals` is absent. Before any handler runs, the normal path answers an
unauthenticated caller with `401` and skips row filters for members of the
`admins` group. The handler then decides by who the caller is, in this order:

1. **`principals` present** (dataspace path only), even empty: a delegated
   request, resolved for the named members on this service's identity (RF-05).
2. **A service account**: not narrowed at all. A service is not a registry
   member, has no meters of its own, and has already passed the policy's
   `dataset.query` check; the handler returns no predicate and the whole table
   is served.
3. **A person**: the registry is asked for the caller's own assets with the
   caller's token, and the rows narrow to their sensor ids — or to no rows
   when they own no metered asset (RF-11) or are no member at all (RF-12).

The second case is deliberate and easy to miss. Any service allowed to query a
`rec_registry`-filtered dataset reads every member's rows, so a service that
relays those rows to a person narrows them itself — or forwards the person's
token, as the Digital Twin does for a participant's queries, so that the third
case applies. The order is what keeps a delegated request from ever reaching the
second case: a dataspace query always arrives on a service identity.
Code: `src/celine/dataset/api/dataset_query/row_filters/handlers/rec_registry.py`.

## Clauses

A clause marked **planned** states behaviour decided but not yet in the code;
the change that lands it adds its tests and removes the status line.

### RF-01 — The filter travels whole

A verdict's `row_filter` carries `handler`, `args`, `principals`, `keys` and
`subject_dids`, never a column and a list of ids. A decision reduced to a column
would force this service to assume a handler, and assuming the wrong one injects
a predicate that matches by coincidence or not at all.

A verdict with no `row_filter` is an allow with no filter: the dataset carries no
data subject and every row may leave. That is never how a filter that could not
be built is reported — the two would be indistinguishable here.
Tests: `tests/security/test_dataplane_row_filter_contract.py`.

### RF-02 — An unknown field in the filter refuses the request

The filter is parsed with unknown fields forbidden. A field this service does not
know is a narrowing it would otherwise skip, and skipping a narrowing serves rows
the control plane said to withhold — silently, on both sides. The refusal is a
`502`: the consumer did nothing wrong and can do nothing about it, the two ends
of the contract disagree.

The cost is accepted and symmetric with ds's own choice: upgrading the connector
ahead of this service stops the data plane rather than widening it. **So every
field in the filter but `handler` has a default**, here and in ds: `forbid` governs what a
reader has never heard of, and making a known field *required* would refuse an
older connector too. One impossible direction is a deployment order; two are a
deadlock. See RF-10.

Only the filter of a dataset the query actually touches is parsed, so a filter
for another dataset cannot refuse a query that is otherwise fine. The envelope
around the verdicts is **not** parsed strictly — fields there are not narrowings.
Tests: `tests/security/test_dataplane_row_filter_contract.py`,
`tests/api/test_edr_subject_key_filter.py`.

### RF-03 — A typed data key is `"<type>:<value>"`

The type is a lowercase token (`[a-z][a-z0-9_-]{0,31}`); the value is everything
after the **first** colon, so a value may contain one. The type is open: neither
ds nor this service interprets it, and `pod` is simply the one in use.

A malformed key is skipped, not fatal. The list holds several subjects' keys, and
one unreadable entry must narrow the answer by one subject rather than turn a bad
row at the collecting organisation into an outage for everyone who consented.
Code: `src/celine/dataset/api/dataset_query/row_filters/keys.py`, which mirrors ds's
`split_key` / `values_of_type` because `ds.governance` is not on this service's index.
Tests: `tests/api/dataset_query/test_subject_key_match.py`.

### RF-04 — `subject_key_match` matches the column against one type's keys

`args`: `column` (required) and `key_type` (required). The predicate is
`column IN (…)` over the values of that type, sorted so the same allow-list always
renders the same SQL. Keys of another type are not ours to match.

**The principals are ignored, and are not a fallback.** They name the same people
in a vocabulary this column does not speak; matching a username against a supply
point returns nothing, or something by coincidence. A filter naming no `column`
or no `key_type` cannot be applied and is an error (a `500`), never an empty filter.
Tests: `tests/api/dataset_query/test_subject_key_match.py`,
`tests/api/test_edr_subject_key_filter.py`.

### RF-05 — An empty allow-list narrows to nothing

Both lists are allow-lists. An empty one means *no rows*; it never means *no
filter*, and it never falls back to the caller's own rows — in a dataspace query
the caller is a service identity that owns none of the data.

`principals` distinguishes **absent** (`None`, a self-service request on the
normal API path) from **present and empty** (`[]`, a delegated request naming
nobody). Reading `[]` as self-service is how a decision that named no member
came to be answered with every row in the table, through `rec_registry`'s
service-account bypass. ds can send that shape now, because a decision may name
its subjects by key alone.
Tests: `tests/api/dataset_query/test_delegated_allow_lists.py`,
`tests/api/dataset_query/test_subject_key_match.py`.

### RF-06 — A handler this data plane cannot apply refuses the request

An *allow* carrying a filter says *these rows*. A data plane that cannot apply
the filter has not been permitted to serve unfiltered ones — it has been given an
instruction it does not understand. An unknown handler, or a handler that refuses
delegation, is a `403` naming the handler; no rows are served and no disclosure
is recorded, because nothing was disclosed.
Tests: `tests/api/test_edr_subject_key_filter.py`.

### RF-07 — A resolved plan is cached by whose rows it is for

The plan cache is keyed by handler, table, caller, **both allow-lists** and args.
The caller alone is not enough: in delegation it is one service account for every
agreement. Nor are the principals alone: a `subject_key_match` decision often
carries none, so two consents would differ only in their keys and the second
would be served the first one's rows.

The keys enter the cache key as a digest, not in clear. They are personal data and
a cache key is the kind of string that ends up in a repr.

A plan lives for `ROW_FILTERS_CACHE_TTL` seconds (default 300), shortened to ds's
`cache_ttl` on the dataspace path and to the token's remaining lifetime on the
normal path; the cache holds at most `ROW_FILTERS_CACHE_MAXSIZE` plans (default
10 000).
Tests: `tests/api/dataset_query/test_delegated_allow_lists.py`.

### RF-08 — Keys reach the predicate and nothing else

They are personal data the collecting organisation registered with the consent.
They must not reach the audit disclosure sent to ds (which names subjects by
DID — RF-10), a plan's `meta` (which carries a count and the type), an error detail,
or the logs — including the debug line that renders the completed SQL, which is
withheld on a dataspace request because the predicate is a literal list of the
consenting subjects.

Not covered: what the database driver logs, and what a query error message may
quote. A deployment that logs statements server-side logs the predicate with them.
Tests: `tests/api/test_edr_subject_key_filter.py`,
`tests/api/dataset_query/test_subject_key_match.py`.

### RF-09 — A handler written before typed keys keeps working

Handlers arrive from this package, from `ROW_FILTERS_MODULES` and from the
`celine.dataset.row_filters` entry points, and the last two are packaged
elsewhere. `keys` is passed only to a `resolve` that declares it (or accepts
`**kwargs`), so an older handler is not broken by an argument it was never going
to read — the lists name the same people, and a handler reads the one it knows.
Tests: `tests/api/dataset_query/test_delegated_allow_lists.py`.

### RF-10 — The disclosure names the subjects by DID

`authorized_subject_ids` on `POST /internal/audit/query` is the accountability
record of a disclosure, and ds records codes, pseudonymous DIDs and hashes and
nothing else — it drops whatever else arrives. `subject_dids` is the list this
service reports; neither allow-list is. The principals are registry-native, and
in a realm where the Keycloak username is the person's email they *are* the
person's email: sending them put 22 addresses into one run's `QueryExecuted`
provenance (measured 2026-09-20, in ds).

**`subject_dids` narrows nothing**, and is the one field in the filter whose
absence is not a hazard. It never reaches a predicate — no handler matches a
column against a DID — so a decision that omits it can only thin the audit
record, which is precisely what ds's own filtering already does. It therefore
defaults to `[]` rather than being required, and that is what makes a staged
rollout possible:

> **Rebuild this service first, the ds connector second.** This service accepts
> the field before ds sends it. The reverse order is a `502` on every filtered
> dataspace query for as long as the two are out of step (RF-02).

An allow with no `row_filter` at all reports `None`, not `[]`: it names no
subjects, and an empty list would read as *authorised for nobody*.
Tests: `tests/security/test_dataplane_row_filter_contract.py`,
`tests/dps/test_pull.py`, `tests/api/test_edr_subject_key_filter.py`.

### RF-11 — A caller with no metered asset gets no rows

On the self-service path (a person's own token, `principals` absent), a caller
whose registry assets carry no sensor id is answered with no rows — a `deny`
plan, the shape RF-05 and the delegated path already use — and never with
`column IN ()`. That caller is ordinary, not an edge case: it is every member
between approval and a manager attaching their meter.

The handler used to build the predicate from whatever the list held, and an
empty list rendered `IN ()`, which PostgreSQL rejects as a syntax error; the
query failed where it should have answered empty. An empty assets page is a
valid answer and reaches the deny, not a 500. The distinction RF-05 draws
holds here too: a registry that cannot be reached, or gives no answer at all,
is still an error, never read as "owns nothing".
Issue: [celine-eu/dataset-api#74](https://github.com/celine-eu/dataset-api/issues/74).
Tests: `tests/api/dataset_query/test_rec_registry_self_service.py` — a caller
with no assets, and a caller whose only assets have no sensor id, each answered
with a deny that renders no `IN` and executes to no rows; metered assets still
narrow to their sensor ids; no answer and a registry failure stay errors.

### RF-12 — A caller who is no registry member gets no rows

On the self-service path, the registry answers `GET /user/assets` for a person
with no membership in any community with
`403 {"detail": "You are not a member of any community", "code": "not_a_member"}`
(rec-registry's error-code vocabulary; a registry older than the codes sends the
detail alone). That answer is a fact about the caller, not a failure: they own
no asset, so no row is theirs, and the handler answers with the same `deny` plan
as RF-11. Before, the SDK's `UnexpectedStatus` propagated and the query failed
with a 500.

Only that answer maps to a deny. The status must be 403 and the body the
registry's own JSON object, judged this way:

- a body with a `code` is judged by the code alone: `not_a_member` is the
  answer whatever the detail says, so the registry may reword its sentence; any
  other code (or a code that is not that exact string) is not, even beside the
  old detail;
- a body with no `code` (or `code: null`) — a registry that predates the codes —
  is the answer only with the exact detail above.

It is the only 403 that route gives — the registry's middleware answers a
missing or invalid token with 401 — but a bare 403 could come from anything in
front of the registry (a gateway, a proxy), and reading that as "owns nothing"
would turn an outage or a misconfiguration into a silent deny. Every other
answer stays an error, as RF-05 draws the line: a 403 with any other body or
code, a 401, a 404 or 5xx (even carrying the same code or words), an unreachable
registry. The deny is cached like any plan (RF-07), so a person who becomes a
member sees their rows once the cache entry expires.

The handler asks through the SDK's public `RecRegistryUserClient.get_my_assets`
(celine-sdk REQ-0132), which raises `RecRegistryApiError` on anything but a
readable 200 — never `UnexpectedStatus` — carrying the status, the raw body and
the registry's `code` and `detail` read from the top level of a JSON object
body (`None` for a body that is not one). The match reads that error: status
403, then the `code`, then — only when the body names no `code` at all — the
detail. A `code` the SDK does not read (one that is not a string) still counts
as a code, so it never falls through to the detail match. Any other exception
is not the answer. A failure is logged with its status and code only, not the
registry's sentence. This needs a celine-sdk that carries REQ-0132, the
1.21.0 release: `pyproject.toml` requires `celine-sdk>=1.21.0`. Against 1.20.0,
`get_my_assets` raises `UnexpectedStatus` on the 403 (or returns `None`) and the
answer is a 500 (the loud side), never a deny. `uv.lock` pins 1.21.0 from PyPI.
`tests/test_sdk_dependency_floor.py` pins the floor.
Code: `_is_not_a_member` in
`src/celine/dataset/api/dataset_query/row_filters/handlers/rec_registry.py`.
Tests: `tests/api/dataset_query/test_rec_registry_not_a_member.py` — the real SDK
client over a mocked transport: the coded answer, the coded answer reworded and
the detail-only answer are each a deny that executes to no rows, logged without
the caller; each other status, body and code above, and an unreachable
registry, still raise; a raw `UnexpectedStatus` carrying the answer is not read;
the handler goes through the public call and no private part of the client; a
failure's log carries status and code, not the sentence; a member's 200 on the
wire still narrows to its sensor ids, a 200 with no items is RF-11's deny, and
an unreadable 200 raises.

## Configuring a dataset for it

In `governance.yaml`, on the dataset the holder serves:

```yaml
consent_required: true
row_filters:
  - handler: subject_key_match
    args:
      column: pod        # the column this holder keys its rows by
      key_type: pod      # which type of the consent's keys that column holds
```

The column and the key type are the holder's own vocabulary. The values in it
never appear in governance: they arrive per request, on the consent.
