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

| Handler | Reads | Resolves |
|---|---|---|
| `direct_user_match` | `principals` | nothing — the column holds the principal itself |
| `rec_registry` | `principals` | a member to the meters they own, through the REC registry |
| `subject_key_match` | `keys` | nothing — the column holds a data key, and `args.key_type` names which type |
| `http_in_list` | — | the caller's own rows; refuses a delegated request |
| `table_pointer` | — | the caller's own rows; refuses a delegated request |

Handler names belong to the data plane, not to ds: ds passes the name through
from `governance.yaml` and never interprets it. The two ends agree through that
file, which the connector reads and this service must recognise.

## Clauses

### RF-01 — The filter travels whole

A verdict's `row_filter` carries `handler`, `args`, `principals` and `keys`,
never a column and a list of ids. A decision reduced to a column would force
this service to assume a handler, and assuming the wrong one injects a predicate
that matches by coincidence or not at all.

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
ahead of this service stops the data plane rather than widening it.

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
or no `key_type` cannot be applied and is an error, never an empty filter.
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
Tests: `tests/api/dataset_query/test_delegated_allow_lists.py`.

### RF-08 — Keys reach the predicate and nothing else

They are personal data the collecting organisation registered with the consent.
They must not reach the audit disclosure sent to ds (which names subjects by
principal), a plan's `meta` (which carries a count and the type), an error detail,
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
