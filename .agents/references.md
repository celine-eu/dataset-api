# References

The register of values this repository refers to but does not commit. It holds names
and never values: the values live in `references.local.md` beside it, which is
gitignored and never leaves the machine (REQ-0010, REQ-0011).

Two kinds, and they fail differently:

| Kind | What it is | What committing it costs |
| --- | --- | --- |
| `local` | true of one machine — a home directory, a checkout, a hostname, a port | noise: the next reader silently substitutes their own |
| `restricted` | true everywhere, publishable nowhere — an environment, an organisation, a customer, a person | a disclosure, and it survives the commit that removes it |

## How to use one

Declare the name here, put the value in `references.local.md`, and cite the name in
`{{DOUBLE_BRACES}}` wherever a document needs it.

```text
references.md         - `DEPLOY_HOST`: restricted — the environment this deploys to
references.local.md   - `DEPLOY_HOST`: the-actual-hostname
a document            Deploys are made to {{DEPLOY_HOST}} from the release branch.
```

One entry per line, `- NAME: rest`, with `NAME` in upper snake case. Fenced code is not
parsed, which is why the block above declares nothing.

## Registered

- `AGENTS_COMPANION`: local — where this repository's companion is checked out. It holds
  the knowledge, playbooks, plans and work, and it is the only source of truth for them.
  The path is one machine's fact, so it lives in `references.local.md` and never here.

Every repository declares this one, because without it nothing else in the contract can
be found. Anything beyond it is added the first time a document needs to refer to
something it must not carry.

## What this is not

- **Not a secret store.** A credential does not belong in either file. Rotate it and use
  whatever the repository already uses for secrets.
- **Not history.** A value already committed stays in the objects, and removing it here
  does not remove it there. That is a rewrite or a rotation, not a conformance finding.
- **Not a classifier.** Nothing infers that a word is an organisation's name. What is
  declared here is what the checker holds the repository to — an undeclared name is
  invisible to it, exactly as it was before.

Not every local fact needs a name. Where a tool lives on this machine is discoverable in
a second and belongs in no file; most local facts want deleting rather than declaring.
