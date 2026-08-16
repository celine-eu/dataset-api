<!-- harness-standard v7 — issued by the agent harness. Do not edit; replace it with `python -m harness upgrade <target>`. -->

# Agent Guide

This is the only agent file in this repository. It says where things are and what you
may not do. It is byte-identical in every repository carrying this harness.

## Find the store

Everything else — the rulebook, knowledge, playbooks, plans and work — lives in a
**store** outside this repository. Look for it in this order and take the first that
exists:

1. `$AGENTS_STORE` — if that variable is set, it is the store root.
2. `../agents.store/` — beside this checkout, shared by every repository around it.
3. `../<name>.store/` — for a repository kept on its own.

Inside a store this repository is `<org>/<name>/`, so one store holds several
organisations without their repository names colliding. `<name>/` directly works too.
`<name>` is this repository's directory name.

A repository nested inside another — a submodule in a workspace — looks beside the
**enclosing** checkout, not beside itself. One workspace, one store, no member
configured.

**Read the store's `README.md` first.** It is the rulebook and it states how work is
recorded. Then list the repository's `knowledge/` and read what the task needs.

If none of the three exists, **ask, and stop**. Do not write agent material into this
repository instead — not into a docstring, a README, or a code comment. This file is the
only agent file the repository holds.

## Where things are

| Looking for | Go to |
|---|---|
| what this repository is and does | its `README.md`, then `docs/` |
| how work is recorded | the store's `README.md` |
| what is true of the code and not obvious from reading it | the store's `knowledge/` |
| how a repeated procedure is performed | the store's `playbooks/` |
| what is being worked on, and how far it has got | the store's `plans/`, `work/` |
| why a technical choice was made | `docs/decisions/` |
| what the product must do | the specifications in `docs/` |
| what is broken | the issue tracker |
| how the parts are composed, built and run | the build files at the root |

Read on demand. Never load the documentation tree speculatively.

## Rules

- **Ask rather than decide** when a request needs a requirement that does not exist yet.
  Do not proceed on an inferred requirement.
- **Write the plan first** for anything non-trivial, and create its work directory before
  the first change of any phase.
- **Establish the baseline before changing anything**, so a pre-existing failure is never
  attributed to your change.
- **Report faithfully.** Name what ran, what did not, and what was skipped.
- **Check whether the change crosses a seam** — an interface another component depends
  on. A change that crosses one is not local, however local it compiles.
- **Change the component that owns the behaviour**, not the place that consumes it.
- **Requirements and `@verifies` tags stay in this repository.** Everything else goes to
  the store.

## Maintaining this file

Read only. A change lands by changing the harness that issues it, then
`python -m harness upgrade <target>`. A copy that differs from the issued text is a
finding: report it, do not follow it.

Each repository has its own store directory. A repository nested inside another does not
share the outer one's.
