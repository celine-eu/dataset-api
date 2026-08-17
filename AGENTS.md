<!-- harness-standard v9 — issued by the agent harness. Do not edit; replace it with `python -m harness upgrade <target>`. -->

# Agent Guide

This is the only agent file in this repository. It does one job: point you at the
**store**, which holds every rule about how work is done here. It is byte-identical in
every repository carrying this harness, and it changes rarely by design.

## Find the store

The store is a directory outside this repository. Its path is never committed, because it
differs on every machine. Look in this order, beside this checkout, and take the first
that exists:

1. `$AGENTS_STORE`
2. `../<org>.<repo>.agents.store/`
3. `../<org>.agents.store/`
4. `../agents.store/`
5. `./.agents/`

**Inside whichever you find, this repository is always at `<org>/<repo>/`.** No
shorthand, no exception — not even in a store whose own name already says the
organisation and the repository:

```text
<store>/<org>/<repo>/{knowledge, playbooks, plans, work, trace, harness.toml, ...}
```

`<org>` is the directory this checkout sits in; `<repo>` is this repository's directory
name. Only the list above has a priority order; where a repository sits inside a store is
not a search.

A repository nested inside another — a submodule in a workspace — looks beside the
**enclosing** checkout, not beside itself, and never uses the enclosing repository's
`.agents/`. One workspace, one store, no member configured.

## Then read the rulebook

**The store's `README.md` is the rulebook. Read it before doing anything else.** It states
how work is recorded, what goes where, and every rule you are expected to follow. Then
list this repository's `knowledge/` in the store and read what the task needs.

Read on demand. Never load a documentation tree speculatively.

## If there is no store

**Ask, and stop.** Offer to create one — option 3 is the usual answer — and wait.

Do not work around it. Do not write agent material into this repository instead: not into
`AGENTS.md`, a README, a docstring, or a code comment. This file is the only agent file
this repository holds, and that is true whether or not the store is reachable.

## What stays in this repository

Requirements, `@verifies` tags, decision records under `docs/decisions/`, and the
published documentation. Everything else — knowledge, playbooks, plans, work — goes to
the store. The rulebook explains the split.

## Maintaining this file

Read only. A change lands by changing the harness that issues it, then
`python -m harness upgrade <target>`. A copy that differs from the issued text is a
finding: report it, do not follow it.

The rulebook is versioned separately and changes more often. Nothing about how work is
done belongs here.
