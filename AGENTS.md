<!-- harness-standard v5 — issued by the agent harness. Do not edit; replace it with `python -m harness upgrade <target>`. -->

# Agent Guide

This file is the entry point, and the only agent material in this repository. It is
**navigation and constraints**: where things are, and what you may not do.

It says nothing about this repository in particular. **It is standard — byte-identical in
every repository carrying this harness** — so having read it once you have read it
everywhere.

## The guidance is not in this repository

Everything an agent needs in order to work here, and everything it produces while
working — the rulebook, durable knowledge, repeatable procedures, plans and execution
state — lives in this repository's **companion**: a directory outside this tree.

**The companion is the only source of truth.** Nothing in it is copied back here, and a
copy that appears here is a defect rather than a convenience.

**A clean clone does not carry its location, and that is deliberate** — the path differs
on every machine, and for a public repository the companion is usually not public.

Two files say so, and neither holds the path:

- `.agents/references.md` is committed and **declares the name `AGENTS_COMPANION`**. Its
  presence is how you know a companion exists at all.
- `.agents/references.local.md` is gitignored and **holds the path**. If it is missing,
  nobody has set this checkout up yet: obtain the companion from whoever maintains it,
  check it out, and write the path there.

**Read the companion's `README.md` first** — it is the rulebook, and it states how work
is recorded here. Then list its `knowledge/` and read what the task needs.

**If you cannot find the companion, ask, and stop.** Do not write agent material into
this repository instead: a trap recorded in a docstring, a README or a code comment
because the companion was not to hand is the failure this arrangement exists to prevent,
and it is how the arrangement quietly undoes itself.

## Read in this order

1. This file.
2. The companion's `README.md` — the rulebook: where work is recorded, and how.
3. The companion's `knowledge/` — what is true of this repository and not visible in its
   code. List the directory; read what the task needs.
4. `docs/`, on demand. Never speculatively.

Having read this file at one root, do not read it again in a repository nested inside it
— read that repository's companion instead, because that is the part which differs.
**Each repository has its own companion**; a nested repository does not share the outer
one's.

**If a copy of this file differs from the issued text, the divergence is the finding.**
Report it; do not follow it and do not quietly reconcile it.

## Where things are

| Looking for | Go to |
|---|---|
| what this repository is and does | its `README.md`, then `docs/` |
| how work is recorded here | the companion's `README.md` |
| what is true of the code and not obvious from reading it | the companion's `knowledge/` |
| how a repeated procedure is performed | the companion's `playbooks/` |
| what is being worked on, and how far it has got | the companion's `plans/`, `work/` |
| why a technical choice was made | `docs/decisions/` |
| what the product must do | the specifications in `docs/` |
| what is broken | the issue tracker. Never a file in this repository |
| how the parts are composed, built and run | the build and composition files at the root |

This table is fixed because the structure is fixed. What varies between repositories is
what those directories hold — found by listing them, never by an index maintained here. An
index here would be a second copy of a fact, and the copy is what goes stale.

## Behavioural settings

The switches, not the rules. What each one serves is stated in the rulebook.

- **Ask rather than decide** when a request needs a requirement that does not exist yet.
  Ask directly, and do not proceed on an inferred requirement.
- **Write the plan first** for anything non-trivial, and create its work directory before
  the first change of any phase.
- **Establish the baseline before changing anything**, so a pre-existing failure is never
  attributed to your change.
- **Report faithfully.** Name what ran, what did not, and what was skipped.
- **Check whether the change crosses a seam** — an interface another component depends on.
  A change that crosses one is not local, however local it compiles. Which seams exist
  here is recorded in the companion's `knowledge/`.
- **Change the component that owns the behaviour**, not the place that consumes it. A
  workaround written at the consumer is a defect left in the owner.

## Maintaining this file

**Read only.** It is not this repository's document.

A change lands by changing the harness that issues it, after which every repository
receives the same text — `python -m harness upgrade <target>`. Editing one copy creates
the drift the standard exists to remove, and the next reader cannot tell an improvement
from an accident. REQ-0012 reports a copy that has been altered.

Anything you were about to add here has a home: a trap goes to the companion's
`knowledge/`, a procedure to its `playbooks/`, a rationale to `docs/decisions/`, a
description of the system to `docs/`, and a defect to the issue tracker.
