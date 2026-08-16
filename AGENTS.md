<!-- harness-standard v4 — issued by the agent harness. Do not edit; replace it with `python -m harness upgrade <target>`. -->

# Agent Guide

This file is the entry point. It is **navigation and constraints**: where things are, and
what you may not do.

It says nothing about this repository in particular. **It is standard — byte-identical in
every repository carrying this harness** — so having read it once you have read it
everywhere. Nothing repository-specific is ever added here. Content that seems to belong
in this file belongs in one of the homes below instead, and the rule that decides which is
in the rulebook.

## Read in this order

1. This file.
2. `.agents/README.md` — the rulebook: where work is recorded, and how. Also standard,
   also identical everywhere.
3. `.agents/references.local.md` — gitignored, and it names this repository's
   **companion**: the parallel directory holding the knowledge, playbooks, plans and work.
   The companion is the only source of truth for all four.
4. The companion's `knowledge/` — what is true of this repository and not visible in its
   code. List the directory; read what the task needs.
5. `docs/`, on demand. Never speculatively.

The two standard files are the same wherever they appear. Having read them at one root, do
not read them again in a repository nested inside it — read that repository's companion
`knowledge/` instead, because that is the part which differs. **Each repository has its
own companion**; a nested repository does not share the outer one's.

**If a copy of a standard file does differ, the divergence is the finding.** Report it;
do not follow it and do not quietly reconcile it.

## Where things are

| Looking for | Go to |
|---|---|
| what this repository is and does | its `README.md`, then `docs/` |
| where the companion is | `.agents/references.local.md` |
| what is true of the code and not obvious from reading it | companion `knowledge/` |
| how a repeated procedure is performed | companion `playbooks/` |
| what is being worked on, and how far it has got | companion `plans/`, `work/` |
| why a technical choice was made | `docs/decisions/` |
| what the product must do | the specifications in `docs/` |
| whether a requirement is verified | `.agents/trace/`, or the tool named in `.agents/harness.toml` |
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
  here is recorded in the companion `knowledge/`.
- **Change the component that owns the behaviour**, not the place that consumes it. A
  workaround written at the consumer is a defect left in the owner.

## Maintaining this file

**Read only.** Do not edit it, and do not edit `.agents/README.md` beside it. Neither is
this repository's document.

A change lands by changing the harness that issues it, after which every repository
receives the same text — `python -m harness upgrade <target>`. Editing one copy creates
the drift the standard exists to remove, and the next reader cannot tell an improvement
from an accident. REQ-0012 reports a copy that has been altered.

Anything you were about to add here has a home: a trap goes to the companion `knowledge/`, a
procedure to its `playbooks/`, a rationale to `docs/decisions/`, a description of the
system to `docs/`, and a defect to the issue tracker.
