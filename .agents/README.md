# `.agents/` — checker configuration only

This directory holds what the conformance checker reads about **this** repository:

| File | What it is |
|---|---|
| `harness.toml` | what this repository declares about itself — profile, traceability delegation, workspace members |
| `references.md` | the register of values the repository refers to but must not commit |
| `references.local.md` | those values, and the companion's location. **Gitignored** |
| `trace/` | the generated requirement-to-verification matrix, where this checker owns it |

**Nothing else belongs here.** Knowledge, playbooks, plans and work live in this
repository's **companion** — a directory outside this tree, which is the only source of
truth for all four (REQ-0013). The rulebook that governs them is the companion's own
`README.md`; start there, and read `AGENTS.md` at the root first.

A repository that keeps no local declarations may gitignore this directory entirely. The
checker then reports the requirements about it as *not applicable*, which is the correct
answer rather than a gap.
