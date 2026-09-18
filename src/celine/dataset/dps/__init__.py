"""dataset-api in EDC mode: a Data Plane Signaling (DPS) data plane.

EDC deprecated its own data plane and moved EDR handling to data planes that
speak DPS (`eclipse-dataplane-signaling/dataplane-signaling`). This package is
that data plane for the governed query API: a control plane signals it, it
issues the pull token at `/start`, and the pull endpoint serves rows only while
the signalled flow is `STARTED`.

Layout — the first four modules import nothing from dataset-api, so another
data plane can lift them unchanged:

- `messages`  — the DPS wire shapes (tolerant of EDC 0.18.0's RC2 names)
- `flows`     — the data flow state machine and its in-memory store
- `tokens`    — the pull token, shaped like EDC's own
- `service`   — the data plane: signals in, status messages out
- `callbacks` — data plane → control plane notifications
- `settings`, `auth`, `api` — how dataset-api mounts and guards it

The specification is `docs/dps-data-plane.md`.
"""
