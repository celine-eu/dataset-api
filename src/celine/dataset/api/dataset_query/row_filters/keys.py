"""Typed data keys — the values a holder stores a consenting subject's data under.

A dataspace decision names the consenting subjects twice, and the two namings are
not interchangeable:

- ``principals`` are identifiers **native to this system** — usernames a handler
  can resolve to rows;
- ``keys`` are **the values the rows are already keyed by**: supply points,
  meter identifiers, whatever the holder stores the data under. The organisation
  that collected the consent registers them with it, so the holder's data plane
  needs no call back to that organisation at query time.

A key is ``"<type>:<value>"`` — the type a lowercase token, the value everything
after the **first** colon, so a value may itself contain one. The type is open:
neither the control plane nor this module interprets it. A handler declares the
type it keys rows on (``args.key_type``) and matches the column against the
values of that type only.

**This grammar is a contract with the control plane, not a local convention.**
It is defined by ds in ``ds.governance.dataplane`` (``split_key``,
``values_of_type``), which this repository cannot import: ``ds.governance`` is a
library of another platform's repository and is not on this service's index.
What is duplicated is thirty lines of parsing with a test naming the same cases
as ds's own contract test; what must not be duplicated is the *decision* about
what a key means, which stays ds's. If the grammar ever moves to a published
package, delete this module and import it.

Keys are personal data. They may reach a predicate and nothing else — not the
audit disclosure, not the logs, not a plan's ``meta``.
"""

from __future__ import annotations

import re
from typing import Iterable

#: ``"<type>:<value>"``. The type is a lowercase token of at most 32 characters;
#: the value is any run of non-space characters, split off at the **first** colon.
SUBJECT_KEY_PATTERN = re.compile(
    r"^(?P<type>[a-z][a-z0-9_-]{0,31}):(?P<value>\S{1,256})$"
)

#: The one type in use today: a supply point. Named here so a test and a
#: document can agree on the spelling, never to close the set — the type is open
#: and a handler is configured with the one it wants.
POD = "pod"


def split_key(key: str) -> tuple[str, str]:
    """``("pod", "EX…")`` from ``"pod:EX…"``; ``ValueError`` for anything else."""
    match = SUBJECT_KEY_PATTERN.match(key or "")
    if match is None:
        raise ValueError(
            f"{key!r} is not a typed key — expected '<type>:<value>', e.g. 'pod:…'"
        )
    return match.group("type"), match.group("value")


def values_of_type(keys: Iterable[str], key_type: str) -> set[str]:
    """The values in *keys* whose type is *key_type*.

    A malformed key is skipped rather than fatal. The list is an allow-list of
    several subjects' keys, and one unparseable entry must narrow the answer by
    one subject, never widen it to everything or refuse the whole decision —
    which would turn a single bad row at the collector into an outage for
    everyone who consented.
    """
    values: set[str] = set()
    for key in keys or ():
        try:
            kind, value = split_key(key)
        except (ValueError, TypeError):
            continue
        if kind == key_type:
            values.add(value)
    return values
