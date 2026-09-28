"""What of a SQL statement may reach a log (QE-03, docs/query-engine.md).

A statement's literals are the caller's data, not its structure: the point a
boundary lookup asks about is a supply address's coordinates, and the `IN` list
a row filter adds names a member's sensor ids. A log keeps the statement's
**shape** — every literal replaced by `?` — which is what a reader debugging a
rejection or a slow query needs, and none of those values.

No hash of the original text is logged either. Coordinates and ids are low
entropy inside a known shape, so a hash of the statement could be reversed by
enumerating candidates.
"""

from __future__ import annotations

import sqlglot
from sqlglot import exp

WITHHELD = "<withheld: not parseable>"


def _mask(node: exp.Expression) -> exp.Expression:
    if isinstance(node, exp.Literal):
        return exp.Var(this="?")
    return node


def sql_shape(sql: str | exp.Expression) -> str:
    """The statement with every literal replaced by `?`, rendered as postgres.

    Text that sqlglot cannot parse is withheld whole rather than logged raw: a
    statement nobody can parse is also one nobody can mask.
    """
    try:
        ast = (
            sqlglot.parse_one(sql, read="postgres") if isinstance(sql, str) else sql
        )
        if ast is None:
            return WITHHELD
        # `comments=False`: a comment is free text and can carry anything; the
        # raw statement is logged before the parser refuses comments.
        return ast.copy().transform(_mask).sql(dialect="postgres", comments=False)
    except Exception:
        return WITHHELD
