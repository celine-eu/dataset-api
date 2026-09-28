"""The paginated outer query (QE-04, docs/query-engine.md).

The engine answers a page by wrapping the caller's statement:
`SELECT * FROM (<statement>) AS q ORDER BY … LIMIT :limit OFFSET :offset`. The
SQL standard does not promise that a subquery's `ORDER BY` survives the outer
`SELECT`, so the statement's top-level `ORDER BY` is carried onto the outer
query, rewritten to name the columns of `q`.

The outer query sees only `q`'s output columns, so each ordering key is
rewritten only where it provably names one of them:

- a bare column that names an output column (an alias or a selected column),
  which is also how PostgreSQL itself reads a bare name in `ORDER BY`;
- a key written exactly as a selected expression (`ORDER BY t.ts` beside
  `SELECT t.ts`, `ORDER BY count(*)` beside `SELECT count(*) AS n`);
- a position (`ORDER BY 2`) that points at a named output column;
- for `SELECT *` from one source with no join, any column reference, since `q`
  then carries every column of that source under its own name.

An output name selected twice, a star beside other projections, a key that is
none of the above (an expression over columns not selected, say): the key
cannot be named on `q` without risking an error the caller's statement does not
have. Then no outer `ORDER BY` is added at all — not a partial one, which would
reorder rows by a prefix of the keys — and the statement's own `ORDER BY`
stays where it is, as before this clause.
"""

from __future__ import annotations

from sqlglot import exp

OUTER_ALIAS = "q"


def _normalized(ident: exp.Identifier) -> str:
    """How PostgreSQL names the column: an unquoted identifier folds to lower case."""
    return ident.this if ident.quoted else ident.this.lower()


def _output_identifier(projection: exp.Expression) -> exp.Identifier | None:
    """The name a projection gives its output column, or None when unnamed."""
    if isinstance(projection, exp.Alias):
        alias = projection.args.get("alias")
        return alias if isinstance(alias, exp.Identifier) else None
    if isinstance(projection, exp.Column) and isinstance(
        projection.this, exp.Identifier
    ):
        return projection.this
    return None


def _is_star(projection: exp.Expression) -> bool:
    return isinstance(projection, exp.Star) or (
        isinstance(projection, exp.Column) and isinstance(projection.this, exp.Star)
    )


def _single_source(select: exp.Select) -> bool:
    """One `FROM` item and no join: `q` then has that item's columns, each once."""
    if select.args.get("joins"):
        return False
    # sqlglot names the arg `from_` since v28, `from` before.
    from_ = select.args.get("from_") or select.args.get("from")
    return from_ is not None and from_.this is not None


def _on_q(ident: exp.Identifier) -> exp.Column:
    return exp.Column(
        this=ident.copy(), table=exp.Identifier(this=OUTER_ALIAS, quoted=False)
    )


def outer_order_by(ast: exp.Expression) -> str | None:
    """The `ORDER BY` clause for the outer query, or None to add none.

    `ast` is the final statement (tables mapped, row filters applied). The
    answer is rendered as PostgreSQL, like the statement it wraps.
    """
    if not isinstance(ast, exp.Select):
        return None
    order = ast.args.get("order")
    if order is None or not order.expressions:
        return None

    projections = list(ast.expressions)
    stars = [p for p in projections if _is_star(p)]

    if stars:
        # Only a lone `*` over one source: then `q` holds that source's columns
        # and any column reference the statement could order by is one of them.
        if len(projections) != 1 or not isinstance(projections[0], exp.Star):
            return None
        if not _single_source(ast):
            return None
        keys: list[exp.Expression] = []
        for ordered in order.expressions:
            key = ordered.this
            if not isinstance(key, exp.Column) or not isinstance(
                key.this, exp.Identifier
            ):
                return None
            keys.append(_with_key(ordered, _on_q(key.this)))
        return _render(keys)

    # Explicit projections: the output names, in order.
    outputs: list[exp.Identifier | None] = [_output_identifier(p) for p in projections]
    counts: dict[str, int] = {}
    for ident in outputs:
        if ident is not None:
            counts[_normalized(ident)] = counts.get(_normalized(ident), 0) + 1

    def unique(ident: exp.Identifier | None) -> exp.Identifier | None:
        if ident is None or counts.get(_normalized(ident), 0) != 1:
            return None
        return ident

    by_name = {
        _normalized(i): i for i in outputs if i is not None and unique(i) is not None
    }
    by_expression: dict[str, exp.Identifier] = {}
    for projection, ident in zip(projections, outputs):
        if unique(ident) is None:
            continue
        source = projection.this if isinstance(projection, exp.Alias) else projection
        by_expression.setdefault(source.sql(dialect="postgres"), ident)

    keys = []
    for ordered in order.expressions:
        key = ordered.this
        target: exp.Identifier | None = None

        if isinstance(key, exp.Literal) and not key.is_string:
            # A position: 1-based, and only an integer names a column.
            try:
                position = int(key.this)
            except ValueError:
                return None
            if str(position) != key.this or not 1 <= position <= len(outputs):
                return None
            target = unique(outputs[position - 1])
        elif (
            isinstance(key, exp.Column)
            and not key.table
            and isinstance(key.this, exp.Identifier)
            and _normalized(key.this) in by_name
        ):
            # A bare name: PostgreSQL reads it as an output column first.
            target = by_name[_normalized(key.this)]
        else:
            target = by_expression.get(key.sql(dialect="postgres"))

        if target is None:
            return None
        keys.append(_with_key(ordered, _on_q(target)))

    return _render(keys)


def _with_key(ordered: exp.Expression, key: exp.Expression) -> exp.Expression:
    """The ordering term with its key replaced; direction and NULLS kept."""
    rewritten = ordered.copy()
    rewritten.set("this", key)
    return rewritten


def _render(keys: list[exp.Expression]) -> str:
    return "ORDER BY " + ", ".join(k.sql(dialect="postgres") for k in keys)


def paginated_sql(complete_sql: str, ast: exp.Expression) -> str:
    """The page query: the statement wrapped, its order carried, the page bound."""
    order_by = outer_order_by(ast)
    return f"""
        SELECT *
        FROM (
            {complete_sql}
        ) AS {OUTER_ALIAS}
        {order_by or ""}
        LIMIT :limit OFFSET :offset
    """
