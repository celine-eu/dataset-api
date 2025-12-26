# celine/dataset/api/dataset_query/parser.py
from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Any, Dict, Optional, cast

from fastapi import HTTPException
from sqlalchemy import Table, and_, func, literal, not_, or_, select
from sqlalchemy.sql import Select
from sqlalchemy.sql.elements import ColumnElement

import sqlglot
from sqlglot import exp

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Security guards
# ---------------------------------------------------------------------------

SAFE_LITERAL = re.compile(r"'([^']|'')*'")
ISO_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T")


def has_unquoted_semicolon(s: str) -> bool:
    idx = 0
    while idx < len(s):
        m = SAFE_LITERAL.match(s, idx)
        if m:
            idx = m.end()
            continue
        if s[idx] == ";":
            return True
        idx += 1
    return False


# ---------------------------------------------------------------------------
# Allowed SQL subset
# ---------------------------------------------------------------------------

FORBIDDEN_NODES = (
    exp.Insert,
    exp.Update,
    exp.Delete,
    exp.Merge,
    exp.Drop,
    exp.Alter,
    exp.Create,
    exp.TruncateTable,
    exp.Grant,
    exp.Revoke,
    exp.Commit,
    exp.Rollback,
    exp.Transaction,
    exp.Join,
    exp.Union,
    exp.Intersect,
    exp.Except,
    exp.Window,
    exp.Lateral,
)

FUNCTION_NODES = (
    exp.Func,
    exp.Anonymous,
    exp.Command,
    exp.UserDefinedFunction,
)

POSTGIS_FUNCTIONS = {
    # spatial predicates
    "st_intersects",
    "st_contains",
    "st_within",
    "st_dwithin",
    # constructors / transforms
    "st_geomfromgeojson",
    "st_setsrid",
    "st_transform",
    # accessors
    "st_x",
    "st_y",
    "st_area",
    "st_length",
}

ALLOWED_FUNCTIONS = {
    "coalesce",
    "greatest",
    "least",
    "lower",
    "upper",
    "length",
    "abs",
    "round",
    "max",
    "min",
    # PostGIS
    *POSTGIS_FUNCTIONS,
}

OP_MAP = {
    exp.EQ: lambda c, v: c == v,
    exp.NEQ: lambda c, v: c != v,
    exp.GT: lambda c, v: c > v,
    exp.GTE: lambda c, v: c >= v,
    exp.LT: lambda c, v: c < v,
    exp.LTE: lambda c, v: c <= v,
}

ALLOWED_EXPR_NODES = (
    exp.Identifier,
    exp.Column,
    exp.Where,
    exp.Paren,
    exp.Alias,
    exp.Subquery,
    exp.Select,
    exp.Literal,
    exp.Null,
    exp.Boolean,
    exp.Star,
    exp.And,
    exp.Or,
    exp.Not,
    exp.EQ,
    exp.NEQ,
    exp.GT,
    exp.GTE,
    exp.LT,
    exp.LTE,
    exp.Between,
    exp.In,
    exp.Is,
    exp.Neg,
    exp.Add,
    exp.Sub,
    exp.Mul,
    exp.Div,
    exp.Mod,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _bad_request(msg: str) -> HTTPException:
    return HTTPException(status_code=400, detail=msg)


def _require(node: exp.Expression | None, msg: str) -> exp.Expression:
    if node is None:
        raise _bad_request(msg)
    return node


def _convert_literal(lit: exp.Literal) -> Any:
    token = lit.this

    if lit.is_string:
        s = token[1:-1].replace("''", "'") if token.startswith("'") else token
        if ISO_RE.match(s):
            try:
                return datetime.fromisoformat(s.replace("Z", "+00:00"))
            except Exception:
                return s
        return s

    if lit.is_int:
        return int(token)

    if lit.is_number:
        return float(token)

    return token


def _dump_ast_debug(label: str, node: exp.Expression) -> None:
    logger.debug("%s AST: %s", label, node)
    try:
        logger.debug("%s AST dump:\n%s", label, node.dump())
    except Exception:
        logger.debug("%s AST dump: <unavailable>", label)


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def _validate_statement_is_select(stmt: exp.Expression) -> exp.Select:
    if not isinstance(stmt, exp.Select):
        raise _bad_request("Only SELECT queries are allowed")
    return stmt


def _reject_forbidden_nodes(stmt: exp.Expression) -> None:
    for node in stmt.walk():
        if isinstance(node, FORBIDDEN_NODES):
            raise _bad_request(f"Forbidden SQL construct: {type(node).__name__}")


def _get_from_sources(from_: exp.From) -> list[exp.Expression]:
    if getattr(from_, "expressions", None):
        return list(from_.expressions)
    if getattr(from_, "this", None) is not None:
        return [from_.this]
    return []


def _validate_from_single_source(
    sel: exp.Select,
    *,
    dataset_table_name: str,
    allowed_cte_names: set[str],
) -> str:
    from_ = sel.args.get("from_")
    if not from_:
        raise _bad_request("FROM clause is required")

    # SQLGlot may store the source either in `this` or `expressions`
    if from_.this is not None:
        table_expr = from_.this
    elif len(from_.expressions) == 1:
        table_expr = from_.expressions[0]
    else:
        raise _bad_request("Exactly one FROM source is required")

    if not isinstance(table_expr, exp.Table):
        raise _bad_request("FROM must reference a table")

    if table_expr.args.get("alias") is not None:
        raise _bad_request("Table aliases are not allowed")

    if table_expr.args.get("db") is not None:
        raise _bad_request("Qualified table names are not allowed")

    table_name = table_expr.name

    if table_name != dataset_table_name and table_name not in allowed_cte_names:
        raise _bad_request(f"Access to table '{table_name}' is not allowed")

    return table_name


def _function_name(node: exp.Expression) -> str:
    """
    Extract a normalized SQL function name from a sqlglot AST node.
    """
    if isinstance(node, exp.Anonymous):
        return str(node.this).lower()

    if isinstance(node, exp.Func):
        return node.sql_name().lower()

    if isinstance(node, exp.UserDefinedFunction):
        return node.name.lower()

    return node.__class__.__name__.lower()


def _validate_expression_tree(expr: exp.Expression) -> None:
    if isinstance(expr, exp.Where):
        _validate_expression_tree(expr.this)
        return
    if isinstance(expr, exp.Paren):
        _validate_expression_tree(expr.this)
        return

    for node in expr.walk():
        if isinstance(
            node,
            (exp.Subquery, exp.Select, exp.From, exp.With, exp.CTE, exp.Table),
        ):
            continue

        if isinstance(node, FUNCTION_NODES):
            fname = _function_name(node)
            if fname and fname not in ALLOWED_FUNCTIONS:
                raise _bad_request(f"Function '{fname}' not allowed")
            continue

        if isinstance(node, ALLOWED_EXPR_NODES):
            continue

        raise _bad_request(f"Unsupported expression node: {type(node).__name__}")


def _validate_distinct(sel: exp.Select) -> None:
    distinct = sel.args.get("distinct")
    if isinstance(distinct, exp.Distinct):
        if distinct.args.get("on") is not None or distinct.expressions:
            raise _bad_request("DISTINCT ON is not allowed")


def _validate_ctes(
    sel: exp.Select, *, dataset_table_name: str
) -> Dict[str, exp.Select]:
    with_clause = sel.args.get("with")
    if not with_clause:
        return {}

    ctes: Dict[str, exp.Select] = {}

    for cte in with_clause.expressions:
        alias = cte.alias_or_name
        if not alias:
            raise _bad_request("CTE missing name")
        if alias in ctes:
            raise _bad_request(f"Duplicate CTE '{alias}'")

        sub = cte.this
        if not isinstance(sub, exp.Select):
            raise _bad_request("CTE body must be SELECT")

        _reject_forbidden_nodes(sub)
        _validate_distinct(sub)

        _validate_from_single_source(
            sub,
            dataset_table_name=dataset_table_name,
            allowed_cte_names=set(),
        )

        ctes[alias] = sub

    return ctes


# ---------------------------------------------------------------------------
# AST → SQLAlchemy
# ---------------------------------------------------------------------------


def _resolve_column(
    col: exp.Column,
    *,
    table: Table,
    cte_sources: Dict[str, Any],
) -> ColumnElement:
    if col.table:
        raise _bad_request("Qualified column references are not allowed")

    name = col.name
    if name in table.c:
        return table.c[name]

    for src in cte_sources.values():
        if hasattr(src, "c") and name in src.c:
            return src.c[name]

    raise _bad_request(f"Unknown column '{name}'")


def _ast_to_sqla(
    node: exp.Expression,
    *,
    table: Table,
    cte_sources: Dict[str, Any],
    allowed_cte_names: set[str],
    depth: int = 0,
) -> ColumnElement:
    if isinstance(node, exp.Where):
        return _ast_to_sqla(
            node.this,
            table=table,
            cte_sources=cte_sources,
            allowed_cte_names=allowed_cte_names,
            depth=depth + 1,
        )

    if isinstance(node, exp.Paren):
        return _ast_to_sqla(
            node.this,
            table=table,
            cte_sources=cte_sources,
            allowed_cte_names=allowed_cte_names,
            depth=depth + 1,
        )

    if isinstance(node, exp.Literal):
        return literal(_convert_literal(node))

    if isinstance(node, exp.Null):
        return literal(None)

    if isinstance(node, exp.Boolean):
        return literal(str(node.this).lower() == "true")

    if isinstance(node, exp.Column):
        return _resolve_column(node, table=table, cte_sources=cte_sources)

    if isinstance(node, exp.Alias):
        inner = _ast_to_sqla(
            node.this,
            table=table,
            cte_sources=cte_sources,
            allowed_cte_names=allowed_cte_names,
            depth=depth + 1,
        )
        return inner.label(node.alias)

    if isinstance(node, exp.And):
        return and_(
            _ast_to_sqla(
                node.this,
                table=table,
                cte_sources=cte_sources,
                allowed_cte_names=allowed_cte_names,
            ),
            _ast_to_sqla(
                node.expression,
                table=table,
                cte_sources=cte_sources,
                allowed_cte_names=allowed_cte_names,
            ),
        )

    if isinstance(node, exp.Or):
        return or_(
            _ast_to_sqla(
                node.this,
                table=table,
                cte_sources=cte_sources,
                allowed_cte_names=allowed_cte_names,
            ),
            _ast_to_sqla(
                node.expression,
                table=table,
                cte_sources=cte_sources,
                allowed_cte_names=allowed_cte_names,
            ),
        )

    if isinstance(node, exp.Not):
        return not_(
            _ast_to_sqla(
                node.this,
                table=table,
                cte_sources=cte_sources,
                allowed_cte_names=allowed_cte_names,
            )
        )

    for op, fn in OP_MAP.items():
        if isinstance(node, op):
            return fn(
                _ast_to_sqla(
                    node.this,
                    table=table,
                    cte_sources=cte_sources,
                    allowed_cte_names=allowed_cte_names,
                ),
                _ast_to_sqla(
                    node.expression,
                    table=table,
                    cte_sources=cte_sources,
                    allowed_cte_names=allowed_cte_names,
                ),
            )

    if isinstance(node, FUNCTION_NODES):
        fname = _function_name(node)
        if fname not in ALLOWED_FUNCTIONS:
            raise _bad_request(f"Function '{fname}' not allowed")

        args = [
            _ast_to_sqla(
                a,
                table=table,
                cte_sources=cte_sources,
                allowed_cte_names=allowed_cte_names,
            )
            for a in (node.expressions or [])
        ]
        return getattr(func, fname)(*args)

    if isinstance(node, exp.Subquery):
        sq = _build_select(
            node.this,
            table=table,
            cte_sources=cte_sources,
            allowed_cte_names=allowed_cte_names,
            is_subquery=True,
        )
        return sq.scalar_subquery()

    if isinstance(node, exp.Between):
        low = node.args.get("low")
        high = node.args.get("high")

        if low is None or high is None:
            raise _bad_request("Malformed BETWEEN expression")

        return _ast_to_sqla(
            node.this,
            table=table,
            cte_sources=cte_sources,
            allowed_cte_names=allowed_cte_names,
        ).between(
            _ast_to_sqla(
                low,
                table=table,
                cte_sources=cte_sources,
                allowed_cte_names=allowed_cte_names,
            ),
            _ast_to_sqla(
                high,
                table=table,
                cte_sources=cte_sources,
                allowed_cte_names=allowed_cte_names,
            ),
        )

    raise _bad_request(f"Unsupported expression: {type(node).__name__}")


def _build_select(
    sel: exp.Select,
    *,
    table: Table,
    cte_sources: Dict[str, Any],
    allowed_cte_names: set[str],
    is_subquery: bool = False,
) -> Select:

    # Collect CTE names defined at this level
    local_cte_names = set(allowed_cte_names)

    with_ = sel.args.get("with_")
    if with_:
        for cte in with_.expressions:
            name = cte.alias_or_name
            if not name:
                raise _bad_request("CTE missing name")
            local_cte_names.add(name)

    cte_sources = dict(cte_sources)

    if with_:
        for cte in with_.expressions:
            name = cte.alias_or_name
            if not name:
                raise _bad_request("CTE missing name")

            cte_stmt = _build_select(
                cte.this,
                table=table,
                cte_sources=cte_sources,
                allowed_cte_names=local_cte_names,
                is_subquery=True,
            )

            cte_sources[name] = cte_stmt.cte(name)

    src_name = _validate_from_single_source(
        sel,
        dataset_table_name=table.name,
        allowed_cte_names=local_cte_names,
    )

    from_obj = table if src_name == table.name else cte_sources[src_name]

    sa_cols = []
    for e in sel.expressions:
        if isinstance(e, exp.Star):
            sa_cols.extend(list(from_obj.c))
        else:
            sa_cols.append(
                _ast_to_sqla(
                    e,
                    table=table,
                    cte_sources=cte_sources,
                    allowed_cte_names=local_cte_names,
                )
            )

    stmt = select(*sa_cols).select_from(from_obj)

    if sel.args.get("distinct") is not None:
        stmt = stmt.distinct()

    if sel.args.get("where") is not None:
        stmt = stmt.where(
            _ast_to_sqla(
                sel.args["where"],
                table=table,
                cte_sources=cte_sources,
                allowed_cte_names=local_cte_names,
            )
        )

    if is_subquery and len(sa_cols) != 1:
        raise _bad_request("Subquery must return exactly one column")

    if sel.args.get("order") is not None:
        order_clauses = []
        for ordered in sel.args["order"].expressions:
            col = _ast_to_sqla(
                ordered.this,
                table=table,
                cte_sources=cte_sources,
                allowed_cte_names=local_cte_names,
            )

            if ordered.desc:
                col = col.desc()
            else:
                col = col.asc()

            order_clauses.append(col)

        stmt = stmt.order_by(*order_clauses)

    return stmt


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def parse_sql_query(sql: str, table: Table) -> Select:
    if not sql or not sql.strip():
        raise _bad_request("Empty SQL query")

    if has_unquoted_semicolon(sql):
        raise _bad_request("Semicolons are not allowed")

    stmt = sqlglot.parse_one(sql)
    _dump_ast_debug("QUERY", stmt)

    sel = _validate_statement_is_select(stmt)
    _reject_forbidden_nodes(sel)
    _validate_distinct(sel)

    cte_asts = _validate_ctes(sel, dataset_table_name=table.name)
    cte_sources: Dict[str, Any] = {}

    for name, cte_sel in cte_asts.items():
        cte_stmt = _build_select(
            cte_sel,
            table=table,
            cte_sources=cte_sources,
            allowed_cte_names=set(),
        )
        cte_sources[name] = cte_stmt.cte(name=name)

    sa_stmt = _build_select(
        sel,
        table=table,
        cte_sources=cte_sources,
        allowed_cte_names=set(cte_sources.keys()),
    )

    logger.debug("SQLAlchemy statement built: %s", sa_stmt)
    return sa_stmt


def parse_sql_filter(filter_sql: str, table: Table) -> ColumnElement:
    """
    Backward-compatible helper used by the Dataset Query API.

    Parses a SQL WHERE-like expression and returns a SQLAlchemy
    boolean expression usable in .where().
    """
    sql = f"SELECT * FROM {table.name} WHERE {filter_sql}"
    stmt = parse_sql_query(sql, table)

    if stmt.whereclause is None:
        raise HTTPException(status_code=400, detail="Invalid filter expression")

    return stmt.whereclause
