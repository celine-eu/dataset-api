# dataset/api/dataset_query/parser.py
from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Optional

from fastapi import HTTPException
from sqlalchemy import and_, func, literal, not_, or_
from sqlalchemy.sql.elements import ColumnElement
from sqlalchemy import Table

import sqlglot
from sqlglot import exp

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# SQL-glot configuration & guards
# ---------------------------------------------------------------------------

ALLOWED_NODES = (
    exp.Column,
    exp.Identifier,
    exp.Literal,
    exp.Paren,
    exp.And,
    exp.Or,
    exp.Not,
    exp.EQ,
    exp.NEQ,
    exp.GT,
    exp.GTE,
    exp.LT,
    exp.LTE,
)

FUNCTION_NODES = (
    exp.Func,
    exp.Anonymous,
    exp.Command,
    exp.UserDefinedFunction,
)

ALLOWED_FUNCTIONS = {
    "st_intersects",
    "st_within",
    "st_contains",
    "st_overlaps",
    "st_touches",
    "st_crosses",
    "st_equals",
    "st_disjoint",
    "st_distance",
    "st_transform",
    "st_setsrid",
    "st_astext",
    "st_asewkt",
    "st_geomfromgeojson",
    "st_point",
}

OP_MAP = {
    exp.EQ: lambda c, v: c == v,
    exp.NEQ: lambda c, v: c != v,
    exp.GT: lambda c, v: c > v,
    exp.GTE: lambda c, v: c >= v,
    exp.LT: lambda c, v: c < v,
    exp.LTE: lambda c, v: c <= v,
}

ISO_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T")
SAFE_LITERAL = re.compile(r"'([^']|'')*'")


# ---------------------------------------------------------------------------
# Semicolon / SQL injection guard
# ---------------------------------------------------------------------------


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
# AST helpers
# ---------------------------------------------------------------------------


def _require_expr(node: exp.Expression | None) -> exp.Expression:
    if node is None:
        raise HTTPException(400, "Invalid SQL expression: missing operand")
    return node


def _convert_literal(lit: exp.Literal):
    token = lit.this

    if lit.is_string:
        s = token
        if s.startswith("'") and s.endswith("'"):
            s = s[1:-1].replace("''", "'")
        if ISO_RE.match(s):
            try:
                return datetime.fromisoformat(s.replace("Z", "+00:00"))
            except Exception:  # pragma: no cover - defensive
                pass
        return s

    if lit.is_int:
        try:
            return int(token)
        except Exception:  # pragma: no cover - defensive
            pass

    if lit.is_number:
        try:
            return float(token)
        except Exception:  # pragma: no cover - defensive
            pass

    return token


def _validate_ast(expr: exp.Expression):
    for node in expr.walk():
        if isinstance(node, FUNCTION_NODES):
            fname = (node.name or "").lower()
            if not fname:
                continue
            if fname not in ALLOWED_FUNCTIONS:
                raise HTTPException(400, f"Function '{fname}' not allowed")
            continue

        if isinstance(node, ALLOWED_NODES):
            continue

        if isinstance(node, (exp.Expression, exp.Tuple)):
            continue

        raise HTTPException(400, f"Unsupported SQL expression: {type(node).__name__}")


def _ensure_boolean(expr: ColumnElement) -> ColumnElement:
    # Placeholder for future type checks/casts if needed
    return expr


def _ast_to_sqla(expr: exp.Expression, table: Table, depth: int = 0) -> ColumnElement:
    indent = "  " * depth
    logger.debug("%s[enter] %s (%s)", indent, expr, type(expr).__name__)

    # WHERE wrapper
    if isinstance(expr, exp.Where):
        logger.debug("%sUnwrapping WHERE node", indent)
        return _ast_to_sqla(expr.this, table, depth + 1)

    # Parentheses
    if isinstance(expr, exp.Paren):
        return _ast_to_sqla(expr.this, table, depth + 1)

    # Boolean AND / OR
    if isinstance(expr, exp.And):
        left_node = _require_expr(expr.args.get("this"))
        right_node = _require_expr(expr.args.get("expression"))

        left = _ensure_boolean(_ast_to_sqla(left_node, table, depth + 1))
        right = _ensure_boolean(_ast_to_sqla(right_node, table, depth + 1))
        result = and_(left, right)
        logger.debug("%s[return] AND → %s", indent, result)
        return result

    if isinstance(expr, exp.Or):
        left_node = _require_expr(expr.args.get("this"))
        right_node = _require_expr(expr.args.get("expression"))

        left = _ensure_boolean(_ast_to_sqla(left_node, table, depth + 1))
        right = _ensure_boolean(_ast_to_sqla(right_node, table, depth + 1))
        result = or_(left, right)
        logger.debug("%s[return] OR → %s", indent, result)
        return result

    # NOT
    if isinstance(expr, exp.Not):
        inner = _ensure_boolean(_ast_to_sqla(expr.this, table, depth + 1))
        result = not_(inner)
        logger.debug("%s[return] NOT → %s", indent, result)
        return result

    # Functions (AFTER boolean logic)
    if isinstance(expr, exp.Func):
        name = (expr.name or "").lower()
        logger.debug(
            "%sFunc node: name=%r, expressions=%r, args=%r",
            indent,
            name,
            expr.expressions,
            expr.args,
        )

        if not name:
            inner = expr.this or (expr.expressions[0] if expr.expressions else None)
            logger.debug("%sFunc with empty name, unwrapping to %s", indent, inner)
            if inner is None:
                raise HTTPException(400, "Empty function node with no inner expression")
            result = _ast_to_sqla(inner, table, depth + 1)
            logger.debug("%s[return] Func(empty) → %s", indent, result)
            return result

        if name not in ALLOWED_FUNCTIONS:
            raise HTTPException(400, f"Function '{name}' not allowed")

        args = [_ast_to_sqla(arg, table, depth + 1) for arg in expr.expressions]
        result = getattr(func, name)(*args)
        logger.debug("%s[return] Func %s → %s", indent, name, result)
        return result

    # Literals
    if isinstance(expr, exp.Literal):
        val = _convert_literal(expr)
        sa_val = literal(val)
        logger.debug("%s[return] Literal %r → %s", indent, val, sa_val)
        return sa_val

    # Columns
    if isinstance(expr, exp.Column):
        col = expr.name
        if col not in table.c:
            logger.error("%sUnknown column referenced in filter: %s", indent, col)
            raise HTTPException(400, f"Unknown column '{col}' in filter expression")

        result = table.c[col]
        logger.debug("%s[return] Column %s → table.c[%s]", indent, col, col)
        return result

    # Unary minus
    if isinstance(expr, exp.Neg):
        inner = expr.this
        logger.debug(
            "%sHandling Neg: inner=%s (%s)", indent, inner, type(inner).__name__
        )
        inner_sa = _ast_to_sqla(inner, table, depth + 1)
        result = -inner_sa
        logger.debug("%s[return] Neg → %s", indent, result)
        return result

    # Comparison operators
    for op_type, builder in OP_MAP.items():
        if isinstance(expr, op_type):
            logger.debug(
                "%sComparison node %s args=%r", indent, type(expr).__name__, expr.args
            )
            left_node = _require_expr(expr.args.get("this"))
            right_node = _require_expr(
                expr.args.get("right") or expr.args.get("expression")
            )
            left = _ast_to_sqla(left_node, table, depth + 1)
            right = _ast_to_sqla(right_node, table, depth + 1)
            result = builder(left, right)
            logger.debug("%s[return] %s → %s", indent, type(expr).__name__, result)
            return result

    logger.error(
        "%sUNSUPPORTED NODE %s (%s), args=%r",
        indent,
        expr,
        type(expr).__name__,
        expr.args,
    )
    raise HTTPException(400, f"Unsupported expression: {expr}")


def parse_sql_filter(
    filter_str: Optional[str], table: Table
) -> ColumnElement[bool] | None:
    """Parse and validate a SQL-like WHERE filter string into a SQLAlchemy expression."""
    if filter_str is None or filter_str.strip() == "":
        return None

    raw = filter_str.strip()
    logger.debug("Received SQL filter: %s", raw)

    if has_unquoted_semicolon(raw):
        raise HTTPException(400, "Invalid SQL filter: semicolons not allowed.")

    try:
        wrapped = f"SELECT * FROM t WHERE {raw}"
        stmt = sqlglot.parse_one(wrapped)
        expr = stmt.args.get("where")
        if expr is None:
            raise HTTPException(400, "Invalid filter expression")
    except Exception as exc:
        raise HTTPException(400, f"Invalid SQL filter: {exc}")

    logger.debug("Parsed SQL filter AST: %s", expr)
    logger.debug("AST dump:\n%s", expr.dump())

    _validate_ast(expr)
    return _ast_to_sqla(expr, table)
