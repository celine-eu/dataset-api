from __future__ import annotations

import json
import logging
import re
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import MetaData, Table, and_, func, literal, or_, select, not_
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.elements import ColumnElement

import sqlglot
from sqlglot import exp
from geoalchemy2 import Geometry

from dataset.catalogue.db import get_session
from dataset.catalogue.models import DatasetEntry
from dataset.security.auth import get_optional_user
from dataset.security.opa import authorize_dataset_query

logger = logging.getLogger(__name__)
router = APIRouter()


# ==============================================================================
# Request model
# ==============================================================================


class DatasetQueryModel(BaseModel):
    filter: Optional[str] = None
    limit: int = 100
    offset: int = 0


# ==============================================================================
# SQL-glot configuration & guards
# ==============================================================================

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


# ==============================================================================
# Semicolon / SQL injection guard
# ==============================================================================


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


# ==============================================================================
# AST helpers
# ==============================================================================


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
            except Exception:
                pass
        return s

    if lit.is_int:
        try:
            return int(token)
        except:
            pass

    if lit.is_number:
        try:
            return float(token)
        except:
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
    return expr


def _extract_operands(expr: exp.Expression, cls):
    """
    Recursively flatten logical expressions for AND/OR.
    sqlglot has several inconsistent AST shapes depending on dialect.
    """
    ops = []

    # Standard children
    if expr.this is not None:
        if isinstance(expr.this, cls):
            ops.extend(_extract_operands(expr.this, cls))
        else:
            ops.append(expr.this)

    if expr.expression is not None:
        if isinstance(expr.expression, cls):
            ops.extend(_extract_operands(expr.expression, cls))
        else:
            ops.append(expr.expression)

    # Extra children frequently used in nested logical chains
    for child in expr.args.get("expressions", []):
        if isinstance(child, cls):
            ops.extend(_extract_operands(child, cls))
        else:
            ops.append(child)

    return ops


def _all_children(expr: exp.Expression):
    """
    Extract ONLY real expression children for boolean nodes.
    Skip sqlglot metadata keys like 'i', 'k', 'm', 'comments'.
    """
    children: list[exp.Expression] = []

    # 1) Standard attributes
    for attr in ("this", "expression"):
        node = getattr(expr, attr, None)
        if isinstance(node, exp.Expression):
            children.append(node)

    # 2) Args – but skip metadata
    for key, val in expr.args.items():
        if key in ("i", "k", "m", "comments"):
            continue

        if isinstance(val, exp.Expression):
            children.append(val)
        elif isinstance(val, list):
            for v in val:
                if isinstance(v, exp.Expression):
                    children.append(v)

    logger.debug(
        "  _all_children(%s) → %d children: %s",
        type(expr).__name__,
        len(children),
        [f"{type(c).__name__}:{c}" for c in children],
    )

    return children


def _ast_to_sqla(expr: exp.Expression, table: Table, depth: int = 0) -> ColumnElement:
    indent = "  " * depth
    logger.debug("%s[enter] %s (%s)", indent, expr, type(expr).__name__)

    # ------------------------------------------------------------------
    # WHERE wrapper
    # ------------------------------------------------------------------
    if isinstance(expr, exp.Where):
        logger.debug("%sUnwrapping WHERE node", indent)
        return _ast_to_sqla(expr.this, table, depth + 1)

    # ------------------------------------------------------------------
    # Parentheses
    # ------------------------------------------------------------------
    if isinstance(expr, exp.Paren):
        return _ast_to_sqla(expr.this, table, depth + 1)

    # ------------------------------------------------------------------
    # Boolean AND / OR — must be BEFORE Func
    # ------------------------------------------------------------------
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

    # ------------------------------------------------------------------
    # NOT
    # ------------------------------------------------------------------
    if isinstance(expr, exp.Not):
        inner = _ensure_boolean(_ast_to_sqla(expr.this, table, depth + 1))
        result = not_(inner)
        logger.debug("%s[return] NOT → %s", indent, result)
        return result

    # ------------------------------------------------------------------
    # Functions (generic) — AFTER AND/OR/NOT
    # ------------------------------------------------------------------
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
            # This was accidentally catching AND/OR before; now it should only
            # see truly anonymous function-like stuff. Be conservative:
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

    # ------------------------------------------------------------------
    # Literals
    # ------------------------------------------------------------------
    if isinstance(expr, exp.Literal):
        val = _convert_literal(expr)
        sa_val = literal(val)
        logger.debug("%s[return] Literal %r → %s", indent, val, sa_val)
        return sa_val

    # ------------------------------------------------------------------
    # Columns
    # ------------------------------------------------------------------
    if isinstance(expr, exp.Column):
        col = expr.name
        if col not in table.c:
            logger.error("%sUnknown column referenced in filter: %s", indent, col)
            raise HTTPException(400, f"Unknown column '{col}' in filter expression")

        result = table.c[col]
        logger.debug("%s[return] Column %s → table.c[%s]", indent, col, col)
        return result

    # ------------------------------------------------------------------
    # Unary minus, e.g. -5 (sqlglot emits Neg)
    # ------------------------------------------------------------------
    if isinstance(expr, exp.Neg):
        inner = expr.this
        logger.debug(
            "%sHandling Neg: inner=%s (%s)", indent, inner, type(inner).__name__
        )
        inner_sa = _ast_to_sqla(inner, table, depth + 1)
        result = -inner_sa
        logger.debug("%s[return] Neg → %s", indent, result)
        return result

    # ------------------------------------------------------------------
    # Comparison operators (EQ, GT, etc.)
    # ------------------------------------------------------------------
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


# ==============================================================================
# Table reflection
# ==============================================================================


async def reflect_table_async(db: AsyncSession, table_name: str) -> Table:
    metadata = MetaData()

    if "." in table_name:
        schema, tbl = table_name.split(".", 1)
    else:
        schema, tbl = None, table_name

    def _reflect(sync_conn):
        metadata.reflect(bind=sync_conn, only=[tbl], schema=schema)

    conn = await db.connection()
    await conn.run_sync(_reflect)

    key = f"{schema}.{tbl}" if schema else tbl
    table = metadata.tables.get(key)
    if table is None:
        raise HTTPException(500, f"Table '{table_name}' not found")

    for col in table.columns:
        if (
            getattr(col.type, "datatype", None) == "geometry"
            or col.type.__class__.__name__.lower() == "geometry"
        ):
            col.type = Geometry(geometry_type="GEOMETRY", srid=4326)

    logger.debug("Successfully reflected table %s", key)
    return table


# ==============================================================================
# Dataset lookup
# ==============================================================================


async def _get_entry(dataset_id: str, db: AsyncSession):
    stmt = (
        select(DatasetEntry)
        .where(DatasetEntry.dataset_id == dataset_id)
        .where(DatasetEntry.expose.is_(True))
    )
    res = await db.execute(stmt)
    entry = res.scalars().first()
    if not entry:
        raise HTTPException(404, "Dataset not found")
    return entry


# ==============================================================================
# Query handler
# ==============================================================================


@router.post("/{dataset_id}/query")
async def query_dataset_post(
    dataset_id: str,
    body: DatasetQueryModel,
    db: AsyncSession = Depends(get_session),
    user: Optional[dict] = Depends(get_optional_user),
):

    entry = await _get_entry(dataset_id, db)

    if entry.backend_type != "postgres":
        raise HTTPException(400, "Querying only supported for postgres backend")

    table_name = entry.backend_config.get("table") if entry.backend_config else None
    if not table_name:
        raise HTTPException(500, "Dataset missing backend table definition")

    table = await reflect_table_async(db, table_name)

    # OPA check
    allowed = await authorize_dataset_query(
        entry=entry, user=user, raw_filter=body.filter
    )
    if not allowed:
        raise HTTPException(403, "Not authorized to query this dataset.")

    sa_filter = parse_sql_filter(body.filter, table) if body.filter else None

    stmt = select(table)
    if sa_filter is not None:
        stmt = stmt.where(sa_filter)

    stmt = stmt.limit(body.limit).offset(body.offset)

    logger.debug(
        "Rendered SQL for dataset %s:\n%s",
        dataset_id,
        stmt.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        ),
    )

    try:
        result = await db.execute(stmt)
        rows = result.mappings().all()
    except Exception as exc:
        raise HTTPException(500, f"Query execution failed: {exc}")

    items = []
    for r in rows:
        row = dict(r)
        for col, val in list(row.items()):
            if val is None:
                continue
            if hasattr(val, "__geo_interface__"):
                row[col] = val.__geo_interface__
            elif val.__class__.__name__ == "WKBElement":
                geojson = await db.scalar(select(func.ST_AsGeoJSON(val)))
                if geojson:
                    row[col] = json.loads(geojson)
        items.append(row)

    return {
        "@context": "https://example.org/dataset-query",
        "dataset": dataset_id,
        "limit": body.limit,
        "offset": body.offset,
        "items": items,
    }


# ==============================================================================
# GET wrapper
# ==============================================================================


@router.get("/{dataset_id}/query")
async def query_dataset_get(
    dataset_id: str,
    filter: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
    db: AsyncSession = Depends(get_session),
    user: Optional[dict] = Depends(get_optional_user),
):
    body = DatasetQueryModel(filter=filter, limit=limit, offset=offset)
    return await query_dataset_post(dataset_id, body, db, user)
