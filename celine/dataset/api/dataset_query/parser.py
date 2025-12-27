from __future__ import annotations

import logging
import re
from typing import Dict, Optional, Set
from fastapi import HTTPException
from sqlalchemy import Table
from dataclasses import dataclass
import sqlglot
from sqlglot import ParseError, exp
import sqlglot.errors

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ParsedSQL:
    ast: exp.Expression
    tables: Set[str]  # physical tables only, CTEs excluded

    @property
    def sql(self) -> str:
        """
        Logical SQL rendered from the validated AST.
        Table names are dataset IDs (no physical substitution).
        """
        return self.ast.sql()

    def to_sql(self, tables_map: Optional[Dict[str, str]] = None) -> str:
        """
        Render SQL from the AST.

        If table_map is provided, logical table names (dataset IDs)
        are replaced with their physical backend table names.

        table_map: {logical_name -> physical_table}
        """
        if not tables_map:
            return self.ast.sql()

        # Work on a copy to keep ParsedSQL immutable
        ast = self.ast.copy()

        for table in ast.find_all(exp.Table):
            logical = table.name

            if logical not in tables_map:
                logger.debug(
                    f"Logical table name '{logical}' not found in physical tables mapping"
                )
                continue

            physical = tables_map[logical]

            logger.debug(f"Mapping table {logical} -> {physical}")

            table.set(
                "this",
                exp.Identifier(this=physical, quoted=False),
            )

        return ast.sql()


# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

# Allowed top-level query forms
_ALLOWED_ROOT_EXPRESSIONS = (
    exp.Select,
    exp.Union,
)

ALLOWED_EXPRESSIONS = (
    # --- Core query structure ---
    exp.Select,
    exp.From,
    exp.With,
    exp.CTE,
    exp.Table,
    exp.TableAlias,
    # --- Projection ---
    exp.Star,
    exp.Column,
    exp.Identifier,
    exp.Alias,
    exp.Distinct,
    # --- Joins ---
    exp.Join,
    # --- Filtering / logic ---
    exp.Where,
    exp.And,
    exp.Or,
    exp.Not,
    exp.Paren,
    exp.Max,
    exp.Min,
    exp.Avg,
    exp.Sum,
    exp.Count,
    # --- Comparisons ---
    exp.EQ,
    exp.NEQ,
    exp.GT,
    exp.GTE,
    exp.LT,
    exp.LTE,
    exp.In,
    exp.Between,
    # --- Literals ---
    exp.Literal,
    exp.Boolean,
    exp.Null,
    # --- Ordering / pagination ---
    exp.Order,
    exp.Ordered,
    exp.Limit,
    exp.Offset,
    # --- Aggregation (safe) ---
    exp.Group,
    exp.Having,
    # --- Subqueries (allowed for now) ---
    exp.Subquery,
)

# Hard-disallowed statement types
_DISALLOWED_EXPRESSIONS = (
    exp.Insert,
    exp.Update,
    exp.Delete,
    exp.Merge,
    exp.Create,
    exp.Drop,
    exp.Alter,
    exp.TruncateTable,
    exp.Command,  # catches EXEC, CALL, COPY, etc.
)

FORBIDDEN_EXPRESSIONS = (
    # Functions = server capability surface
    exp.Func,
    # Set operations
    exp.Union,
    exp.Intersect,
    exp.Except,
    # DDL / DML (belt & suspenders)
    exp.Insert,
    exp.Update,
    exp.Delete,
    exp.Drop,
    exp.Create,
    exp.Alter,
    # Comments / hints
    exp.Comment,
)

ALLOWED_FUNCTIONS = {
    # PostGIS
    "st_intersects",
    "st_within",
    "st_contains",
    "st_distance",
    "st_setsrid",
    "st_geomfromgeojson",
    "st_point",
    # string
    "lower",
    "upper",
    "length",
    "trim",
    "ltrim",
    "rtrim",
    "substring",
    "replace",
    # numeric
    "abs",
    "round",
    "ceil",
    "floor",
    # comparison
    "coalesce",
    "nullif",
    # date
    "current_date",
    "current_timestamp",
    "date",
    "date_trunc",
    "extract",
}

# Reject statement stacking
_SEMICOLON_RE = re.compile(r";")

# -----------------------------------------------------------------------------
# Errors
# -----------------------------------------------------------------------------


def _bad_request(message: str) -> HTTPException:
    logger.warning("SQL validation error: %s", message)
    return HTTPException(status_code=400, detail=message)


def _parse_sql_query_impl(sql: str) -> ParsedSQL:
    """
    Validate a raw SQL query and return a safe SQL string.

    Guarantees:
    - SELECT-only
    - No statement stacking
    - No schema-qualified access
    - Only explicitly allowed physical tables
    - CTEs and subqueries allowed
    """
    logger.debug(f"Starting SQL validation RAW={sql}")

    if not sql or not sql.strip():
        raise _bad_request("Empty SQL query")

    _reject_statement_stacking(sql)

    # Reject comments
    if re.search(r"--|/\*", sql):
        raise _bad_request("SQL comments are not allowed")

    try:
        ast = sqlglot.parse_one(sql)
    except sqlglot.errors.ParseError as exc:
        logger.error(f"SQL parse error: {exc}")
        raise _bad_request(f"Invalid SQL syntax: {exc}") from exc

    select = ast.find(exp.Select)
    if not select or not select.expressions:
        raise _bad_request("Query must have at least a SELECT")

    _reject_top_level_pagination(ast)
    _check_ast_depth(ast)

    for node in ast.walk():

        # Reject tautologies eg 1=1
        if isinstance(node, exp.EQ):
            if node.left.sql() == node.right.sql():
                raise _bad_request("Tautological predicates are not allowed")

        # --- Allowlisted functions ---
        if isinstance(node, exp.Anonymous):
            fn_name = node.name.lower()

            if fn_name not in ALLOWED_FUNCTIONS:
                raise HTTPException(
                    400,
                    f"SQL function not allowed: {node.name}",
                )
            continue

        if isinstance(node, ALLOWED_EXPRESSIONS):
            continue

        if isinstance(node, FORBIDDEN_EXPRESSIONS):
            raise _bad_request(f"SQL construct not allowed: {node.__class__.__name__}")

        raise _bad_request(f"Unsupported SQL construct: {node.__class__.__name__}")

    _validate_root(ast)
    _reject_disallowed_nodes(ast)

    tables = _collect_physical_tables(ast)

    return ParsedSQL(tables=tables, ast=ast)


# -----------------------------------------------------------------------------
# Public API
# -----------------------------------------------------------------------------


def parse_sql_query(sql: str) -> ParsedSQL:
    try:
        # all validation + parsing happens inside
        return _parse_sql_query_impl(sql)

    except HTTPException:
        # already normalized → rethrow
        raise

    except ParseError as exc:
        logger.warning("Invalid SQL syntax: %s", exc)
        raise HTTPException(
            status_code=400,
            detail="Invalid SQL syntax",
        ) from None

    except Exception as exc:
        # absolute safety net
        logger.exception("Unexpected SQL parser error")
        raise HTTPException(
            status_code=400,
            detail="Invalid SQL query",
        ) from None


# -----------------------------------------------------------------------------
# Validation helpers
# -----------------------------------------------------------------------------


def _check_ast_depth(ast: exp.Expression, depth_limit=50):
    ast_depth = max(len(list(node.walk())) for node in ast.walk())
    if ast_depth > depth_limit:
        raise _bad_request(f"Query too complex, max depth limit is {depth_limit}")


def _reject_top_level_pagination(ast: exp.Expression) -> None:
    if isinstance(ast, exp.Select):
        if ast.args.get("limit") is not None:
            raise _bad_request("LIMIT not allowed in top-level query")
        if ast.args.get("offset") is not None:
            raise _bad_request("OFFSET not allowed in top-level query")


def _collect_physical_tables(ast: exp.Expression) -> Set[str]:
    """
    Collect physical table names referenced by the query.

    - Excludes CTE names
    - Includes tables in joins, subqueries, nested selects
    """

    # 1. Collect CTE names
    cte_names: Set[str] = set()

    for with_expr in ast.find_all(exp.With):
        for cte in with_expr.expressions:
            # cte.alias is the exposed name
            cte_names.add(cte.alias)

    # 2. Collect all table references
    tables: Set[str] = set()

    for table in ast.find_all(exp.Table):
        name = table.name

        # Skip references to CTEs
        if name in cte_names:
            continue

        tables.add(name)

    return tables


def _reject_statement_stacking(sql: str) -> None:
    """
    Reject multiple SQL statements.
    """
    if _SEMICOLON_RE.search(sql):
        raise _bad_request("Multiple SQL statements are not allowed")


def _validate_root(ast: exp.Expression) -> None:
    """
    Ensure query is SELECT / UNION at top level.
    """
    if not isinstance(ast, _ALLOWED_ROOT_EXPRESSIONS):
        raise _bad_request(
            f"Only SELECT statements are allowed (got {type(ast).__name__})"
        )


def _reject_disallowed_nodes(ast: exp.Expression) -> None:
    """
    Walk AST and reject write / command operations.
    """
    for node in ast.walk():
        if isinstance(node, _DISALLOWED_EXPRESSIONS):
            raise _bad_request(f"Disallowed SQL operation: {type(node).__name__}")
