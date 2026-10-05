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
from sqlglot.optimizer.scope import traverse_scope

from celine.dataset.api.dataset_query.log_safety import sql_shape
from celine.dataset.security.audit import Refused

logger = logging.getLogger(__name__)

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
    exp.Dot,
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
    exp.Tuple,
    exp.Between,
    exp.Is,
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
    exp.ArrayAgg,
    exp.Filter,
    # Ordered-set aggregates (PERCENTILE_CONT(...) WITHIN GROUP (ORDER BY ...)).
    # The aggregate itself is still checked against ALLOWED_FUNCTIONS.
    exp.WithinGroup,
    # Window functions. OVER adds partitioning and ordering, not reach: the
    # windowed function is still checked on its own.
    exp.Window,
    exp.WindowSpec,
    # --- Conditional expressions ---
    exp.Case,
    exp.If,
    # --- Constant rows (a VALUES list in a CTE); reads nothing ---
    exp.Values,
    # --- Subqueries (allowed for now) ---
    exp.Subquery,
    # --- Arithmetic ---
    exp.Add,
    exp.Sub,
    exp.Mul,
    exp.Div,
    # --- Date/time ---
    exp.Interval,
    exp.Var,
    exp.TsOrDsToTimestamp,
    exp.CurrentDate,
    exp.CurrentDatetime,
    exp.CurrentTimestamp,
    exp.DateAdd,
    exp.DateSub,
    # --- Type casting ---
    exp.Cast,
    exp.DataType,
    exp.DataTypeParam,
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

# Matched against a function's SQL names whether sqlglot parses it as a typed
# node (`exp.Coalesce`) or as `exp.Anonymous`. Hash and crypto functions are
# deliberately absent: no read path needs them, and they are the building block
# for fingerprinting values.
ALLOWED_FUNCTIONS = {
    # PostGIS
    "st_intersects",
    "st_within",
    "st_contains",
    "st_transform",
    "st_distance",
    "st_setsrid",
    "st_geomfromgeojson",
    "st_point",
    "st_xmin",
    "st_ymin",
    "st_xmax",
    "st_ymax",
    "st_extent",
    # QE-01 (docs/query-engine.md): a selected shape as GeoJSON, simplified in
    # the statement so a detailed boundary polygon leaves the database at a
    # bounded size. Both read only the row's own geometry.
    "st_asgeojson",
    "st_simplify",
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
    "greatest",
    "least",
    # aggregates
    "bool_or",
    "bool_and",
    "percentile_cont",
    "percentile_disc",
    # window ranking
    "row_number",
    "rank",
    "dense_rank",
    # date
    "current_date",
    "current_timestamp",
    "date",
    "date_trunc",
    "extract",
    "now",
}

# Reject statement stacking
_SEMICOLON_RE = re.compile(r";\s*\S")


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
        return self.ast.sql(dialect="postgres")

    def to_sql(self, tables_map: Optional[Dict[str, str]] = None) -> str:
        """
        Render SQL from the AST.

        If table_map is provided, logical table names (dataset IDs)
        are replaced with their physical backend table names.

        table_map: {logical_name -> physical_table}
        """
        return self.to_ast(tables_map).sql(dialect="postgres")

    def to_ast(self, tables_map: Optional[Dict[str, str]] = None) -> exp.Expression:
        """
        A copy of the AST with physical table names substituted.

        Row filters are applied to this rather than to a re-parse of `to_sql`.
        A round trip through text changes the dialect (`INTERVAL '30 minutes'`
        comes back as `INTERVAL '30' MINUTES`, which PostgreSQL refuses) and the
        table shape (`schema.table` splits into db and name, so a plan keyed on
        the physical name stops matching and its predicate is silently dropped).
        """
        # Work on a copy to keep ParsedSQL immutable
        ast = self.ast.copy()
        if not tables_map:
            return ast

        ctes = _cte_reference_ids(ast)
        for table in ast.find_all(exp.Table):
            # A CTE reference is not a dataset, whatever it is called.
            if id(table) in ctes:
                continue
            logical = _table_identifier(table)

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
            table.set("db", None)
            table.set("catalog", None)

        return ast


# -----------------------------------------------------------------------------
# Errors
# -----------------------------------------------------------------------------


#: The audit reason of a statement the guard refuses on what it would do — a
#: refused statement kind, function, construct or operation (GS-02).
SQL_REFUSED = "sql_refused"


def _bad_request(
    message: str, *, log_message: str | None = None, refusal: str | None = None
) -> HTTPException:
    """A 400 for the caller. `log_message` replaces `message` in the log when the
    detail quotes the caller's SQL: the caller may read their own literals back,
    a log may not (QE-03). `refusal` names the audit reason when the 400 refuses
    what the statement would reach or do, rather than how it is written (GS-02)."""
    logger.warning("SQL validation error: %s", log_message or message)
    if refusal is not None:
        return Refused(400, message, reason=refusal)
    return HTTPException(status_code=400, detail=message)


def _parse_error_position(exc: ParseError) -> str:
    """Where sqlglot stopped, without the context it quotes from the statement."""
    parts = []
    for err in getattr(exc, "errors", None) or []:
        parts.append(
            f"{err.get('description')} (line {err.get('line')}, col {err.get('col')})"
        )
    return "; ".join(parts) or type(exc).__name__


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

    if not sql or not sql.strip():
        raise _bad_request("Empty SQL query")

    _reject_statement_stacking(sql)

    # Reject comments
    if re.search(r"--|/\*", sql):
        raise _bad_request("SQL comments are not allowed", refusal=SQL_REFUSED)

    try:
        ast = sqlglot.parse_one(sql)
    except sqlglot.errors.ParseError as exc:
        # sqlglot's message quotes the statement around the error, literals and
        # all: the caller gets it back, the log gets the position only (QE-03).
        raise _bad_request(
            f"Invalid SQL syntax: {exc}",
            log_message=f"Invalid SQL syntax: {_parse_error_position(exc)}",
        ) from exc

    select = ast.find(exp.Select)
    if not select or not select.expressions:
        raise _bad_request("Query must have at least a SELECT")

    _reject_top_level_pagination(ast)
    _check_ast_depth(ast)

    for node in ast.walk():

        # Alert on tautologies eg 1=1
        # Standalone (WHERE 1=1) is a common query-builder pattern — warn only.
        # Inside OR it can unconditionally satisfy any predicate (injection bypass) — reject.
        if isinstance(node, exp.EQ):
            left_sql = node.left.sql()
            right_sql = node.right.sql()
            if left_sql == right_sql:
                ancestor = node.parent
                while ancestor is not None:
                    if isinstance(ancestor, exp.Or):
                        raise _bad_request(
                            f"Tautological predicate in OR context is not allowed: {left_sql} = {right_sql}",
                            log_message="Tautological predicate in OR context is not "
                            f"allowed: {sql_shape(node)}",
                            refusal=SQL_REFUSED,
                        )
                    ancestor = ancestor.parent
                logger.warning(
                    "Tautological predicate detected in query: %s", sql_shape(node)
                )

        # --- Allowlisted functions ---
        if isinstance(node, exp.Anonymous):
            fn_name = node.name.lower()

            if fn_name not in ALLOWED_FUNCTIONS:
                raise _bad_request(
                    f"SQL function not allowed: {node.name}", refusal=SQL_REFUSED
                )
            continue

        if isinstance(node, ALLOWED_EXPRESSIONS):
            continue

        # QE-02 (docs/query-engine.md): a unary minus is admitted on a numeric
        # literal and on nothing else, so `ST_Point(-0.5, 1)` parses. `-col`,
        # `-(…)`, `- -1`, `-'1'` and `-1::int` (a minus over a cast) stay refused:
        # no reader needs them, and none of them is a constant number.
        if isinstance(node, exp.Neg):
            operand = node.this
            if isinstance(operand, exp.Literal) and not operand.is_string:
                continue
            raise _bad_request(
                "Unary minus is allowed only on a numeric literal", refusal=SQL_REFUSED
            )

        # sqlglot parses every function it knows into a typed node, so the
        # allowlist has to be consulted here too, or it never applies to the
        # functions it names. Any of the class's SQL names counts: `IFNULL` is
        # parsed as `exp.Coalesce`.
        if isinstance(node, exp.Func):
            if ALLOWED_FUNCTIONS.isdisjoint(n.lower() for n in type(node).sql_names()):
                raise _bad_request(
                    f"SQL function not allowed: {node.sql_name()}", refusal=SQL_REFUSED
                )
            continue

        if isinstance(node, FORBIDDEN_EXPRESSIONS):
            raise _bad_request(
                f"SQL construct not allowed: {node.__class__.__name__}",
                refusal=SQL_REFUSED,
            )

        raise _bad_request(
            f"Unsupported SQL construct: {node.__class__.__name__}", refusal=SQL_REFUSED
        )

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
        logger.warning("Invalid SQL syntax: %s", _parse_error_position(exc))
        raise HTTPException(
            status_code=400,
            detail="Invalid SQL syntax",
        ) from None

    except Exception as exc:
        # absolute safety net. The type only, no message and no traceback (QE-03):
        # sqlglot's TokenError (an unterminated literal, say) is no ParseError and
        # its text quotes the statement, literals and all.
        logger.error("Unexpected SQL parser error: %s", type(exc).__name__)
        raise HTTPException(
            status_code=400,
            detail="Invalid SQL query",
        ) from None


# -----------------------------------------------------------------------------
# Validation helpers
# -----------------------------------------------------------------------------


def _check_ast_depth(ast: exp.Expression, depth_limit=200):
    def _max_depth(node: exp.Expression) -> int:
        child_depths = [
            _max_depth(child)
            for child in node.iter_expressions()
        ]
        return 1 + max(child_depths) if child_depths else 1

    ast_depth = _max_depth(ast)
    if ast_depth > depth_limit:
        raise _bad_request(f"Query too complex, max depth limit is {depth_limit}")


def _reject_top_level_pagination(ast: exp.Expression) -> None:
    if isinstance(ast, exp.Select):
        if ast.args.get("limit") is not None:
            raise _bad_request("LIMIT not allowed in top-level query")
        if ast.args.get("offset") is not None:
            raise _bad_request("OFFSET not allowed in top-level query")


def _cte_reference_ids(ast: exp.Expression) -> Set[int]:
    """The `exp.Table` nodes (by `id`) that name a CTE **visible where they stand**.

    Resolved per scope, the way PostgreSQL resolves them, not by name alone. A
    name match is not enough: a non-recursive CTE's body cannot see the CTE
    itself, a CTE defined in a subquery is invisible outside it, and a later CTE
    is invisible to an earlier one. In each of those places the name is a
    *physical* table, and it has to reach the dataset gate as one (QE-05).

    Only an unqualified name can be a CTE reference. Fails closed: a query whose
    scopes cannot be resolved is refused.
    """
    try:
        scopes = traverse_scope(ast)
    except Exception as exc:  # noqa: BLE001 — unresolvable scopes are refused
        raise _bad_request(
            "Query structure could not be resolved",
            log_message=f"scope resolution failed: {type(exc).__name__}",
            refusal="scope_unresolved",
        ) from exc
    refs: Set[int] = set()
    for scope in scopes:
        for table in scope.tables:
            if table.args.get("db") or table.args.get("catalog"):
                continue
            if table.name in scope.cte_sources:
                refs.add(id(table))
    return refs


def _collect_physical_tables(ast: exp.Expression) -> Set[str]:
    """
    Collect physical table names referenced by the query.

    Every table reference counts unless scope resolution proves it names a
    visible CTE (`_cte_reference_ids`) — joins, subqueries, nested selects and
    CTE bodies included. What is collected must then resolve to a catalogue
    dataset, or the query is refused (`resolve_datasets_for_tables`).
    """
    ctes = _cte_reference_ids(ast)
    return {
        _table_identifier(table)
        for table in ast.find_all(exp.Table)
        if id(table) not in ctes
    }


def _reject_statement_stacking(sql: str) -> None:
    """
    Reject multiple SQL statements.
    """
    stripped = sql.strip().rstrip(";").strip()
    if _SEMICOLON_RE.search(stripped):
        raise _bad_request("Multiple SQL statements are not allowed", refusal=SQL_REFUSED)


def _validate_root(ast: exp.Expression) -> None:
    """
    Ensure query is SELECT / UNION at top level.
    """
    if not isinstance(ast, _ALLOWED_ROOT_EXPRESSIONS):
        raise _bad_request(
            f"Only SELECT statements are allowed (got {type(ast).__name__})",
            refusal=SQL_REFUSED,
        )


def _reject_disallowed_nodes(ast: exp.Expression) -> None:
    """
    Walk AST and reject write / command operations.
    """
    for node in ast.walk():
        if isinstance(node, _DISALLOWED_EXPRESSIONS):
            raise _bad_request(
                f"Disallowed SQL operation: {type(node).__name__}", refusal=SQL_REFUSED
            )


def _table_identifier(table: exp.Table) -> str:
    parts = []
    if table.catalog:
        parts.append(table.catalog)
    if table.db:
        parts.append(table.db)
    parts.append(table.name)
    return ".".join(parts)
