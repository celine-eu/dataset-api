# src/celine/dataset/api/dataset_query/user_filter.py
"""
User-based row filtering for dataset queries.

When a dataset has `userFilterColumn` defined in its governance metadata,
queries are automatically filtered to only return rows belonging to the
authenticated user (based on their JWT `sub` claim).

Admins (users in the "admins" group) bypass this filtering.
"""
from __future__ import annotations

import logging
from typing import List, Optional

import sqlglot
from sqlglot import exp

from celine.dataset.db.models.dataset_entry import DatasetEntry
from celine.dataset.security.models import AuthenticatedUser

logger = logging.getLogger(__name__)

# Groups that bypass user filtering
ADMIN_GROUPS = {"admins"}


def get_user_filter_column(entry: DatasetEntry) -> Optional[str]:
    """
    Get the user filter column from dataset governance metadata.

    Returns the column name if userFilterColumn is defined, None otherwise.

    Governance metadata location:
        lineage.facets.governance.userFilterColumn
    """
    if not entry.lineage:
        return None

    facets = entry.lineage.get("facets", {})
    governance = facets.get("governance", {})

    # Support both camelCase and snake_case
    filter_column = governance.get("userFilterColumn") or governance.get(
        "user_filter_column"
    )

    if filter_column:
        logger.debug(
            f"Dataset {entry.dataset_id} has user filter column: {filter_column}"
        )

    return filter_column


def is_admin_user(user: Optional[AuthenticatedUser]) -> bool:
    """
    Check if user is an admin and should bypass user filtering.
    """
    if user is None:
        return False

    # Check groups from claims
    groups = user.claims.get("groups", [])
    if not isinstance(groups, list):
        groups = []

    return bool(ADMIN_GROUPS & set(groups))


def inject_user_filter(sql: str, filters: List[dict]) -> str:
    """
    Inject WHERE conditions to filter by user.

    Args:
        sql: The SQL query string
        filters: List of filter definitions:
            [
                {
                    "table": "schema.table_name",
                    "column": "sub",
                    "user_sub": "user-uuid-here"
                },
                ...
            ]

    Returns:
        Modified SQL with user filters applied

    Example:
        Input:  SELECT * FROM schema.meters_data
        Filter: {"table": "schema.meters_data", "column": "sub", "user_sub": "abc123"}
        Output: SELECT * FROM schema.meters_data WHERE sub = 'abc123'
    """
    if not filters:
        return sql

    try:
        ast = sqlglot.parse_one(sql)
    except Exception as e:
        logger.error(f"Failed to parse SQL for user filter injection: {e}")
        return sql

    # Build filter conditions
    for filter_def in filters:
        table_name = filter_def["table"]
        column_name = filter_def["column"]
        user_sub = filter_def["user_sub"]

        # Create the condition: column = 'user_sub'
        # We use a literal string with proper escaping
        condition = exp.EQ(
            this=exp.Column(this=exp.Identifier(this=column_name)),
            expression=exp.Literal.string(user_sub),
        )

        # Find the SELECT and add/extend WHERE clause
        ast = _add_where_condition(ast, condition)

    result = ast.sql()
    logger.debug(f"SQL after user filter injection: {result}")

    return result


def _add_where_condition(
    ast: exp.Expression, condition: exp.Expression
) -> exp.Expression:
    """
    Add a WHERE condition to the AST.

    If WHERE exists, adds as AND condition.
    If no WHERE, creates new WHERE clause.
    """
    # Find the main SELECT (not subqueries)
    select = ast.find(exp.Select)

    if select is None:
        logger.warning("No SELECT found in AST, cannot inject user filter")
        return ast

    # Get existing WHERE clause
    existing_where = select.find(exp.Where)

    if existing_where:
        # Combine with AND
        new_condition = exp.And(
            this=existing_where.this,
            expression=condition,
        )
        existing_where.set("this", new_condition)
    else:
        # Create new WHERE clause
        where_clause = exp.Where(this=condition)
        select.set("where", where_clause)

    return ast


def validate_user_filter_column(entry: DatasetEntry, column_name: str) -> bool:
    """
    Validate that the user filter column exists in the dataset schema.

    This is optional but recommended to catch configuration errors early.
    """
    # Get schema from lineage facets
    if not entry.lineage:
        return True  # Can't validate, assume OK

    facets = entry.lineage.get("facets", {})
    schema_facet = facets.get("schema", {})
    fields = schema_facet.get("fields", [])

    if not fields:
        return True  # Can't validate, assume OK

    field_names = {f.get("name") for f in fields if f.get("name")}

    if column_name not in field_names:
        logger.warning(
            f"User filter column '{column_name}' not found in dataset schema. "
            f"Available columns: {field_names}"
        )
        return False

    return True
