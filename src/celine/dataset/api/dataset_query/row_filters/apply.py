from __future__ import annotations

import logging
from typing import Iterable

import sqlglot
from sqlglot import exp

from celine.dataset.api.dataset_query.row_filters.models import RowFilterPlan

logger = logging.getLogger(__name__)


def _table_name(table: exp.Table) -> str:
    # table.this is Identifier; may contain dots if set that way
    ident = table.args.get("this")
    if isinstance(ident, exp.Identifier):
        return ident.this
    return table.sql()


def _qualify_columns(expr: exp.Expression, alias: str) -> exp.Expression:
    """Qualify unqualified Column nodes in expr with alias."""
    e = expr.copy()
    for col in e.find_all(exp.Column):
        if col.args.get("table") is None:
            col.set("table", exp.Identifier(this=alias, quoted=False))
    return e


def _add_where(select: exp.Select, condition: exp.Expression) -> None:
    existing = select.args.get("where")
    if isinstance(existing, exp.Where):
        new_cond = exp.And(this=existing.this, expression=condition)
        existing.set("this", new_cond)
    else:
        select.set("where", exp.Where(this=condition))


def _tables_in_select(select: exp.Select) -> list[exp.Table]:
    tables: list[exp.Table] = []
    for t in select.find_all(exp.Table):
        anc = t.find_ancestor(exp.Select)
        if anc is select:
            tables.append(t)
    return tables


def apply_row_filter_plans(ast: exp.Expression, plans: Iterable[RowFilterPlan]) -> exp.Expression:
    """Apply row filter plans to an AST (returns a modified copy)."""
    plans_by_table: dict[str, list[RowFilterPlan]] = {}
    for p in plans:
        plans_by_table.setdefault(p.table, []).append(p)

    out = ast.copy()

    # If any plan is deny -> inject FALSE predicate at top-level
    for ps in plans_by_table.values():
        for p in ps:
            if p.kind == "deny":
                top = out.find(exp.Select)
                if top is None:
                    return out
                _add_where(top, exp.Boolean(this=False))
                return out

    for select in out.find_all(exp.Select):
        tables = _tables_in_select(select)
        for table in tables:
            name = _table_name(table)
            if name not in plans_by_table:
                continue

            alias = table.alias_or_name
            for plan in plans_by_table[name]:
                if plan.kind != "predicate" or plan.predicate_template is None:
                    continue
                cond = _qualify_columns(plan.predicate_template, alias)
                _add_where(select, cond)

    return out
