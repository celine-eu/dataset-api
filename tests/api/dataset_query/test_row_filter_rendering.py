"""Row filters are applied to the mapped AST and the result stays valid postgres.

The executor used to render the mapped query, re-parse it with sqlglot's default
dialect, apply the plans and render it again. That round trip broke intervals
(`INTERVAL '30' MINUTES`) and split `schema.table` so that no plan matched, which
dropped every predicate on a schema-qualified physical table without a trace.
"""

import sqlglot
from sqlglot import exp

from celine.dataset.api.dataset_query.parser import parse_sql_query
from celine.dataset.api.dataset_query.row_filters.apply import apply_row_filter_plans
from celine.dataset.api.dataset_query.row_filters.models import RowFilterPlan

LOGICAL = "datasets.ds_dev_gold.meters_data_15m"
PHYSICAL = "ds_dev_gold.meters_data_15m"


def _own_devices(table: str = PHYSICAL) -> RowFilterPlan:
    return RowFilterPlan(
        table=table,
        kind="predicate",
        predicate_template=exp.In(
            this=exp.column("device_id"),
            expressions=[exp.Literal.string("mine")],
        ),
    )


def _filtered(sql: str, *plans: RowFilterPlan) -> str:
    parsed = parse_sql_query(sql)
    mapped = parsed.to_ast(tables_map={LOGICAL: PHYSICAL})
    return apply_row_filter_plans(mapped, plans).sql(dialect="postgres")


def test_predicate_applies_to_a_schema_qualified_physical_table():
    rendered = _filtered(f"SELECT device_id, ts FROM {LOGICAL}", _own_devices())

    where = sqlglot.parse_one(rendered, read="postgres").find(exp.Where)
    assert where is not None
    assert "'mine'" in where.sql()


def test_predicate_applies_inside_a_cte():
    rendered = _filtered(
        f"WITH d AS (SELECT device_id, MAX(ts) AS last_seen FROM {LOGICAL} "
        "GROUP BY device_id) SELECT COUNT(*) FROM d",
        _own_devices(),
    )

    cte = sqlglot.parse_one(rendered, read="postgres").find(exp.CTE)
    assert "'mine'" in cte.sql()


def test_interval_survives_row_filtering_as_valid_postgres():
    rendered = _filtered(
        "SELECT COUNT(*) FILTER (WHERE ts >= CAST('2026-09-16T00:00:00Z' AS timestamptz) "
        f"- INTERVAL '30 minutes') AS reporting FROM {LOGICAL}",
        _own_devices(),
    )

    assert "MINUTES" not in rendered.upper().replace("'30 MINUTES'", "")
    assert "INTERVAL '30 MINUTES'" in rendered.upper()


def test_a_passthrough_plan_leaves_the_query_valid():
    # A service account's plan: matched, but with no predicate.
    passthrough = RowFilterPlan(table=PHYSICAL, kind="predicate", predicate_template=None)
    rendered = _filtered(
        f"SELECT ts FROM {LOGICAL} WHERE ts >= NOW() - INTERVAL '24 hours'", passthrough
    )

    assert "INTERVAL '24 HOURS'" in rendered.upper()
    assert "IN (" not in rendered.upper()


def test_a_parsed_schema_qualified_table_still_matches_the_plan():
    # The shape the executor produced before: db and name split by a re-parse.
    ast = sqlglot.parse_one(f"SELECT device_id FROM {PHYSICAL}")

    rendered = apply_row_filter_plans(ast, [_own_devices()]).sql(dialect="postgres")

    assert "'mine'" in rendered


def test_a_plan_for_another_table_does_not_filter():
    rendered = _filtered(
        f"SELECT device_id FROM {LOGICAL}", _own_devices("ds_dev_gold.other_table")
    )

    assert sqlglot.parse_one(rendered, read="postgres").find(exp.Where) is None
