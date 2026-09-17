"""The allowlist admits pure analytical SQL and still refuses everything else.

sqlglot parses every function it knows into a typed node, so these pin that the
allowlist applies to typed functions too, and not only to `exp.Anonymous`.
"""

import pytest
from fastapi import HTTPException

from celine.dataset.api.dataset_query.parser import parse_sql_query


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT COALESCE(MAX(v), 0) FROM solar",
        "SELECT IFNULL(v, 0) FROM solar",
        "SELECT NULLIF(v, 0) FROM solar",
        "SELECT GREATEST(0, v), LEAST(1, v) FROM solar",
        "SELECT FLOOR(EXTRACT(EPOCH FROM (CAST('2026-01-02' AS timestamptz) - ts)) / 60) FROM solar",
        "SELECT ROUND(v), ABS(v), LOWER(name) FROM solar",
        "SELECT DATE_TRUNC('day', ts) FROM solar",
        "SELECT CASE WHEN v IS NULL THEN 'none' WHEN v > 1 THEN 'high' ELSE 'low' END FROM solar",
        "SELECT id, RANK() OVER (ORDER BY v DESC) AS r FROM solar",
        "SELECT ROW_NUMBER() OVER (PARTITION BY id ORDER BY ts), DENSE_RANK() OVER (ORDER BY v) FROM solar",
        "SELECT SUM(v) OVER (PARTITION BY id ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM solar",
        "SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY v) FROM solar",
        "SELECT PERCENTILE_DISC(0.9) WITHIN GROUP (ORDER BY v) FROM solar",
        "SELECT id, BOOL_OR(flag), BOOL_AND(flag) FROM solar GROUP BY id",
        "SELECT COUNT(*) FILTER (WHERE ts >= NOW() - INTERVAL '30 minutes') FROM solar",
        (
            "WITH buckets(label, minimum) AS (VALUES ('0', 0), ('1-50', 1)) "
            "SELECT b.label, COUNT(s.id) FROM buckets b "
            "LEFT JOIN solar s ON s.v >= b.minimum GROUP BY b.label"
        ),
    ],
)
def test_pure_analytical_constructs_are_accepted(sql: str):
    parsed = parse_sql_query(sql)
    assert parsed.tables == {"solar"}


@pytest.mark.parametrize(
    "sql",
    [
        # hash functions are deliberately absent from the allowlist
        "SELECT MD5(name) FROM solar",
        "SELECT SHA256(name) FROM solar",
        # string concatenation is not admitted
        "SELECT name || 'x' FROM solar",
        # typed functions outside the allowlist
        "SELECT TO_CHAR(ts, 'YYYY') FROM solar",
        "SELECT STRING_AGG(name, ',') FROM solar",
        # server capability functions stay out, wherever they are nested
        "SELECT CASE WHEN pg_sleep(1) IS NULL THEN 1 END FROM solar",
        "SELECT RANK() OVER (ORDER BY pg_sleep(1)) FROM solar",
        "SELECT COALESCE(pg_read_file('/etc/passwd'), '') FROM solar",
        "SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY current_setting('x')) FROM solar",
        "SELECT version() FROM solar",
        # a VALUES list does not make a set operation acceptable
        "SELECT v FROM solar UNION SELECT 1",
    ],
)
def test_other_functions_and_constructs_stay_rejected(sql: str):
    with pytest.raises(HTTPException) as exc:
        parse_sql_query(sql)
    assert exc.value.status_code == 400
