"""QE-01 — `ST_AsGeoJSON` and `ST_Simplify` are admitted, and nothing beside them.

The widening is two names in `ALLOWED_FUNCTIONS`. These cases pin what it lets
through (each function alone, in its argument forms, and nested the way the
Digital Twin's boundary-shape fetcher asks), and that the neighbourhood stays
shut: other PostGIS functions nobody listed, and the functions that reach the
server, still refuse — alone and wrapped inside an admitted call.
"""

import pytest
import sqlglot
from fastapi import HTTPException

from celine.dataset.api.dataset_query.parser import parse_sql_query


def _functions(sql: str) -> set[str]:
    return {
        f.name.lower()
        for f in sqlglot.parse_one(sql).find_all(sqlglot.exp.Anonymous)
    }


# ---------------------------------------------------------------------------
# Accepted
# ---------------------------------------------------------------------------


def test_st_asgeojson_alone():
    parsed = parse_sql_query("SELECT id, ST_AsGeoJSON(geometry) AS shape FROM boundaries")
    assert parsed.tables == {"boundaries"}
    assert "st_asgeojson" in _functions(parsed.sql)


def test_st_simplify_alone():
    parsed = parse_sql_query("SELECT ST_Simplify(geometry, 0.001) FROM boundaries")
    assert parsed.tables == {"boundaries"}
    assert "st_simplify" in _functions(parsed.sql)


def test_boundary_shape_form_nested():
    """The form QE-01 names: a simplified shape serialised in one statement."""
    parsed = parse_sql_query(
        "SELECT id, ST_AsGeoJSON(ST_Simplify(geometry, 0.0005)) AS shape "
        "FROM boundaries WHERE id IN ('AC000E00001', 'AC000E00002')"
    )
    assert parsed.tables == {"boundaries"}
    assert {"st_asgeojson", "st_simplify"} <= _functions(parsed.sql)


@pytest.mark.parametrize(
    "call",
    [
        "ST_AsGeoJSON(geometry, 6)",  # max decimal digits
        "ST_AsGeoJSON(geometry, 6, 0)",  # digits + options bitmask
        "st_asgeojson(geometry)",  # case does not matter
        "ST_AsGeoJSON(ST_Transform(geometry, 4326))",
        "ST_Simplify(geometry, 0.001, TRUE)",  # preserveCollapsed
        "ST_Simplify(geometry, CAST(0.001 AS DOUBLE))",
    ],
)
def test_argument_forms(call: str):
    parsed = parse_sql_query(f"SELECT {call} FROM boundaries")
    assert parsed.tables == {"boundaries"}


def test_boundary_at_point_with_shape():
    """A point-in-shape lookup returning the shape, with functions already listed."""
    parsed = parse_sql_query(
        "SELECT id, ST_AsGeoJSON(ST_Simplify(geometry, 0.001)) FROM boundaries "
        "WHERE ST_Contains(geometry, ST_SetSRID(ST_Point(11.1, 46.1), 4326))"
    )
    assert parsed.tables == {"boundaries"}


# ---------------------------------------------------------------------------
# Still refused
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "fn",
    [
        # PostGIS neighbours nobody listed. Serialisers and constructors other
        # than the two admitted ones stay out until a reader needs them.
        "ST_AsText(geometry)",
        "ST_AsBinary(geometry)",
        "ST_AsKML(geometry)",
        "ST_AsSVG(geometry)",
        "ST_AsMVT(geometry)",
        "ST_SimplifyPreserveTopology(geometry, 0.001)",
        "ST_SimplifyVW(geometry, 0.001)",
        "ST_Buffer(geometry, 1000)",
        "ST_Union(geometry)",
        "ST_Dump(geometry)",
        "ST_GeomFromText('POINT(0 0)')",
        "PostGIS_Full_Version()",
    ],
)
def test_other_postgis_functions_still_refused(fn: str):
    with pytest.raises(HTTPException) as exc:
        parse_sql_query(f"SELECT {fn} FROM boundaries")
    assert exc.value.status_code == 400
    assert "not allowed" in str(exc.value.detail)


@pytest.mark.parametrize(
    "fn",
    [
        "pg_read_file('/etc/passwd')",
        "pg_sleep(10)",
        "dblink('host=x', 'SELECT 1')",
        "lo_import('/etc/passwd')",
        "current_setting('is_superuser')",
        "set_config('role', 'postgres', false)",
        "md5(geometry)",
    ],
)
def test_dangerous_functions_refused_inside_admitted_calls(fn: str):
    """Admitting the outer call admits nothing about its arguments."""
    for sql in (
        f"SELECT ST_AsGeoJSON({fn}) FROM boundaries",
        f"SELECT ST_Simplify({fn}, 0.001) FROM boundaries",
        f"SELECT ST_AsGeoJSON(ST_Simplify(geometry, {fn})) FROM boundaries",
    ):
        with pytest.raises(HTTPException) as exc:
            parse_sql_query(sql)
        assert exc.value.status_code == 400


def test_prefix_of_an_admitted_name_is_not_admitted():
    """The allowlist matches whole names, not prefixes."""
    with pytest.raises(HTTPException):
        parse_sql_query("SELECT ST_AsGeoJSONX(geometry) FROM boundaries")
    with pytest.raises(HTTPException):
        parse_sql_query("SELECT ST_Simplify_Anything(geometry, 1) FROM boundaries")

