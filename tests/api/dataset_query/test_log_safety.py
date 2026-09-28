"""QE-03 — `sql_shape`: what of a statement may reach a log.

Every literal becomes `?` — numbers (a lookup's coordinates) and strings (a row
filter's sensor ids) alike — comments are dropped, and text that cannot be
parsed is withheld whole rather than logged raw.
"""
import pytest
from sqlglot import parse_one

from celine.dataset.api.dataset_query.log_safety import WITHHELD, sql_shape


def test_numbers_are_masked_including_negative_ones():
    shape = sql_shape(
        "SELECT cod_ac FROM boundaries "
        "WHERE ST_Intersects(geometry, ST_SetSRID(ST_Point(-0.0371, 0.0829), 4326))"
    )
    assert "0371" not in shape and "0829" not in shape and "4326" not in shape
    assert "ST_POINT(-?, ?)" in shape.upper()
    assert "boundaries" in shape and "cod_ac" in shape


def test_strings_are_masked():
    shape = sql_shape(
        "SELECT kwh FROM readings WHERE device_id IN ('ex-00001', 'ex-00002')"
    )
    assert "ex-0000" not in shape
    assert "IN (?, ?)" in shape


def test_an_ast_is_masked_and_left_unchanged():
    ast = parse_one("SELECT a FROM t WHERE b = 0.0829")
    shape = sql_shape(ast)
    assert "0829" not in shape
    assert "0.0829" in ast.sql(), "the caller's AST is not mutated"


def test_comments_are_dropped():
    shape = sql_shape("SELECT a /* 0.0371 0.0829 */ FROM t -- ex-00001")
    assert "0371" not in shape and "ex-00001" not in shape


def test_the_executors_wrapper_statement_is_masked():
    """The paginated text the driver error path logs, with its bind names."""
    shape = sql_shape(
        "SELECT * FROM (SELECT a FROM t WHERE x = 0.0829) AS q "
        "LIMIT :limit OFFSET :offset"
    )
    assert "0829" not in shape
    assert "limit" in shape.lower()


@pytest.mark.parametrize("text", ["SELECT ((( 0.0371", "not sql at all 0.0829 '"])
def test_unparseable_text_is_withheld(text):
    assert sql_shape(text) == WITHHELD


def test_every_kind_of_literal_is_replaced():
    shape = sql_shape("SELECT 1, 'x', 2.5, INTERVAL '30 minutes' FROM t")
    assert "30" not in shape and "2.5" not in shape and "'x'" not in shape
    assert shape.count("?") == 4


@pytest.mark.parametrize(
    "text",
    [
        # An unterminated literal fails in sqlglot's tokenizer (TokenError, not a
        # ParseError), which reached the parser's catch-all safety net.
        "SELECT ST_Point(0.0371, 0.0829) FROM t WHERE x = 'abc",
        "SELECT a FROM t WHERE x = 0.0371 AND y = '0.0829",
    ],
)
def test_a_tokenizer_error_logs_no_literal(text, caplog):
    from fastapi import HTTPException

    from celine.dataset.api.dataset_query.parser import parse_sql_query

    caplog.set_level("DEBUG")
    with pytest.raises(HTTPException) as refused:
        parse_sql_query(text)
    assert refused.value.status_code == 400
    logged = caplog.text + "".join(
        str(r.exc_text or "") + str(r.exc_info or "") for r in caplog.records
    )
    assert "0371" not in logged and "0829" not in logged
    assert all(r.exc_info is None for r in caplog.records), "no traceback"
