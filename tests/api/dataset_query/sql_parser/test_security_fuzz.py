import pytest
from fastapi import HTTPException
from hypothesis import given, strategies as st

from celine.dataset.api.dataset_query.parser import parse_sql_query


@given(
    st.text(
        min_size=0,
        max_size=500,
        alphabet=st.characters(
            blacklist_categories=("Cs",),  # no surrogate chars
        ),
    )
)
def test_fuzz_inputs_do_not_crash(sql: str):
    """
    Fuzz test: the parser must NEVER crash, hang, or raise
    unexpected exceptions for arbitrary input.

    It may:
      - accept the SQL
      - reject it with HTTPException (400)

    It must NOT:
      - raise ValueError / KeyError / AttributeError
      - hang
      - leak internal errors
    """
    try:
        parse_sql_query(sql)
    except HTTPException as exc:
        # Expected: controlled rejection
        assert exc.status_code == 400
    except Exception as exc:  # pragma: no cover
        pytest.fail(f"Unexpected exception type: {type(exc)}: {exc}")
