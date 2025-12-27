
import pytest
from celine.dataset.api.dataset_query.parser import parse_sql_query

def test_invalid_sql():
    with pytest.raises(Exception):
        parse_sql_query("SELEC * FROM solar")
