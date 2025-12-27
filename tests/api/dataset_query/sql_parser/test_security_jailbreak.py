
import pytest
from celine.dataset.api.dataset_query.parser import parse_sql_query

def test_write_disallowed():
    with pytest.raises(Exception):
        parse_sql_query("DELETE FROM solar")
