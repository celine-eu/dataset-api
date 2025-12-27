
import pytest
from celine.dataset.api.dataset_query.parser import parse_sql_query

def test_basic_select_parses():
    parsed = parse_sql_query("SELECT * FROM dwd_icon_d2_solar_energy")
    assert "SELECT" in parsed.sql.upper()
    assert parsed.tables == {"dwd_icon_d2_solar_energy"}

def test_cte_same_table():
    sql = '''
    WITH latest_run AS (
        SELECT max(run_time_utc) AS run_time_utc
        FROM dwd_icon_d2_solar_energy
    )
    SELECT run_time_utc
    FROM dwd_icon_d2_solar_energy
    WHERE run_time_utc = (
        SELECT run_time_utc FROM latest_run
    )
    '''
    parsed = parse_sql_query(sql)
    assert "WITH latest_run AS" in parsed.sql
    assert parsed.tables == {"dwd_icon_d2_solar_energy"}
