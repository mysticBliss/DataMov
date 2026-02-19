import pytest
from datetime import date
from datamov.core.data_flow import DataFlow

@pytest.fixture
def data_flow_config():
   return {
       "name": "hive-to-hive-voice-data",
       "description": "DataFlow that loads sample hive table to hive with select SQL inline transformation",
       "active": True,
       "source_execution_date": None,
       "source_frequency_value": None,
       "source_frequency_unit": None,
       "source_type": "hive",
       "source_format": None,
       "source_table": None,
       "source_sql": "select '1' as a, '2' as b, '3' as c",
       "source_partition_column": None,
       "source_fs_path": None,
       "source_data_format": None,
       "destination_type": "hive",
       "destination_mode": "overwrite",
       "destination_table": "default.sample_etl",
       "destination_partitions": [],
       "destination_fs_path": None,
       "destination_fs_func": None,
       "destination_path": None,
       "destination_sql": "SELECT a, b FROM"
   }

def test_data_flow(data_flow_config):
   df = DataFlow(**data_flow_config)

   # Check if all attributes were correctly set
   for key, value in data_flow_config.items():
       assert getattr(df, key) == value

def test_subtract_month_static():
    # Test direct static call
    d = date(2023, 3, 15)

    # 1 month back: Feb 2023
    result = DataFlow._subtract_month(d, 1)
    assert result == date(2023, 2, 15)

    # 2 months back: Jan 2023
    result = DataFlow._subtract_month(d, 2)
    assert result == date(2023, 1, 15)

    # 3 months back: Dec 2022
    result = DataFlow._subtract_month(d, 3)
    assert result == date(2022, 12, 15)

    # 15 months back: Dec 2021
    result = DataFlow._subtract_month(d, 15)
    assert result == date(2021, 12, 15)

def test_subtract_month_edge_cases():
    # March 31 -> Feb 28 (2023 is not leap)
    d = date(2023, 3, 31)
    try:
        result = DataFlow._subtract_month(d, 1)
        # Expect clamping or some valid date.
        # Ideally Feb 28.
        assert result.month == 2
        assert result.day == 28
    except ValueError:
        pytest.fail("ValueError raised for March 31 - 1 month")

    # March 31 2024 -> Feb 29 (Leap year)
    d = date(2024, 3, 31)
    result = DataFlow._subtract_month(d, 1)
    assert result == date(2024, 2, 29)

def test_generate_paths_months():
    config = {
        "source_execution_date": None,
        "source_frequency_value": 2,
        "source_frequency_unit": "months",
        "source_data_format": "'{:%Y-%m-%d}'.format(dt)",
        "source_fs_path": "/data/{data_format}/"
    }
    df = DataFlow(**config)
    paths = df.generate_paths

    assert len(paths) == 2
    assert "/data/" in paths[0]
