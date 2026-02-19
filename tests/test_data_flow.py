import pytest
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

from datetime import date, timedelta

def test_generate_paths_with_dates():
    df = DataFlow(
        source_execution_date=None,
        source_frequency_value=3,
        source_frequency_unit='days',
        source_fs_path='/tmp/data/{data_format}/file.csv',
        source_data_format='dt.strftime("%Y-%m-%d")'
    )

    paths = df.generate_paths

    today = date.today()
    expected_dates = [today - timedelta(days=x + 1) for x in range(3)]
    expected_paths = ['/tmp/data/{}/file.csv'.format(dt.strftime("%Y-%m-%d")) for dt in expected_dates]

    assert paths == expected_paths
