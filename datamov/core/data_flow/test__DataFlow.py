import pytest
from datamov import DataFlow

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