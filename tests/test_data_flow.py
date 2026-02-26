import pytest
from datetime import date, timedelta
from unittest.mock import patch
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

class TestGeneratePaths:

    @patch('datamov.core.data_flow.DataFlow.date')
    def test_generate_paths_days(self, mock_date):
        fixed_today = date(2023, 1, 1)
        mock_date.today.return_value = fixed_today
        mock_date.side_effect = lambda *args, **kwargs: date(*args, **kwargs)

        df = DataFlow(
            source_frequency_unit='days',
            source_frequency_value=3,
            source_fs_path="/data/{data_format}"
        )

        paths = df.generate_paths

        expected_dates = [fixed_today - timedelta(days=x + 1) for x in range(3)]
        expected_paths = ["/data/{}".format(d) for d in expected_dates]

        assert paths == expected_paths

    @patch('datamov.core.data_flow.DataFlow.date')
    def test_generate_paths_months(self, mock_date):
        fixed_today = date(2023, 1, 1)
        mock_date.today.return_value = fixed_today
        mock_date.side_effect = lambda *args, **kwargs: date(*args, **kwargs)

        df = DataFlow(
            source_frequency_unit='months',
            source_frequency_value=2,
            source_fs_path="/data/{data_format}"
        )

        paths = df.generate_paths

        # Correct logic (using d.year + y as per fix in DataFlow.py)
        def subtract_month(d, months):
            y, m = divmod(d.month - months - 1, 12)
            return date(d.year + y, m + 1, d.day)

        expected_dates = [subtract_month(fixed_today, x + 1) for x in range(2)]
        expected_paths = ["/data/{}".format(d) for d in expected_dates]

        assert paths == expected_paths

    @patch('datamov.core.data_flow.DataFlow.date')
    def test_generate_paths_with_execution_date(self, mock_date):
        df = DataFlow(
            source_execution_date="2023-01-01",
            source_fs_path="/data/{data_format}"
        )
        paths = df.generate_paths
        assert paths == ["/data/2023-01-01"]

    @patch('datamov.core.data_flow.DataFlow.date')
    def test_generate_paths_with_format(self, mock_date):
        fixed_today = date(2023, 1, 1)
        mock_date.today.return_value = fixed_today
        mock_date.side_effect = lambda *args, **kwargs: date(*args, **kwargs)

        df = DataFlow(
            source_frequency_unit='days',
            source_frequency_value=1,
            source_data_format="dt.strftime('%Y%m%d')",
            source_fs_path="/data/{data_format}"
        )

        paths = df.generate_paths
        expected_date = fixed_today - timedelta(days=1)
        expected_str = expected_date.strftime('%Y%m%d')

        assert paths == ["/data/{}".format(expected_str)]

    def test_generate_paths_invalid_unit(self):
        df = DataFlow(
            source_frequency_unit='invalid_unit',
            source_frequency_value=1,
            source_fs_path="/data/{data_format}"
        )
        with pytest.raises(ValueError, match="Invalid frequency unit provided"):
            _ = df.generate_paths
