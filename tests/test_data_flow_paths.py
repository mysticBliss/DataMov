from datetime import date, timedelta
import pytest
from datamov.core.data_flow.DataFlow import DataFlow

class TestDataFlowPaths:

    def test_generate_paths_no_format(self):
        """Test default behavior when source_data_format is None."""
        df = DataFlow(
            name="test_flow",
            source_frequency_unit='days',
            source_frequency_value=1,
            source_fs_path='/data/{data_format}/file.parquet'
        )
        # Should generate path for yesterday
        yesterday = date.today() - timedelta(days=1)
        expected = [f'/data/{yesterday}/file.parquet']
        assert df.generate_paths == expected

    def test_generate_paths_strftime(self):
        """Test with source_data_format as a strftime string."""
        fmt = "%Y%m%d"
        df = DataFlow(
            name="test_flow",
            source_frequency_unit='days',
            source_frequency_value=1,
            source_fs_path='/data/{data_format}/file.parquet',
            source_data_format=f"dt.strftime('{fmt}')"
        )
        yesterday = date.today() - timedelta(days=1)
        expected = [f'/data/{yesterday.strftime(fmt)}/file.parquet']
        assert df.generate_paths == expected

    def test_generate_paths_literal_format(self):
        """Test with a format string containing literals."""
        fmt = "year-%Y/month-%m/day-%d"
        df = DataFlow(
            name="test_flow",
            source_frequency_unit='days',
            source_frequency_value=1,
            source_fs_path='/data/{data_format}/file.parquet',
            source_data_format=f"dt.strftime('{fmt}')"
        )
        yesterday = date.today() - timedelta(days=1)
        expected = [f'/data/{yesterday.strftime(fmt)}/file.parquet']
        assert df.generate_paths == expected

    def test_generate_paths_invalid_strftime(self):
        """Test with an invalid format string to check error handling (fallback)."""
        # Using invalid syntax to trigger eval exception and fallback
        invalid_code = "1 +"
        df = DataFlow(
            name="test_flow",
            source_frequency_unit='days',
            source_frequency_value=1,
            source_fs_path='/data/{data_format}/file.parquet',
            source_data_format=invalid_code
        )
        yesterday = date.today() - timedelta(days=1)

        # Should fallback to str(dt) which is YYYY-MM-DD
        expected_fallback = [f'/data/{yesterday}/file.parquet']

        assert df.generate_paths == expected_fallback
