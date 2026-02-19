import pytest
from unittest.mock import MagicMock, patch
from datamov.core.data_processor.DataProcessor import DataProcessor

class TestDataProcessor:
    def test_create_temp_table_and_resultant_df_unique_name(self):
        # Setup
        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_df_transformed = MagicMock()
        mock_spark.sql.return_value = mock_df_transformed

        processor = DataProcessor(mock_spark)
        destination_sql = "SELECT * FROM"

        # Act 1
        processor.create_temp_table_and_resultant_df(mock_df, destination_sql)

        # Act 2
        processor.create_temp_table_and_resultant_df(mock_df, destination_sql)

        # Assert
        assert mock_df.createOrReplaceTempView.call_count == 2

        call_args_list = mock_df.createOrReplaceTempView.call_args_list
        name1 = call_args_list[0][0][0]
        name2 = call_args_list[1][0][0]

        # Currently expecting this to FAIL or pass depending on what we assert.
        # The goal is to assert that they are unique and NOT 'datamov_tmp'.
        # But before the fix, they WILL be 'datamov_tmp'.

        # So I will assert what I EXPECT after the fix, so checking for failure now.
        assert name1 != "datamov_tmp", f"Expected unique name, got {name1}"
        assert name2 != "datamov_tmp", f"Expected unique name, got {name2}"
        assert name1 != name2, "Expected different names for different calls"

        # Also check that the SQL query used the correct table name
        sql_calls = mock_spark.sql.call_args_list
        assert len(sql_calls) == 2
        sql1 = sql_calls[0][0][0]
        sql2 = sql_calls[1][0][0]

        assert name1 in sql1
        assert name2 in sql2
