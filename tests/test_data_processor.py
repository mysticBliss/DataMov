import pytest
from unittest.mock import MagicMock, patch
from datamov.core.data_processor.DataProcessor import DataProcessor

class TestDataProcessor:
    @pytest.fixture
    def mock_spark(self):
        spark = MagicMock()
        return spark

    def test_save_data_hive_missing_table_name(self, mock_spark):
        """
        Test that save_data logs an error and returns False status when format_type is 'hive'
        but table_name is None.
        """
        processor = DataProcessor(mock_spark)
        df = MagicMock()

        # Patch logger to verify error logging
        with patch("datamov.core.data_processor.DataProcessor.logger") as mock_logger:
            result = processor.save_data(
                df=df,
                destination_path=None,
                format_type="hive",
                mode="append",
                table_name=None
            )

            # Verify result status is False
            assert result["status"] is False
            assert result["output"] == df

            # Verify that the correct error message was logged
            # The code catches ValueError("Table name is required for Hive.") and logs it
            error_calls = mock_logger.error.call_args_list
            assert len(error_calls) > 0

            # The logged message is "Error occurred while saving data: Table name is required for Hive."
            logged_message = error_calls[0][0][0]
            assert "Table name is required for Hive." in logged_message

    def test_save_data_hive_valid(self, mock_spark):
        """
        Test that save_data proceeds correctly when format_type is 'hive' and table_name is provided.
        """
        processor = DataProcessor(mock_spark)
        df = MagicMock()
        table_name = "test_db.test_table"
        mode = "append"
        partition_cols = ["dt"]

        with patch("datamov.core.data_processor.DataProcessor.logger") as mock_logger:
            result = processor.save_data(
                df=df,
                destination_path=None,
                format_type="hive",
                mode=mode,
                table_name=table_name,
                partition_cols=partition_cols
            )

            # Verify result status is True
            assert result["status"] is True
            assert result["output"] == df

            # Verify that df.write methods were called correctly
            # Expected chain: df.write.mode(mode).format("hive").partitionBy(*partition_cols).saveAsTable(table_name)

            mock_write = df.write
            mock_write.mode.assert_called_with(mode)
            mock_write.mode.return_value.format.assert_called_with("hive")
            mock_write.mode.return_value.format.return_value.partitionBy.assert_called_with(*partition_cols)
            mock_write.mode.return_value.format.return_value.partitionBy.return_value.saveAsTable.assert_called_with(table_name)
