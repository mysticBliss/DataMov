import pytest
from unittest.mock import MagicMock, patch
from datamov.core.data_processor.DataProcessor import DataProcessor

class TestDataProcessorKudu:
    @pytest.fixture
    def mock_spark(self):
        spark = MagicMock()
        return spark

    def test_save_data_kudu_table_exists(self, mock_spark):
        processor = DataProcessor(mock_spark)
        df = MagicMock()

        # Setup fluent mock for spark.read...
        reader_mock = mock_spark.read.format.return_value
        reader_mock.option.return_value = reader_mock
        reader_mock.options.return_value = reader_mock

        # Mock .load().limit(0).collect() to return empty list
        reader_mock.load.return_value.limit.return_value.collect.return_value = []

        with patch("datamov.core.data_processor.DataProcessor.logger") as mock_logger:
            result = processor.save_data(
                df=df,
                destination_path=None,
                format_type="kudu",
                mode="append",
                kudu_masters=["master1", "master2"],
                table_name="kudu_table"
            )

            assert result["status"] is True

            # Verify spark.read.format was called
            mock_spark.read.format.assert_called_with("org.apache.kudu.spark.kudu")

            # Verify load was called
            reader_mock.load.assert_called()

            # Verify save was called
            df.write.format.assert_called_with("org.apache.kudu.spark.kudu")

    def test_save_data_kudu_table_does_not_exist(self, mock_spark):
        processor = DataProcessor(mock_spark)
        df = MagicMock()

        # Setup fluent mock
        reader_mock = mock_spark.read.format.return_value
        reader_mock.option.return_value = reader_mock
        reader_mock.options.return_value = reader_mock

        # Mock .load() to raise Exception
        reader_mock.load.side_effect = Exception("Table not found")

        with patch("datamov.core.data_processor.DataProcessor.logger") as mock_logger:
            result = processor.save_data(
                df=df,
                destination_path=None,
                format_type="kudu",
                mode="append",
                kudu_masters=["master1", "master2"],
                table_name="missing_table"
            )

            assert result["status"] is False

            # Verify error log
            args, _ = mock_logger.error.call_args
            assert "Kudu table 'missing_table' does not exist." in args[0]

            # Verify save was NOT called
            df.write.format.assert_not_called()
