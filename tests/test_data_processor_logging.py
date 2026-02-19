from unittest.mock import MagicMock, patch, call
from datamov.core.data_processor.DataProcessor import DataProcessor

def test_sensitive_query_logging():
    # Mock SparkSession and DataFrame
    spark_mock = MagicMock()
    df_mock = MagicMock()

    # Mock return value of spark.sql to have printSchema method that returns string
    spark_mock.sql.return_value = df_mock
    df_mock.printSchema.return_value = "Schema Info"

    processor = DataProcessor(spark_mock)
    destination_sql = "SELECT sensitive_column FROM sensitive_table"
    temp_table_name = 'datamov_tmp'
    expected_generated_sql = "{}  {}".format(destination_sql, temp_table_name)

    # We want to verify that logger.info was called with the query.
    # Currently, it is. We want to assert this behavior to confirm the vulnerability.

    with patch("datamov.core.data_processor.DataProcessor.logger") as mock_logger:
        processor.create_temp_table_and_resultant_df(df_mock, destination_sql)

        # Check if info was called with the query.
        # This assertion should fail if the vulnerability exists (because we expect it NOT to be called).
        assert call(expected_generated_sql) not in mock_logger.info.mock_calls, \
            "Vulnerability: SQL query was logged at INFO level!"

        # Check if debug was called.
        # This assertion should fail if the fix is not applied (because currently it's not called at debug).
        assert call(expected_generated_sql) in mock_logger.debug.mock_calls, \
             "SQL query should be logged at DEBUG level."
