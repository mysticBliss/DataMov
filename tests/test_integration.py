import pytest
from pyspark.sql import SparkSession
from datamov.core.engine import Engine
from datamov.core.data_flow import DataFlow
from unittest.mock import patch, MagicMock
import os

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[1]") \
        .appName("datamov-test") \
        .config("spark.sql.shuffle.partitions", "1") \
        .config("spark.driver.extraJavaOptions", "-Dderby.system.home=/tmp/derby") \
        .enableHiveSupport() \
        .getOrCreate()

def test_engine_run(spark, tmp_path):
    # Setup source data
    source_df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])
    source_df.createOrReplaceTempView("source_table")

    spark.sql("CREATE DATABASE IF NOT EXISTS datamov_monitoring_db")

    # Define DataFlow
    flow_config = {
        "name": "test-flow",
        "active": True,
        "source_type": "hive",
        "source_sql": "SELECT id, val FROM source_table",
        "destination_type": "parquet",
        "destination_mode": "overwrite",
        "destination_path": str(tmp_path / "output"),
        "destination_sql": "SELECT id, val FROM",
        "expectations": [
            {"type": "expect_column_values_to_not_be_null", "kwargs": {"column": "id"}}
        ]
    }

    flow = DataFlow(**flow_config)

    engine = Engine()
    engine.load_data_flow(flow, None)

    # Patch SparkManager to use our spark session and NOT close it
    with patch("datamov.core.engine.Engine.SparkManager") as MockSparkManager:
        mock_instance = MockSparkManager.return_value
        mock_instance.__enter__.return_value = spark
        mock_instance.__exit__.return_value = None # Do nothing on exit

        # Mock DataProcessor to ensure returned dataframes have integer counts
        with patch("datamov.core.engine.Engine.DataProcessor") as MockDataProcessor:
             # Configure the mock processor instance
            mock_processor_instance = MockDataProcessor.return_value

            # Mock fetch_data to return a mock dataframe with count=2
            mock_fetched_df = MagicMock()
            mock_fetched_df.count.return_value = 2
            mock_processor_instance.fetch_data.return_value = mock_fetched_df

            # Mock create_temp_table_and_resultant_df to return a mock dataframe with count=2
            mock_transformed_df = MagicMock()
            mock_transformed_df.count.return_value = 2

            # Also need to ensure column operations work for 'uuid'
            mock_transformed_df.withColumn.return_value = mock_transformed_df

            mock_processor_instance.create_temp_table_and_resultant_df.return_value = mock_transformed_df

            # Mock save_data to return success
            mock_processor_instance.save_data.return_value = {"status": True, "output": mock_transformed_df}

            # Mock Validator because we are mocking the dataframe passed to it
            with patch("datamov.core.engine.Engine.Validator") as MockValidator:
                mock_validator_instance = MockValidator.return_value
                mock_validator_instance.validate.return_value = True

                engine.run_flow()

                # Verify DataProcessor calls
                mock_processor_instance.fetch_data.assert_called()
                mock_processor_instance.create_temp_table_and_resultant_df.assert_called()
                mock_processor_instance.save_data.assert_called()
