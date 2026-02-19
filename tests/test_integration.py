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
    with patch("datamov.core.engine.Engine.SparkManager") as MockSparkManager, \
         patch("datamov.core.engine.Engine.DataProcessor") as MockDataProcessor:

        mock_spark_manager_instance = MockSparkManager.return_value
        mock_spark_manager_instance.__enter__.return_value = spark
        mock_spark_manager_instance.__exit__.return_value = None # Do nothing on exit

        # Mock DataProcessor to return a DataFrame mock that behaves like a DataFrame
        mock_processor_instance = MockDataProcessor.return_value

        # Configure fetch_data to return a mock DataFrame
        mock_df = MagicMock()
        mock_df.count.return_value = 2 # Set count to return an integer
        mock_processor_instance.fetch_data.return_value = mock_df

        # Configure create_temp_table_and_resultant_df to return the same mock DataFrame
        mock_processor_instance.create_temp_table_and_resultant_df.return_value = mock_df

        # Configure save_data to return success
        mock_processor_instance.save_data.return_value = {'status': True, 'output': mock_df}

        engine.run_flow()

        # Verify calls were made
        assert mock_processor_instance.fetch_data.called
        assert mock_processor_instance.create_temp_table_and_resultant_df.called
        assert mock_processor_instance.save_data.called
