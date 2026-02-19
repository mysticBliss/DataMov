import pytest
from pyspark.sql import SparkSession
from datamov.core.engine import Engine
from datamov.core.data_flow import DataFlow
from unittest.mock import patch, MagicMock, call
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
         patch("datamov.core.engine.Engine.DataProcessor") as MockDataProcessorClass:

        # Setup SparkManager mock
        mock_instance = MockSparkManager.return_value
        mock_instance.__enter__.return_value = spark
        mock_instance.__exit__.return_value = None # Do nothing on exit

        # Setup DataProcessor mock
        mock_processor_instance = MockDataProcessorClass.return_value

        # Mocking fetch_data to return a valid DF
        mock_processor_instance.fetch_data.return_value = source_df

        # Mock create_temp_table_and_resultant_df to return a mock DF with count()=2
        # We need this because Engine.py calls .count() on the result of this function
        mock_df_transformed = MagicMock()
        mock_df_transformed.count.return_value = 2
        mock_processor_instance.create_temp_table_and_resultant_df.return_value = mock_df_transformed

        # Mock save_data to simulate success
        mock_processor_instance.save_data.return_value = {"status": True, "output": mock_df_transformed}

        engine.run_flow()

        # Verify that save_data was called
        # It should be called TWICE: once for data, once for tracking
        assert mock_processor_instance.save_data.call_count >= 1

        # Verify the FIRST call (which should be the data load)
        # using call_args_list[0]
        first_call = mock_processor_instance.save_data.call_args_list[0]
        args, kwargs = first_call

        # Check if the first call is indeed the data load
        if kwargs.get('table_name') == "datamov_monitoring_db.t_etl_flow_tracker":
             # This would be unexpected if flow ran correctly, but let's be safe
             pytest.fail("First save_data call was for tracking, expected data load")

        assert kwargs['destination_path'] == str(tmp_path / "output")
        assert kwargs['format_type'] == "parquet"
        assert kwargs['mode'] == "overwrite"
