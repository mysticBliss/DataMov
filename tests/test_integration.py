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

        # Configure mocks to ensure count() returns an integer,
        # necessary when running in an environment where pyspark is mocked (like CI)
        mock_df = MagicMock()
        mock_df.count.return_value = 2

        # When pyspark is mocked, `spark` fixture is a MagicMock.
        # We need to ensure `spark.sql()` returns our `mock_df`.
        spark.sql.return_value = mock_df
        spark.createDataFrame.return_value = mock_df

        engine.run_flow()

        # Verify output
        # In a mocked environment, no file is written.

        # Verify source data read was attempted
        spark.sql.assert_any_call("SELECT id, val FROM source_table")

        # Verify validation logic ran
        # If expectation failed, it wouldn't reach save.

        # Verify save logic
        # We check if `save` method was called on any object derived from mock_df
        # Since MagicMock chains can be complex, we inspect all calls on mock_df
        found_save = False
        save_path = str(tmp_path / "output")

        for call in mock_df.mock_calls:
            # Look for a call that includes 'save' and the destination path
            if 'save' in str(call) and save_path in str(call):
                found_save = True
                break

        assert found_save, f"Expected save call to {save_path} not found in mock_df calls"
