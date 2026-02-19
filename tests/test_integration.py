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

        # Configure mock for count() if spark is mocked (which it is in this env)
        spark.sql.return_value.count.return_value = 2

        engine.run_flow()

        # Verify output
        # Since we are mocking Spark, no file is written.
        # We verify that save was called with correct arguments.
        # df_transformed is the result of create_temp_table_and_resultant_df which uses spark.sql
        # So spark.sql.return_value is the DF.

        mock_df = spark.sql.return_value

        # df gets transformed by withColumn in Engine.py before saving
        mock_df_final = mock_df.withColumn.return_value

        # DataProcessor.save_data calls:
        # df.write.format(format_type).mode(mode).partitionBy(*partition_cols).save(destination_path)

        mock_df_final.write.format.assert_called_with("parquet")
        mock_df_final.write.format.return_value.mode.assert_called_with("overwrite")

        # partitionBy might be called with empty args
        mock_df_final.write.format.return_value.mode.return_value.partitionBy.assert_called_with()

        mock_df_final.write.format.return_value.mode.return_value.partitionBy.return_value.save.assert_called_with(str(tmp_path / "output"))
