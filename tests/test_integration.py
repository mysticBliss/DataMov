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
    # Also patch Validator to avoid running real GE validation on mocks or requiring complex setup
    # NOTE: We do NOT patch 'lit' here because we are using a real SparkSession.
    # Real DataFrames require real Column objects from real lit().
    # The safe_lit_patch in conftest.py will handle returning the real lit() since SparkContext is active.
    with patch("datamov.core.engine.Engine.SparkManager") as MockSparkManager, \
         patch("datamov.core.engine.Engine.Validator") as MockValidator:

        mock_instance = MockSparkManager.return_value
        mock_instance.__enter__.return_value = spark
        mock_instance.__exit__.return_value = None # Do nothing on exit

        # Configure Validator mock
        mock_validator_instance = MockValidator.return_value
        mock_validator_instance.validate.return_value = True

        engine.run_flow()

        # Verify output
        # Wait, destination_path needs to be checked.
        # But Spark usually creates a directory.
        assert os.path.exists(str(tmp_path / "output"))

        out_df = spark.read.parquet(str(tmp_path / "output"))
        assert out_df.count() == 2
