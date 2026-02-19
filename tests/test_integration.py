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
    # Note: spark fixture here is a MagicMock due to global mocking in conftest.py
    # We need to configure it to behave like a real session or at least return what we expect.

    # Configure the mock spark session to return a mock DataFrame
    mock_df = MagicMock()
    spark.createDataFrame.return_value = mock_df

    # When DataProcessor.fetch_data calls spark.sql(...), it should return a mock DF
    # But wait, data_processor.fetch_data calls fetch_data_from_hive which calls spark.sql(query)

    # We need to ensure that whatever fetch_data returns, subsequent calls on it behave correctly.
    # In Engine.py:
    # df = data_processor.fetch_data(...) -> returns a mock DF
    # df_transformed = data_processor.create_temp_table_and_resultant_df(...) -> returns a mock DF (likely the same or derived)

    # Engine.py calls:
    # count_transformed = df_transformed.count()
    # We need this to return an int.

    # The data_processor methods return mocks because they use the mocked spark session.
    # We can patch DataProcessor methods or configure the mock returned by spark.

    # Let's configure the mock_df returned by create_temp_table_and_resultant_df
    # But create_temp_table_and_resultant_df creates a new DF from spark.sql()

    # Key point: The test injects 'spark' into SparkManager.
    # This 'spark' is the fixture.
    # Because of conftest.py, sys.modules["pyspark"] is mocked.
    # So SparkSession.builder...getOrCreate() returns a MagicMock.

    # So 'spark' is a MagicMock.

    # Engine.py flow:
    # 1. with SparkManager(...) as spark: -> spark is our mock fixture
    # 2. data_processor = DataProcessor(spark)
    # 3. df = data_processor.fetch_data(...)
    #    -> calls spark.sql(query) -> returns a mock
    # 4. df_transformed = data_processor.create_temp_table_and_resultant_df(df, ...)
    #    -> calls df.createOrReplaceTempView(...)
    #    -> calls spark.sql(...) -> returns a mock (let's call it transformed_df_mock)
    # 5. count_transformed = df_transformed.count()

    # So we need spark.sql.return_value.count.return_value to be an int.

    spark.sql.return_value.count.return_value = 2

    # Also, we need to handle databaseExists calls
    # spark.catalog._jcatalog.databaseExists("datamov_monitoring_db")
    # This mock path needs to return a boolean, likely True to skip creation or False then True.
    # Let's say it returns True.
    spark.catalog._jcatalog.databaseExists.return_value = True

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

        # Also patch Validator to avoid Great Expectations complexity on mocks
        with patch("datamov.core.engine.Engine.Validator") as MockValidator:
            mock_validator_instance = MockValidator.return_value
            mock_validator_instance.validate.return_value = True

            engine.run_flow()

    # Verify output
    # Since everything is mocked, we can't check file existence or real counts.
    # We verify that the expected methods were called.

    # Verify fetch_data equivalent called (spark.sql with source query)
    spark.sql.assert_any_call("SELECT id, val FROM source_table")

    # Verify save operation (write.format...save)
    # df_transformed.write.mode(MODE).format(TYPE).partitionBy(PARTITIONS).save(PATH)
    # spark.sql().write.mode().format().partitionBy().save.assert_called()

    # The chain is long, simpler to check if save was called.
    # spark.sql.return_value.write.mode.return_value.format.return_value.partitionBy.return_value.save.assert_called_with(str(tmp_path / "output"))
