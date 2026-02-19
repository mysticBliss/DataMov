import pytest
from datamov.core.engine import Engine
from datamov.core.data_flow import DataFlow
from datamov.utils.exceptions import FlowTypeException
from unittest.mock import patch, MagicMock

@pytest.fixture
def data_flow_config():
   return {
       "name": "test-flow",
       "description": "Test DataFlow",
       "active": True,
       "source_type": "hive",
       "source_sql": "select 1",
       "destination_type": "hive",
       "destination_mode": "overwrite",
       "destination_table": "default.test",
       "destination_sql": "SELECT * FROM"
   }

def test_load_data_flow_valid(data_flow_config):
    engine = Engine()
    flow = DataFlow(**data_flow_config)
    environments = {"env": "test"}

    engine.load_data_flow(flow, environments)

    assert flow in engine.dataflow
    assert engine.dataflow[flow] == environments

def test_load_data_flow_invalid():
    engine = Engine()
    invalid_flow = "not a DataFlow object"

    with pytest.raises(FlowTypeException):
        engine.load_data_flow(invalid_flow, {})

def test_run_flow_tracking_no_debug_print(data_flow_config):
    # Mock SparkManager context manager
    with patch('datamov.core.engine.Engine.SparkManager') as MockSparkManager:
        mock_spark = MagicMock()
        MockSparkManager.return_value.__enter__.return_value = mock_spark

        # Mock database checks
        mock_spark.catalog._jcatalog.databaseExists.return_value = True
        mock_spark.catalog.listDatabases.return_value = [MagicMock(name="datamov_monitoring_db")]

        # Mock DataProcessor
        with patch('datamov.core.engine.Engine.DataProcessor') as MockDataProcessor, \
             patch('datamov.core.engine.Engine.Validator') as MockValidator:

            # Setup Validator mock to pass
            mock_validator_instance = MockValidator.return_value
            mock_validator_instance.validate.return_value = True

            mock_processor = MockDataProcessor.return_value

            # Mock fetched data
            mock_df = MagicMock()
            mock_processor.fetch_data.return_value = mock_df

            # Mock transformed data
            mock_transformed_df = MagicMock()
            mock_processor.create_temp_table_and_resultant_df.return_value = mock_transformed_df
            mock_transformed_df.count.return_value = 10

            # Mock save_data
            mock_processor.save_data.return_value = {"status": True, "output": MagicMock()}

            # Mock createDataFrame
            mock_tracking_df = MagicMock()
            mock_spark.createDataFrame.return_value = mock_tracking_df

            # Setup Engine
            engine = Engine()

            # Setup DataFlow
            flow = DataFlow(**data_flow_config)

            engine.load_data_flow(flow, {})
            engine.run_flow()
