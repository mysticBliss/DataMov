import pytest
from unittest.mock import MagicMock, patch
from datamov.core.data_movements.DataMovements import DataMovements
from datamov.core.data_flow import DataFlow

@pytest.fixture
def mock_config_reader():
    with patch("datamov.core.data_movements.DataMovements.ConfigReader") as MockConfigReader:
        mock_instance = MockConfigReader.return_value

        mock_data = {
            "data_movements_test.json": {
                "data_movements": [
                    {
                        "name": "test_movement",
                        "active": True,
                        "source_type": "hive",
                        "destination_type": "parquet",
                        "source_sql": "SELECT 1",
                        "destination_path": "/tmp/test"
                    }
                ]
            },
            "environment_test.json": {
                "environment_configs": [
                    {
                        "environment": "test_env",
                        "driver_details": {"key": "value"},
                        "kudu_masters": ["master1"]
                    }
                ]
            },
            "random_file.json": {"foo": "bar"}
        }

        mock_instance.get_json_data.return_value = mock_data
        yield mock_instance

def test_load_configs(mock_config_reader):
    dm = DataMovements()

    # Check data movements loaded
    assert "test_movement" in dm.data_movements
    assert isinstance(dm.data_movements["test_movement"], DataFlow)

    # Check environment configs loaded
    assert "test_env" in dm.environment_configs
    assert dm.environment_configs["test_env"].driver_details == {"key": "value"}

    # Baseline check: get_json_data is called twice (once for data movements, once for environments)
    # If the implementation changes, this assertion should be updated to 1
    assert mock_config_reader.get_json_data.call_count == 1
