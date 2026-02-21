import pytest
from unittest.mock import MagicMock, patch
from datamov.core.data_movements.DataMovements import DataMovements
from datamov.core.data_flow import DataFlow

@pytest.fixture
def mock_config_reader():
    with patch("datamov.core.data_movements.DataMovements.ConfigReader") as MockConfigReader:
        # Set up mock data
        mock_instance = MockConfigReader.return_value
        mock_instance.get_json_data.return_value = {
            "data_movements_test.json": {
                "data_movements": [
                    {
                        "name": "test_movement",
                        "active": True,
                        "source_type": "hive",
                        "source_sql": "SELECT * FROM test",
                        "destination_type": "parquet",
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
            "random_file.json": {}
        }
        yield MockConfigReader

def test_load_configs(mock_config_reader):
    dm = DataMovements()

    # Check data movements loaded
    assert "test_movement" in dm.data_movements
    assert isinstance(dm.data_movements["test_movement"], DataFlow)

    # Check environment configs loaded
    assert "test_env" in dm.environment_configs
    # assert isinstance(dm.environments["test_env"], EnvironmentConfig) # Need to import EnvironmentConfig if checking type
