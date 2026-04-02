import pytest
from unittest.mock import MagicMock, patch
from datamov.core.data_movements.DataMovements import DataMovements
from datamov.core.data_flow import DataFlow

@pytest.fixture
def mock_config_reader():
    with patch("datamov.core.data_movements.DataMovements.ConfigReader") as MockConfigReader:
        mock_instance = MockConfigReader.return_value
        yield mock_instance

def test_load_configs_active_only(mock_config_reader):
    """
    Test that only active data movements are loaded when active_only=True.
    """
    mock_data = {
        "data_movements_1.json": {
            "data_movements": [
                {
                    "name": "active_movement",
                    "active": True,
                    "source_type": "hive",
                    "destination_type": "parquet"
                },
                {
                    "name": "inactive_movement",
                    "active": False,
                    "source_type": "hive",
                    "destination_type": "parquet"
                }
            ]
        }
    }
    mock_config_reader.get_json_data.return_value = mock_data

    # Initialize with active_only=True
    dm = DataMovements(active_only=True)

    # Verify only active movement is present
    assert "active_movement" in dm.data_movements
    assert "inactive_movement" not in dm.data_movements
    assert len(dm.data_movements) == 1

def test_load_configs_missing_keys(mock_config_reader):
    """
    Test loading configuration files with missing top-level keys.
    """
    mock_data = {
        "data_movements_missing_key.json": {
            "other_key": []
        },
        "environment_missing_key.json": {
            "other_key": []
        }
    }
    mock_config_reader.get_json_data.return_value = mock_data

    dm = DataMovements()

    # Should be empty as keys are missing
    assert len(dm.data_movements) == 0
    assert len(dm.environment_configs) == 0

def test_load_configs_invalid_file_prefix(mock_config_reader):
    """
    Test that files with invalid prefixes are ignored.
    """
    mock_data = {
        "random_file.json": {
            "data_movements": [
                {"name": "should_not_load", "active": True}
            ]
        }
    }
    mock_config_reader.get_json_data.return_value = mock_data

    dm = DataMovements()

    # Should be empty as file prefix is wrong
    assert "should_not_load" not in dm.data_movements
    assert len(dm.data_movements) == 0

def test_load_configs_malformed_structure(mock_config_reader):
    """
    Test loading configuration files where the value is not a list.
    """
    mock_data = {
        "data_movements_malformed.json": {
            "data_movements": "not_a_list"
        },
        "environment_malformed.json": {
            "environment_configs": {"not": "a_list"}
        }
    }
    mock_config_reader.get_json_data.return_value = mock_data

    dm = DataMovements()

    # Should handle gracefully (checks isinstance(..., list))
    assert len(dm.data_movements) == 0
    assert len(dm.environment_configs) == 0

def test_load_data_movements_deprecated(mock_config_reader):
    """
    Test the deprecated load_data_movements method.
    """
    mock_data = {
        "data_movements_dep.json": {
            "data_movements": [
                {
                    "name": "dep_movement",
                    "active": True,
                    "source_type": "hive",
                    "destination_type": "parquet"
                }
            ]
        }
    }
    mock_config_reader.get_json_data.return_value = mock_data

    dm = DataMovements()

    # Verify initial load
    assert "dep_movement" in dm.data_movements

    # Clear and reload using deprecated method
    dm._data_movements = {}
    dm.load_data_movements()

    # Verify reloaded
    assert "dep_movement" in dm.data_movements

def test_load_environment_configs(mock_config_reader):
    """
    Test loading of environment configurations to ensure the attribute bug is fixed.
    """
    mock_data = {
        "environment_test_loading.json": {
            "environment_configs": [
                {
                    "environment": "test_env_loading",
                    "driver_details": {"driver": "mock"},
                    "kudu_masters": ["master1"]
                }
            ]
        }
    }
    mock_config_reader.get_json_data.return_value = mock_data

    dm = DataMovements()

    # Verify environment configs loaded
    assert "test_env_loading" in dm.environment_configs
    env_config = dm.environment_configs["test_env_loading"]
    assert env_config.driver_details == {"driver": "mock"}
    assert env_config.kudu_masters == ["master1"]
