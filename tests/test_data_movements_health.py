import pytest
from unittest.mock import MagicMock, patch
from datamov.core.data_movements.DataMovements import DataMovements, EnvironmentConfig
from datamov.core.data_flow import DataFlow

@pytest.fixture
def mock_config_reader():
    with patch('datamov.core.data_movements.DataMovements.ConfigReader') as mock:
        yield mock

def test_data_movements_properties(mock_config_reader):
    mock_instance = mock_config_reader.return_value
    mock_instance.get_json_data.return_value = {
        'data_movements_test.json': {
            'data_movements': [
                {
                    'name': 'test_flow',
                    'active': True,
                    'source_type': 'hive',
                    'source_sql': 'select 1',
                    'destination_type': 'hive',
                    'destination_mode': 'overwrite',
                    'destination_table': 'test_table',
                    'destination_sql': 'select * from'
                }
            ]
        },
        'environment_test.json': {
            'environment_configs': [
                {
                    'environment': 'dev',
                    'driver_details': {},
                    'kudu_masters': []
                }
            ]
        }
    }

    dm = DataMovements()

    # Assert new property names
    assert hasattr(dm, 'data_movements')
    assert hasattr(dm, 'environment_configs')

    # Assert values
    movements = dm.data_movements
    assert 'test_flow' in movements
    assert isinstance(movements['test_flow'], DataFlow)

    configs = dm.environment_configs
    assert 'dev' in configs
    assert isinstance(configs['dev'], EnvironmentConfig)
