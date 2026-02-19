import pytest
from datamov.core.engine import Engine
from datamov.core.data_flow import DataFlow
from datamov.utils.exceptions import FlowTypeException

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
