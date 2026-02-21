import pytest
from datamov.utils.exceptions import (
    FlowTypeException,
    PathNotFoundException,
    SqlNotFound,
    CreateTrackingDB,
    EnvTypeException,
)

def test_flow_type_exception_default_message():
    exception = FlowTypeException()
    assert str(exception) == "Flow should be an instance of DataFlow"
    assert isinstance(exception, Exception)

def test_flow_type_exception_custom_message():
    custom_message = "Custom error message"
    exception = FlowTypeException(custom_message)
    assert str(exception) == custom_message
    assert isinstance(exception, Exception)

def test_flow_type_exception_raise():
    with pytest.raises(FlowTypeException) as excinfo:
        raise FlowTypeException("Custom error message")
    assert str(excinfo.value) == "Custom error message"

def test_path_not_found_exception_default_message():
    exception = PathNotFoundException()
    assert str(exception) == "Path not found"
    assert isinstance(exception, Exception)

def test_path_not_found_exception_custom_message():
    custom_message = "Custom path error"
    exception = PathNotFoundException(custom_message)
    assert str(exception) == custom_message
    assert isinstance(exception, Exception)

def test_path_not_found_exception_raise():
    with pytest.raises(PathNotFoundException) as excinfo:
        raise PathNotFoundException("Custom path error")
    assert str(excinfo.value) == "Custom path error"

def test_sql_not_found_exception_default_message():
    exception = SqlNotFound()
    assert str(exception) == "Field source_sql needs to be defined in DataFlow Configuration"
    assert isinstance(exception, Exception)

def test_sql_not_found_exception_custom_message():
    custom_message = "Custom sql error"
    exception = SqlNotFound(custom_message)
    assert str(exception) == custom_message
    assert isinstance(exception, Exception)

def test_sql_not_found_exception_raise():
    with pytest.raises(SqlNotFound) as excinfo:
        raise SqlNotFound("Custom sql error")
    assert str(excinfo.value) == "Custom sql error"

def test_create_tracking_db_exception():
    exception = CreateTrackingDB()
    assert str(exception) == "Create DB datamov_monitoring_db before proceeding..."
    assert isinstance(exception, Exception)

    with pytest.raises(CreateTrackingDB) as excinfo:
        raise CreateTrackingDB()
    assert str(excinfo.value) == "Create DB datamov_monitoring_db before proceeding..."

def test_env_type_exception():
    exception = EnvTypeException()
    assert str(exception) == "env should be an instance of EnvironmentConfig"
    assert isinstance(exception, Exception)

    with pytest.raises(EnvTypeException) as excinfo:
        raise EnvTypeException("env should be an instance of EnvironmentConfig")
    assert str(excinfo.value) == "env should be an instance of EnvironmentConfig"
