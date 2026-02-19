from datamov.utils.exceptions import FlowTypeException, PathNotFoundException, SqlNotFound, CreateTrackingDB, EnvTypeException

def test__default_message():
    exception = FlowTypeException()
    assert str(exception) == "Flow should be an instance of DataFlow"

def test__custom_message():
    custom_message = "Custom error message"
    exception = FlowTypeException(custom_message)
    assert str(exception) == custom_message

def test_path_not_found_exception():
    exception = PathNotFoundException()
    assert str(exception) == "Path not found"

    custom_message = "Custom path error"
    exception = PathNotFoundException(custom_message)
    assert str(exception) == custom_message

def test_sql_not_found_exception():
    exception = SqlNotFound()
    assert str(exception) == "Field source_sql needs to be defined in DataFlow Configuration"

    custom_message = "Custom sql error"
    exception = SqlNotFound(custom_message)
    assert str(exception) == custom_message

def test_create_tracking_db_exception():
    exception = CreateTrackingDB()
    assert str(exception) == "Create DB datamov_monitoring_db before proceeding..."

def test_env_type_exception():
    exception = EnvTypeException()
    assert str(exception) == "env should be an instance of EnvironmentConfig"
