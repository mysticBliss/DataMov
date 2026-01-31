from datamov.utils.exceptions import FlowTypeException

def test__default_message():
    exception = FlowTypeException()
    assert str(exception) == "Flow should be an instance of DataFlow"

def test__custom_message():
    custom_message = "Custom error message"
    exception = FlowTypeException(custom_message)
    assert str(exception) == custom_message
