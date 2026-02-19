import pytest
import logging
from datamov.core.logger.Logger import Logger

@pytest.fixture(autouse=True)
def reset_singleton():
    """Reset the singleton instance before each test."""
    Logger._instance = None
    yield
    Logger._instance = None

def test_logger_singleton():
    """Test that Logger is a singleton."""
    logger1 = Logger()
    logger2 = Logger()
    assert logger1 is logger2
    assert logger1.get_logger() is logger2.get_logger()

def test_get_logger_returns_logger_instance():
    """Test that get_logger returns a logging.Logger instance."""
    logger = Logger()
    assert isinstance(logger.get_logger(), logging.Logger)
    assert logger.get_logger().name == "DataMov"

def test_set_log_level():
    """Test that set_log_level changes the logger's level."""
    logger = Logger()
    # Default is INFO
    assert logger.get_logger().level == logging.INFO

    logger.set_log_level(logging.DEBUG)
    assert logger.get_logger().level == logging.DEBUG

    logger.set_log_level(logging.WARNING)
    assert logger.get_logger().level == logging.WARNING

def test_logger_initialization_idempotency():
    """Test that multiple initializations do not duplicate handlers or reset configuration incorrectly."""
    logger1 = Logger()
    handlers_count = len(logger1.get_logger().handlers)

    # Creating another instance (returns same object, calls __init__ again)
    logger2 = Logger()

    # Handlers count should remain the same
    assert len(logger2.get_logger().handlers) == handlers_count
