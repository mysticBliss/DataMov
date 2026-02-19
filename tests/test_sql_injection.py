import pytest
from unittest.mock import MagicMock
from datamov.core.data_processor import DataProcessor
from datamov.utils.exceptions import SqlInjectionException

def test_sql_injection_prevention():
    # Setup mock SparkSession
    mock_spark = MagicMock()
    processor = DataProcessor(mock_spark)

    # Setup mock DataFrame
    mock_df = MagicMock()

    # The vulnerability: simple concatenation allows injection of comments and multiple statements
    malicious_sql = "SELECT * FROM users; DROP TABLE datamov_tmp; --"

    # Call the method and expect an exception
    with pytest.raises(SqlInjectionException) as excinfo:
        processor.create_temp_table_and_resultant_df(mock_df, malicious_sql)

    assert "Semicolon (;) not allowed" in str(excinfo.value)

def test_comment_injection_prevention():
    mock_spark = MagicMock()
    processor = DataProcessor(mock_spark)
    mock_df = MagicMock()

    malicious_sql = "SELECT * FROM users --"

    with pytest.raises(SqlInjectionException) as excinfo:
        processor.create_temp_table_and_resultant_df(mock_df, malicious_sql)

    assert "Double dash (--) comments not allowed" in str(excinfo.value)

def test_valid_query_works():
    mock_spark = MagicMock()
    processor = DataProcessor(mock_spark)
    mock_df = MagicMock()

    valid_sql = "SELECT * FROM"

    # Should not raise exception
    processor.create_temp_table_and_resultant_df(mock_df, valid_sql)

    # Verify spark.sql called.
    args, _ = mock_spark.sql.call_args
    executed_sql = args[0]

    assert valid_sql in executed_sql
    assert "datamov_tmp_" in executed_sql

def test_semicolon_in_string_literal_allowed():
    mock_spark = MagicMock()
    processor = DataProcessor(mock_spark)
    mock_df = MagicMock()

    # Semicolon inside string literal
    safe_sql = "SELECT * FROM users WHERE name = 'semi;colon'"

    # Should not raise exception
    processor.create_temp_table_and_resultant_df(mock_df, safe_sql)

    # Verify call
    args, _ = mock_spark.sql.call_args
    assert safe_sql in args[0]

def test_comment_in_string_literal_allowed():
    mock_spark = MagicMock()
    processor = DataProcessor(mock_spark)
    mock_df = MagicMock()

    safe_sql = "SELECT * FROM users WHERE name = '--comment'"

    processor.create_temp_table_and_resultant_df(mock_df, safe_sql)

    args, _ = mock_spark.sql.call_args
    assert safe_sql in args[0]

def test_escaped_quote_exploit_blocked():
    mock_spark = MagicMock()
    processor = DataProcessor(mock_spark)
    mock_df = MagicMock()

    # The payload attempts to use an escaped backslash to confuse the parser into thinking the quote is escaped.
    # We want the SQL string to contain: SELECT 'safe \\'; DROP ...
    # This means the first backslash escapes the second. The quote is NOT escaped.
    # So the string ends. The ; is a command separator.
    # In Python, to get two backslashes in a string, we need four backslashes.
    malicious_sql = "SELECT 'safe \\\\'; DROP TABLE students; --'"

    with pytest.raises(SqlInjectionException) as excinfo:
        processor.create_temp_table_and_resultant_df(mock_df, malicious_sql)

    assert "Semicolon (;) not allowed" in str(excinfo.value)

def test_odd_backslashes_escape_quote():
    mock_spark = MagicMock()
    processor = DataProcessor(mock_spark)
    mock_df = MagicMock()

    # 3 backslashes before quote: \\\'
    # In Python string: "\\\\\\'"
    # SQL sees: \\\'
    # 1st \ escapes 2nd \. 3rd \ escapes '.
    # Quote is escaped. String continues.
    # The ; following it is inside the string.
    # Should NOT raise exception.
    safe_sql = "SELECT 'safe \\\\\\'; ignore this semi'"

    processor.create_temp_table_and_resultant_df(mock_df, safe_sql)
