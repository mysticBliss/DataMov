import os
import json
import pytest
from unittest.mock import patch, MagicMock
from datamov.core.config_reader import ConfigReader

def test_config_reader_valid_directory(tmp_path):
    # Create a dummy json file
    d = tmp_path / "subdir"
    d.mkdir()
    p = d / "test.json"
    p.write_text('{"key": "value"}')

    reader = ConfigReader(str(d))
    data = reader.get_json_data()

    assert "test.json" in data
    assert data["test.json"] == {"key": "value"}

def test_config_reader_non_existent_directory():
    # This should not raise an exception now, but log a warning
    with patch("datamov.core.config_reader.ConfigReader.logger") as mock_logger:
        reader = ConfigReader("non_existent_directory_12345")
        data = reader.get_json_data()
        assert data == {}
        # Check if warning was called with expected message
        mock_logger.warning.assert_called_with("Directory non_existent_directory_12345 does not exist. Skipping.")

def test_config_reader_not_a_directory(tmp_path):
    p = tmp_path / "file.txt"
    p.write_text("content")

    with patch("datamov.core.config_reader.ConfigReader.logger") as mock_logger:
        reader = ConfigReader(str(p))
        data = reader.get_json_data()
        assert data == {}
        # Check if warning was called with expected message
        mock_logger.warning.assert_called_with(f"Path {str(p)} is not a directory. Skipping.")
