import os
import json
import pytest
from unittest.mock import patch
from datamov.core.config_reader.ConfigReader import ConfigReader

def test_init_valid_directory(tmp_path):
    """Test successful initialization and data loading with valid JSON files."""
    d = tmp_path / "configs"
    d.mkdir()
    p = d / "config.json"
    content = {"key": "value"}
    p.write_text(json.dumps(content))

    reader = ConfigReader(str(d))
    assert reader.get_json_data() == {"config.json": content}

def test_init_mixed_files(tmp_path):
    """Test that non-JSON files are ignored."""
    d = tmp_path / "configs"
    d.mkdir()

    # JSON file
    p1 = d / "config.json"
    p1.write_text(json.dumps({"a": 1}))

    # Text file
    p2 = d / "notes.txt"
    p2.write_text("some notes")

    reader = ConfigReader(str(d))
    data = reader.get_json_data()
    assert "config.json" in data
    assert "notes.txt" not in data

def test_init_invalid_json(tmp_path):
    """Test that json.JSONDecodeError is raised for malformed JSON files."""
    d = tmp_path / "configs"
    d.mkdir()
    p = d / "bad.json"
    p.write_text("{invalid json")

    with pytest.raises(json.JSONDecodeError):
        ConfigReader(str(d))

def test_init_file_permission_error(tmp_path):
    """Test that IOError is raised when opening a file fails."""
    d = tmp_path / "configs"
    d.mkdir()
    p = d / "config.json"
    p.write_text("{}")

    # We patch builtins.open to raise an IOError.
    # Note: os.listdir will still work as it doesn't use open().
    with patch("builtins.open", side_effect=IOError("Permission denied")):
        with pytest.raises(IOError):
            ConfigReader(str(d))

def test_init_directory_not_found():
    """Test that FileNotFoundError is raised when the directory does not exist."""
    # Using a path that is very unlikely to exist
    non_existent_path = "/path/to/non/existent/directory/unlikely/to/exist"
    with pytest.raises(FileNotFoundError):
        ConfigReader(non_existent_path)

def test_get_json_data(tmp_path):
    """Test that get_json_data returns the correct dictionary."""
    d = tmp_path / "configs"
    d.mkdir()
    p = d / "data.json"
    data = {"foo": "bar"}
    p.write_text(json.dumps(data))

    reader = ConfigReader(str(d))
    assert reader.get_json_data() == {"data.json": data}
