import os
import json
import pytest
from datamov.core.config_reader.ConfigReader import ConfigReader

def test_read_valid_json_files(tmp_path):
    """Test reading valid JSON files from a directory."""
    d = tmp_path / "config"
    d.mkdir()
    p = d / "data.json"
    content = {"key": "value"}
    p.write_text(json.dumps(content))

    reader = ConfigReader(str(d))
    assert reader.get_json_data() == {"data.json": content}

def test_read_empty_directory(tmp_path):
    """Test reading from an empty directory."""
    d = tmp_path / "empty_config"
    d.mkdir()

    reader = ConfigReader(str(d))
    assert reader.get_json_data() == {}

def test_read_no_json_files(tmp_path):
    """Test reading from a directory with no JSON files."""
    d = tmp_path / "no_json"
    d.mkdir()
    p = d / "data.txt"
    p.write_text("some text")

    reader = ConfigReader(str(d))
    assert reader.get_json_data() == {}

def test_read_invalid_json_file(tmp_path):
    """Test reading a directory containing an invalid JSON file."""
    d = tmp_path / "invalid_json"
    d.mkdir()
    p = d / "bad.json"
    p.write_text("{invalid json}")

    with pytest.raises(json.JSONDecodeError):
        ConfigReader(str(d))

def test_read_non_existent_directory(tmp_path):
    """Test initializing with a non-existent directory."""
    non_existent_dir = tmp_path / "non_existent"
    with pytest.raises(FileNotFoundError):
        ConfigReader(str(non_existent_dir))

def test_read_mixed_files(tmp_path):
    """Test reading from a directory with mixed file types."""
    d = tmp_path / "mixed_config"
    d.mkdir()

    # Valid JSON
    p1 = d / "valid.json"
    content = {"valid": True}
    p1.write_text(json.dumps(content))

    # Non-JSON file
    p2 = d / "other.txt"
    p2.write_text("just text")

    reader = ConfigReader(str(d))
    expected = {"valid.json": content}
    assert reader.get_json_data() == expected
