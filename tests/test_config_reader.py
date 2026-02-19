import os
import json
import tempfile
import shutil
import unittest
from datamov.core.config_reader.ConfigReader import ConfigReader

class TestConfigReader(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.test_file = os.path.join(self.test_dir, 'test_config.json')
        with open(self.test_file, 'w') as f:
            json.dump({'key': 'value'}, f)

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_read_json_files(self):
        reader = ConfigReader(self.test_dir)
        data = reader.get_json_data()
        self.assertIn('test_config.json', data)
        self.assertEqual(data['test_config.json']['key'], 'value')

    def test_read_empty_directory(self):
        empty_dir = tempfile.mkdtemp()
        try:
            reader = ConfigReader(empty_dir)
            data = reader.get_json_data()
            self.assertEqual(data, {})
        finally:
            shutil.rmtree(empty_dir)
