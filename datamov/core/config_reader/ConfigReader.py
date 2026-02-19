import os
import json
import re
from ..logger import Logger

class ConfigReader:
    def __init__(self, directory=None, file_pattern=None):
        self.directory = directory if directory else "."
        self.json_data = {}
        self.file_pattern = re.compile(file_pattern) if file_pattern else None
        self.logger = Logger().get_logger()
        self.post_init()

    def post_init(self):
        self.read_json_files()

    def read_json_files(self):
        for filename in os.listdir(self.directory):
            if self.file_pattern:
                if not self.file_pattern.match(filename):
                    continue
            elif not filename.endswith('.json'):
                continue

            file_path = os.path.join(self.directory, filename)
            try:
                with open(file_path, 'r') as file:
                    self.json_data[filename] = json.load(file)
            except (json.JSONDecodeError, IOError) as e:
                self.logger.error(f"Error reading {filename}: {e}")

    def get_json_data(self):
        return self.json_data
