import os
import json
from ..logger import Logger

logger = Logger().get_logger()

class ConfigReader:
    def __init__(self, directory=None):
        self.directory = directory if directory else "."
        self.json_data = {}
        self.post_init()

    def post_init(self):
        self.read_json_files()

    def read_json_files(self):
        if not os.path.exists(self.directory):
            logger.warning("Directory {} does not exist. Skipping.".format(self.directory))
            return

        if not os.path.isdir(self.directory):
            logger.warning("Path {} is not a directory. Skipping.".format(self.directory))
            return

        for filename in os.listdir(self.directory):
            if filename.endswith('.json'):
                file_path = os.path.join(self.directory, filename)
                with open(file_path, 'r') as file:
                    self.json_data[filename] = json.load(file)

    def get_json_data(self):
        return self.json_data
