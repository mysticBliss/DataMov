import os
import json

class ConfigReader:
    def __init__(self, directory=None):
        self.directory = directory if directory else "."
        self.json_data = {}
        self.post_init()

    def post_init(self):
        self.read_json_files()

    def read_json_files(self):
        for filename in os.listdir(self.directory):
            if filename.endswith('.json'):
                file_path = os.path.join(self.directory, filename)
                with open(file_path, 'r') as file:
                    self.json_data[filename] = json.load(file)

    def get_json_data(self):
        return self.json_data