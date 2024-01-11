from ..config_reader import ConfigReader
from ..data_flow import DataFlow
from ...core.logger import Logger


logger = Logger().get_logger()

class EnvironmentConfig:
    """
    Stores details related to a specific environment.

    Args:
        environment (str): Name of the environment.
        driver_details (dict): Details related to the driver.
        kudu_masters (list): List of Kudu masters for the environment.
    """

    def __init__(self, environment, driver_details, kudu_masters):
        """
        Initializes an EnvironmentConfig instance.

        Args:
            environment (str): Name of the environment.
            driver_details (dict): Details related to the driver.
            kudu_masters (list): List of Kudu masters for the environment.
        """

        self.environment = environment
        self.driver_details = driver_details
        self.kudu_masters = kudu_masters

    def __repr__(self):
        """
        Returns a string representation of the EnvironmentConfig instance.

        Returns:
            str: A formatted string representation of the EnvironmentConfig instance.
        """

        return "EnvironmentConfig(environment={}, driver_details={}, kudu_masters={})".format(
            self.environment, self.driver_details, self.kudu_masters
        )



class DataMovements:
    def __init__(self, active_only=False):
        self.configs = ConfigReader()
        self.data_movements = {}
        self.environments = {}
        self.active_only = active_only
        self.load_data_movements()
        self.load_environment_config()

    def load_data_movements(self):
        json_data = self.configs.get_json_data()
        for filename, data in json_data.items():
            logger.info("Found file: {}".format(filename))
            logger.info("Data movement Files should start as : data_movements_*.json \n Envrironment as evironment_*.json")
            if filename.startswith('data_movements_'):
                if 'data_movements' not in data:
                    logger.info("Error: 'data_movements' not found in {}.".format(filename))
                    continue

                movements_config = data['data_movements']
                if isinstance(movements_config, list):
                    for movement_data in movements_config:
                        if self.active_only and movement_data.get('active', False) != True:
                            continue
                        movement = DataFlow(**movement_data)
                        self.data_movements[movement.name] = movement    
            
    def load_environment_config(self):
        json_data = self.configs.get_json_data()
        for filename, data in json_data.items():
            if filename.startswith('environment_'):
                if 'environment_configs' not in data:
                    print("Error: 'environment_configs' not found in {}.".format(filename))
                    continue

                environment_configs = data['environment_configs']
                if isinstance(environment_configs, list):
                    for environment_data in environment_configs:
                        environment = EnvironmentConfig(**environment_data)
                        self.environments[environment.environment] = environment

    @property
    def get_data_movements(self):
        return self.data_movements

    @property
    def get_environment_configs(self):
        return self.environments