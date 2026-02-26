from ..config_reader import ConfigReader
from ..data_flow import DataFlow
from ...core.logger import Logger
from typing import Dict, Any, List, Optional


logger = Logger().get_logger()

class EnvironmentConfig:
    """
    Stores details related to a specific environment.

    Args:
        environment (str): Name of the environment.
        driver_details (dict): Details related to the driver.
        kudu_masters (list): List of Kudu masters for the environment.
    """

    def __init__(self, environment: str, driver_details: Dict[str, Any], kudu_masters: List[str]):
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

    def __repr__(self) -> str:
        """
        Returns a string representation of the EnvironmentConfig instance.

        Returns:
            str: A formatted string representation of the EnvironmentConfig instance.
        """

        return "EnvironmentConfig(environment={}, driver_details=<REDACTED>, kudu_masters={})".format(
            self.environment, self.kudu_masters
        )



class DataMovements:
    def __init__(self, active_only: bool = False):
        self.configs = ConfigReader()
        self._data_movements: Dict[str, DataFlow] = {}
        self._environments: Dict[str, EnvironmentConfig] = {}
        self.active_only = active_only
        self.load_configs()

    def load_configs(self) -> None:
        json_data = self.configs.get_json_data()
        for filename, data in json_data.items():
            logger.info("Found file: {}".format(filename))
            logger.info("Data movement Files should start as : data_movements_*.json \n Envrironment as evironment_*.json")

            if filename.startswith('data_movements_'):
                self._process_data_movements(filename, data)
            elif filename.startswith('environment_'):
                self._process_environment_config(filename, data)

    def _process_data_movements(self, filename: str, data: Dict[str, Any]) -> None:
        if 'data_movements' not in data:
            logger.info("Error: 'data_movements' not found in {}.".format(filename))
            return

        movements_config = data['data_movements']
        if isinstance(movements_config, list):
            for movement_data in movements_config:
                if self.active_only and movement_data.get('active', False) != True:
                    continue
                movement = DataFlow(**movement_data)
                if movement.name:
                    self.data_movements[movement.name] = movement

    def _process_environment_config(self, filename: str, data: Dict[str, Any]) -> None:
        if 'environment_configs' not in data:
            logger.error("Error: 'environment_configs' not found in {}.".format(filename))
            return

        environment_configs = data['environment_configs']
        if isinstance(environment_configs, list):
            for environment_data in environment_configs:
                environment = EnvironmentConfig(**environment_data)
                self._environments[environment.environment] = environment

    def load_data_movements(self) -> None:
        """Deprecated: use load_configs instead."""
        self.load_configs()

    def load_environment_config(self) -> None:
        """Deprecated: use load_configs instead."""
        self.load_configs()

    @property
    def data_movements(self) -> Dict[str, DataFlow]:
        return self._data_movements

    @property
    def environment_configs(self) -> Dict[str, EnvironmentConfig]:
        return self._environments
