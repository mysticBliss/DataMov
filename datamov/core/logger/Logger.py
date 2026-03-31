import logging
from typing import Optional, Any

class Logger:
    """Provides a logging utility."""

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Logger, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        """
        Initializes the Logger object with a logger instance, sets the logging level to INFO,
        and adds a stream handler with a specific logging format.
        """
        if self._initialized:
            return

        self.logger = logging.getLogger("DataMov")
        self.logger.setLevel(logging.INFO)

        if not self.logger.handlers:
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            ch = logging.StreamHandler()
            ch.setFormatter(formatter)
            self.logger.addHandler(ch)

        if not hasattr(self, 'config'):
            self.config = {}
        self._initialized = True

    def get_logger(self) -> logging.Logger:
        """
        Returns the logger instance.

        Returns:
            logging.Logger: The configured logger instance.
        """
        return self.logger

    def set_log_level(self, log_level: int) -> None:
        self.logger.setLevel(log_level)

    def add_config(self, key: str, value: Any) -> None:
        if not hasattr(self, 'config'):
            self.config = {}
        self.config[key] = value
