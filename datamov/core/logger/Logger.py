import logging

class Logger:
    """Provides a logging utility."""

    def __init__(self):
        """
        Initializes the Logger object with a logger instance, sets the logging level to INFO,
        and adds a stream handler with a specific logging format.
        """
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        self.logger.addHandler(ch)

    def get_logger(self):
        """
        Returns the logger instance.

        Returns:
            logging.Logger: The configured logger instance.
        """
        return self.logger
    
    def set_log_level(self, log_level):
        logging.setLevel(log_level)

    def add_config(self, key, value):
        self.config[key] = value