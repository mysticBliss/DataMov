from pyspark.sql import SparkSession
from typing import Optional, Dict, Any
from types import TracebackType

from ...core.logger import Logger


logger = Logger().get_logger()


class SparkManager:
    """
    This is a class for managing Spark sessions.
    It provides methods for starting and stopping a Spark session, setting the log level, and adding configuration.
    """

    def __init__(self, app_name: str, config: Optional[Dict[str, Any]] = None):
        self.app_name = app_name
        self.config = config if config else {}
        self.spark: Optional[SparkSession] = None

    def __enter__(self) -> SparkSession:
        try:
            builder = SparkSession.builder.appName(
                self.app_name).enableHiveSupport()
            for key, value in self.config.items():
                builder.config(key, value)

            self.spark = builder.getOrCreate()
            logger.info("Spark session started successfully.")
            return self.spark
        except Exception as e:
            logger.error(
                "Error occurred while initializing Spark: %s", str(e))
            raise

    def __exit__(self, exc_type: Optional[type], exc_value: Optional[BaseException], traceback: Optional[TracebackType]):
        try:
            if self.spark:
                self.spark.stop()
                logger.info("Spark session stopped successfully.")
        except Exception as e:
            logger.error("Error occurred while stopping Spark: %s", str(e))
        finally:
            self.spark = None
