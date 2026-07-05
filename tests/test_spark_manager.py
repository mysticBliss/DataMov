import pytest
from unittest.mock import patch, MagicMock

from datamov.connectors.spark_manager.SparkManager import SparkManager

def test_spark_manager_enter_success():
    config = {"spark.executor.memory": "1g"}
    manager = SparkManager(app_name="TestApp", config=config)

    with patch("datamov.connectors.spark_manager.SparkManager.SparkSession") as mock_spark_session:
        mock_builder = MagicMock()
        mock_spark_session.builder = mock_builder
        mock_builder.appName.return_value = mock_builder
        mock_builder.enableHiveSupport.return_value = mock_builder
        mock_builder.config.return_value = mock_builder

        mock_spark = MagicMock()
        mock_builder.getOrCreate.return_value = mock_spark

        with manager as spark:
            assert spark == mock_spark

        mock_builder.appName.assert_called_once_with("TestApp")
        mock_builder.enableHiveSupport.assert_called_once()
        mock_builder.config.assert_called_once_with("spark.executor.memory", "1g")
        mock_builder.getOrCreate.assert_called_once()
        mock_spark.stop.assert_called_once()

def test_spark_manager_enter_error():
    manager = SparkManager(app_name="TestApp")

    with patch("datamov.connectors.spark_manager.SparkManager.SparkSession") as mock_spark_session:
        mock_builder = MagicMock()
        mock_spark_session.builder = mock_builder
        mock_builder.appName.return_value = mock_builder
        mock_builder.enableHiveSupport.return_value = mock_builder

        expected_error = Exception("Spark initialization failed")
        mock_builder.getOrCreate.side_effect = expected_error

        with patch("datamov.connectors.spark_manager.SparkManager.logger") as mock_logger:
            with pytest.raises(Exception) as exc_info:
                with manager:
                    pass

            assert exc_info.value == expected_error
            mock_logger.error.assert_called_once_with(
                "Error occurred while initializing Spark: %s", "Spark initialization failed"
            )

def test_spark_manager_exit_error():
    manager = SparkManager(app_name="TestApp")

    with patch("datamov.connectors.spark_manager.SparkManager.SparkSession") as mock_spark_session:
        mock_builder = MagicMock()
        mock_spark_session.builder = mock_builder
        mock_builder.appName.return_value = mock_builder
        mock_builder.enableHiveSupport.return_value = mock_builder

        mock_spark = MagicMock()
        mock_builder.getOrCreate.return_value = mock_spark

        mock_spark.stop.side_effect = Exception("Spark stop failed")

        with patch("datamov.connectors.spark_manager.SparkManager.logger") as mock_logger:
            with manager:
                pass

            mock_logger.error.assert_called_once_with(
                "Error occurred while stopping Spark: %s", "Spark stop failed"
            )
            assert manager.spark is None
