import sys
from unittest.mock import MagicMock

# Try to import great_expectations, mock if missing
try:
    import great_expectations
except ImportError:
    if "great_expectations" not in sys.modules:
        sys.modules["great_expectations"] = MagicMock()

# Try to import pyspark, mock if missing
try:
    import pyspark
except ImportError:
    if "pyspark" not in sys.modules:
        # Create mocks for pyspark and submodules
        pyspark_mock = MagicMock()
        pyspark_sql_mock = MagicMock()

        sys.modules["pyspark"] = pyspark_mock
        sys.modules["pyspark.sql"] = pyspark_sql_mock
        sys.modules["pyspark.sql.functions"] = MagicMock()
        sys.modules["pyspark.sql.types"] = MagicMock()
        sys.modules["pyspark.sql.utils"] = MagicMock()
        sys.modules["pyspark.sql.catalog"] = MagicMock()

        # Configure a DataFrame mock that returns an int for count()
        # This is needed to pass integration tests in environments without pyspark
        df_mock = MagicMock()
        df_mock.count.return_value = 10

        # Configure SparkSession mock
        spark_session_mock = MagicMock()
        spark_session_mock.createDataFrame.return_value = df_mock
        spark_session_mock.sql.return_value = df_mock

        # Configure spark.read.format(...).options(...).load(...)
        read_mock = MagicMock()
        read_mock.format.return_value.options.return_value.load.return_value = df_mock
        spark_session_mock.read = read_mock

        # Configure SparkSession.builder chain
        builder_mock = MagicMock()
        builder_mock.master.return_value = builder_mock
        builder_mock.appName.return_value = builder_mock
        builder_mock.config.return_value = builder_mock
        builder_mock.enableHiveSupport.return_value = builder_mock
        builder_mock.getOrCreate.return_value = spark_session_mock

        # Attach the builder mock to the pyspark.sql.SparkSession class
        pyspark_sql_mock.SparkSession.builder = builder_mock
