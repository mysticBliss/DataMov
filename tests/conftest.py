import sys
from unittest.mock import MagicMock

# Mock pyspark and other dependencies globally for all tests
# This is required because datamov's internal imports are tightly coupled with pyspark
if "pyspark" not in sys.modules:
    sys.modules["pyspark"] = MagicMock()
    sys.modules["pyspark.sql"] = MagicMock()
    sys.modules["pyspark.sql.functions"] = MagicMock()
    sys.modules["pyspark.sql.types"] = MagicMock()
    sys.modules["pyspark.sql.utils"] = MagicMock()
    sys.modules["pyspark.sql.catalog"] = MagicMock()
if "great_expectations" not in sys.modules:
    sys.modules["great_expectations"] = MagicMock()
