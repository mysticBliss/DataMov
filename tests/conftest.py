import sys
from unittest.mock import MagicMock

# Mock pyspark and other dependencies globally for all tests
# We forcibly mock pyspark because running tests with mixed real/mocked pyspark
# in environments with partial installations (like CI) causes issues.
sys.modules["pyspark"] = MagicMock()
sys.modules["pyspark.sql"] = MagicMock()
sys.modules["pyspark.sql.functions"] = MagicMock()
sys.modules["pyspark.sql.types"] = MagicMock()
sys.modules["pyspark.sql.utils"] = MagicMock()
sys.modules["pyspark.sql.catalog"] = MagicMock()

if "great_expectations" not in sys.modules:
    sys.modules["great_expectations"] = MagicMock()
