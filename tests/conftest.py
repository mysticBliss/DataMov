import sys
from unittest.mock import MagicMock

# Mock pyspark and other dependencies globally for all tests
# This is required because datamov's internal imports are tightly coupled with pyspark
try:
    import pyspark
    # Try importing submodules to ensure they are working,
    # as some environments might have a broken pyspark installation.
    # Specifically check for pyspark.testing as it caused issues in CI.
    import pyspark.sql
    import pyspark.sql.functions
    import pyspark.testing
except (ImportError, ModuleNotFoundError):
    # If pyspark is missing or broken, mock it
    sys.modules["pyspark"] = MagicMock()
    sys.modules["pyspark.sql"] = MagicMock()
    sys.modules["pyspark.sql.functions"] = MagicMock()
    sys.modules["pyspark.sql.types"] = MagicMock()
    sys.modules["pyspark.sql.utils"] = MagicMock()
    sys.modules["pyspark.sql.catalog"] = MagicMock()

if "great_expectations" not in sys.modules:
    sys.modules["great_expectations"] = MagicMock()
