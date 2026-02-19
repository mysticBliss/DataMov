import sys
from unittest.mock import MagicMock

# Try to import pyspark first to see if it's available
try:
    import pyspark
except ImportError:
    # Only mock if not available
    sys.modules["pyspark"] = MagicMock()
    sys.modules["pyspark.sql"] = MagicMock()
    sys.modules["pyspark.sql.functions"] = MagicMock()
    sys.modules["pyspark.sql.types"] = MagicMock()
    sys.modules["pyspark.sql.utils"] = MagicMock()
    sys.modules["pyspark.sql.catalog"] = MagicMock()

try:
    import great_expectations
except ImportError:
    if "great_expectations" not in sys.modules:
        sys.modules["great_expectations"] = MagicMock()
