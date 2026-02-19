import sys
from unittest.mock import MagicMock

# Try to import pyspark first. If it fails, mock it.
# This prevents overwriting the real pyspark if it's installed (e.g. in CI/CD).
try:
    import pyspark
    import pyspark.sql
    import pyspark.sql.functions
    import pyspark.sql.types
    import pyspark.sql.utils
    import pyspark.sql.catalog
except ImportError:
    if "pyspark" not in sys.modules:
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
