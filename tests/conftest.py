import sys
from unittest.mock import MagicMock
import pytest

# Ensure mocks are injected BEFORE any imports happen during collection
if "pyspark" not in sys.modules:
    # Create a mock pyspark module structure
    # Must assign mocks to sub-attributes so import works
    pyspark = MagicMock()
    pyspark.sql = MagicMock()
    pyspark.sql.types = MagicMock()
    pyspark.sql.functions = MagicMock()
    pyspark.sql.utils = MagicMock()

    # AnalysisException needs to be an Exception class for try/except blocks
    class MockAnalysisException(Exception):
        pass
    pyspark.sql.utils.AnalysisException = MockAnalysisException

    sys.modules["pyspark"] = pyspark
    sys.modules["pyspark.sql"] = pyspark.sql
    sys.modules["pyspark.sql.types"] = pyspark.sql.types
    sys.modules["pyspark.sql.functions"] = pyspark.sql.functions
    sys.modules["pyspark.sql.utils"] = pyspark.sql.utils

if "great_expectations" not in sys.modules:
    sys.modules["great_expectations"] = MagicMock()

@pytest.fixture(autouse=True)
def safe_lit_patch(monkeypatch):
    """
    Safely patches pyspark.sql.functions.lit to handle cases where
    SparkContext._active_spark_context is None (mocked environment).
    """
    try:
        # If pyspark is actually installed and imported, try to patch it.
        # But if it's mocked by sys.modules above, this import will just get the mock.
        import pyspark.sql.functions as F

        # Check if it's a real module or a mock
        if hasattr(F, "lit") and callable(F.lit) and not isinstance(F.lit, MagicMock):
             # If it's real code, we might want to patch it.
             # If it's a MagicMock, we don't need to patch it because MagicMock returns another MagicMock
             # which is fine.
             pass

    except ImportError:
        pass
