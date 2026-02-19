import sys
from unittest.mock import MagicMock, patch
import pytest
from datetime import date, timedelta

# Mock dependencies
sys.modules["pyspark"] = MagicMock()
sys.modules["pyspark.sql"] = MagicMock()
sys.modules["pyspark.sql.utils"] = MagicMock()
sys.modules["pyspark.sql.functions"] = MagicMock()
sys.modules["pyspark.sql.types"] = MagicMock()
sys.modules["great_expectations"] = MagicMock()
sys.modules["great_expectations.dataset"] = MagicMock()

from datamov.core.data_flow.DataFlow import DataFlow

class TestOptimization:
    def test_simple_strftime_optimized(self):
        """Test simple strftime which should be optimized and NOT call eval."""
        config = {
            "source_execution_date": None,
            "source_frequency_value": 3,
            "source_frequency_unit": "days",
            "source_fs_path": "{data_format}",
            "source_data_format": "dt.strftime('%Y-%m-%d')"
        }
        df = DataFlow(**config)

        # Patch eval to track calls
        with patch('builtins.eval', wraps=eval) as mock_eval:
            paths = df.generate_paths

            # Verify result
            today = date.today()
            expected_dates = [
                (today - timedelta(days=x+1)).strftime('%Y-%m-%d')
                for x in range(3)
            ]
            assert paths == expected_dates

            # Verify eval was NOT called for data formatting
            # Note: eval might be called elsewhere, so we check arguments
            # The original code calls eval(self.source_data_format, ...)
            # We want to ensure it is NOT called with that specific format string

            # Check if any call matches
            was_called_with_format = False
            for call in mock_eval.call_args_list:
                args, _ = call
                if args[0] == config["source_data_format"]:
                    was_called_with_format = True
                    break

            # With optimization, it should be False. Currently (pre-opt), it should be True.
            # So if we run this test before optimization, we expect True.
            # After optimization, we expect False.

            # With optimization, eval should NOT be called for simple strftime
            assert not was_called_with_format, "eval should NOT be called (post-optimization)"

    def test_complex_expression_fallback(self):
        """Test complex expression which should use eval fallback."""
        config = {
            "source_execution_date": None,
            "source_frequency_value": 3,
            "source_frequency_unit": "days",
            "source_fs_path": "{data_format}",
            "source_data_format": "(dt - timedelta(days=1)).strftime('%Y-%m-%d')"
        }
        df = DataFlow(**config)

        with patch('builtins.eval', wraps=eval) as mock_eval:
            paths = df.generate_paths

            today = date.today()
            expected_dates = [
                ((today - timedelta(days=x+1)) - timedelta(days=1)).strftime('%Y-%m-%d')
                for x in range(3)
            ]
            assert paths == expected_dates

            # Verify eval WAS called
            was_called_with_format = False
            for call in mock_eval.call_args_list:
                args, _ = call
                if args[0] == config["source_data_format"]:
                    was_called_with_format = True
                    break
            assert was_called_with_format, "eval should be called for complex expressions"

    def test_invalid_format_fallback(self):
        """Test invalid format which should fallback."""
        config = {
            "source_execution_date": None,
            "source_frequency_value": 1,
            "source_frequency_unit": "days",
            "source_fs_path": "{data_format}",
            "source_data_format": "dt.strftime(1)" # Invalid argument type, raises TypeError
        }
        df = DataFlow(**config)
        paths = df.generate_paths

        today = date.today()
        dt = today - timedelta(days=1)
        # The code catches exception and falls back to str(dt)
        assert paths == [str(dt)]
