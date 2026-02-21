import json
import ast
from datetime import date, timedelta
import time
import uuid
from typing import List, Dict, Any, Optional

from ...core.logger import Logger

logger = Logger().get_logger()

class DataFlow:
    def __init__(self, **kwargs: Any):
        self.name: Optional[str] = None
        self.description: Optional[str] = None
        self.active: bool = False
        self.source_execution_date: Optional[str] = None
        self.source_frequency_value: Optional[int] = None
        self.source_frequency_unit: Optional[str] = None
        self.source_type: Optional[str] = None
        self.source_format: Optional[str] = None
        self.source_table: Optional[str] = None
        self.source_sql: Optional[str] = None
        self.source_partition_column: Optional[str] = None
        self.source_fs_path: Optional[str] = None
        self.source_data_format: Optional[str] = None
        self.destination_type: Optional[str] = None
        self.destination_mode: Optional[str] = None
        self.destination_table: Optional[str] = None
        self.destination_partitions: List[str] = []
        self.destination_fs_path: Optional[str] = None
        self.destination_fs_func: Optional[str] = None
        self.destination_path: Optional[str] = None
        self.destination_sql: Optional[str] = None
        self.expectations: List[Dict[str, Any]] = []

        for key, value in kwargs.items():
            if value is not None:
                setattr(self, key, value)

    @staticmethod
    def _subtract_month(d: date, months: int) -> date:
        y, m = divmod(d.month - months - 1, 12)
        return date(d.year + y, m + 1, d.day)

    @classmethod
    def generate_tracking_id(cls) -> str:
        return "{}-{}".format(uuid.uuid4(), str(int(time.time())))


    def to_dict(self) -> Dict[str, Any]:
        _dict = vars(self)
        logger.debug("DataFlow to Dictionary: {}".format(_dict))
        return _dict


    def _generate_dates(self) -> List[date]:
        today = date.today()
        dates: List[date] = []

        if self.source_frequency_unit == 'days':
            dates = [today - timedelta(days=x + 1)
                    for x in range(self.source_frequency_value)]

        elif self.source_frequency_unit == 'months':
            dates = [self._subtract_month(today, x + 1)
                    for x in range(self.source_frequency_value)]
        else:
            raise ValueError("Invalid frequency unit provided")

        logger.debug("Generated Dates: {}".format(dates))
        return dates

    @property
    def generate_paths(self) -> List[str]:
        # If execution date is explicitly set, use it directly (as a string)
        if self.source_execution_date is not None:
            if self.source_fs_path:
                return [self.source_fs_path.format(data_format=self.source_execution_date)]
            return []

        # If no frequency config, we can't generate date-based paths unless default behavior needed
        if self.source_frequency_value is None:
             return []

        dates = self._generate_dates()
        paths = []

        is_python_expr = False
        optimized_format_str = None

        if self.source_data_format:
            # Check if it looks like a python expression involving dt
            # This heuristic prevents executing random strings (like "1 + 1") as python code,
            # effectively prioritizing "treat as string literal" unless "dt" is involved.
            if "dt" in self.source_data_format:
                is_python_expr = True
                try:
                    # Attempt AST parsing to optimize simple strftime calls
                    tree = ast.parse(self.source_data_format, mode='eval')
                    if isinstance(tree.body, ast.Call) and \
                       isinstance(tree.body.func, ast.Attribute) and \
                       tree.body.func.attr == 'strftime' and \
                       isinstance(tree.body.func.value, ast.Name) and \
                       tree.body.func.value.id == 'dt':

                        args = tree.body.args
                        if len(args) == 1:
                            if hasattr(ast, 'Constant') and isinstance(args[0], ast.Constant):
                                optimized_format_str = args[0].value # Python 3.8+
                            elif isinstance(args[0], ast.Str):
                                optimized_format_str = args[0].s # Python < 3.8
                except Exception:
                    pass

        for dt in dates:
            formatted = str(dt) # Default fallback

            if self.source_data_format:
                if is_python_expr:
                    if optimized_format_str:
                        # Fast path for recognized dt.strftime(...)
                        try:
                            formatted = dt.strftime(optimized_format_str)
                        except Exception as e:
                            logger.warning("Failed to strftime optimized format: {}. Error: {}".format(optimized_format_str, e))
                            formatted = str(dt)
                    else:
                        # Fallback to eval for complex expressions
                        try:
                            # Use restricted scope for eval, but allow builtins (implicitly via empty dict)
                            formatted = eval(self.source_data_format, {}, {"dt": dt, "date": date, "timedelta": timedelta})
                        except Exception as e:
                            logger.warning("Failed to eval source_data_format: {}. Error: {}".format(self.source_data_format, e))
                            formatted = str(dt)
                else:
                    # Treat as direct strftime format string
                    try:
                        formatted = dt.strftime(self.source_data_format)
                    except Exception as e:
                         # If it fails (unlikely for strings), fallback
                         logger.warning("Failed to use source_data_format as strftime: {}. Error: {}".format(self.source_data_format, e))
                         pass

            if self.source_fs_path:
                paths.append(self.source_fs_path.format(data_format=formatted))

        return paths

    def __repr__(self) -> str:
        return "{}({})".format(
            self.__class__.__name__,
            ', '.join(['{}={}'.format(k, v) for k, v in vars(self).items()])
        )
