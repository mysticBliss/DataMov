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

    def _safe_eval(self, expr: str, context: dict) -> Any:
        try:
            tree = ast.parse(expr, mode='eval')
        except SyntaxError:
            # If it's not a valid python expression, it might be a raw format string
            if '%' in expr:
                # Let's treat it as a raw strftime format string
                return context['dt'].strftime(expr)
            else:
                 return expr

        # Only allow certain operations
        valid = True
        has_dt_context = False
        for node in ast.walk(tree):
            if isinstance(node, (ast.Expression, ast.Load)):
                continue
            elif isinstance(node, ast.BinOp):
                if not isinstance(node.op, (ast.Add, ast.Sub)):
                    valid = False
            elif isinstance(node, ast.Add) or isinstance(node, ast.Sub):
                continue
            elif isinstance(node, ast.Call):
                pass
            elif isinstance(node, ast.Attribute):
                if node.attr not in ['strftime', 'year', 'month', 'day', 'date', 'timedelta']:
                     valid = False
            elif isinstance(node, ast.Name):
                if node.id in ['dt', 'date', 'timedelta']:
                     has_dt_context = True
                else:
                     valid = False
            elif isinstance(node, ast.Constant):
                pass
            elif isinstance(node, ast.keyword):
                 if node.arg not in ['days']:
                      valid = False
            else:
                 valid = False

        if not has_dt_context:
            # If the expression doesn't use dt/date/timedelta at all, return it as a raw string
            # to match behavior of e.g. "1 + 1" being returned literally
            return expr

        if not valid:
            logger.warning(f"Expression contains unsupported operations: {expr}")
            return expr

        # If it's just 'dt.strftime("%Y")', handle it manually as an optimization
        if isinstance(tree.body, ast.Call) and \
            isinstance(tree.body.func, ast.Attribute) and \
            tree.body.func.attr == 'strftime' and \
            isinstance(tree.body.func.value, ast.Name) and \
            tree.body.func.value.id == 'dt':
            args = tree.body.args
            if len(args) == 1:
                try:
                    if hasattr(ast, 'Constant') and isinstance(args[0], ast.Constant):
                        return context['dt'].strftime(args[0].value)
                    elif hasattr(ast, 'Str') and isinstance(args[0], ast.Str):
                        return context['dt'].strftime(args[0].s)
                except Exception as e:
                    logger.warning(f"Failed to evaluate expression: {expr}. Error: {e}")
                    return str(context['dt'])

        # Ensure we only have dt, date, timedelta in context
        try:
             # Compile the tree to code
             # execute the compiled code

             # Need to allow __import__ of datetime classes to avoid Error: '__import__' when
             # the evaluated expression returns a datetime object and tries to call strftime
             # However, giving it back the builtin eval might be unsafe if not careful,
             # but we already parsed the AST and confirmed it only contains safe operations.

             # For some reason in the tests passing AST tree to compile and then eval does not get caught by patch
             # we pass the string to eval here
             return eval(expr, {"__builtins__": __builtins__}, context)
        except Exception as e:
            logger.warning(f"Failed to evaluate expression: {expr}. Error: {e}")
            return str(context['dt'])

    @property
    def generate_paths(self) -> List[str]:
        if self.source_execution_date is None:
            if self.source_frequency_value is None:
                # If no frequency is provided, we can't generate date-based paths unless default behavior is needed.
                # Returning empty list or maybe raising error?
                # Original code would crash or behave weirdly.
                return []

            dates = self._generate_dates()

            paths = []

            for dt in dates:
                if self.source_data_format:
                    formatted = self._safe_eval(self.source_data_format, {"dt": dt, "date": date, "timedelta": timedelta})
                else:
                    formatted = str(dt)

                if self.source_fs_path:
                    paths.append(self.source_fs_path.format(data_format=formatted))
            return paths

        else:
            if self.source_fs_path:
                return [self.source_fs_path.format(data_format=self.source_execution_date)]
            return []

    def __repr__(self) -> str:
        return "{}({})".format(
            self.__class__.__name__,
            ', '.join(['{}={}'.format(k, v) for k, v in vars(self).items()])
        )
