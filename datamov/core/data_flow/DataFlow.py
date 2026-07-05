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

    def _safe_eval(self, expr: str, context: Dict[str, Any]) -> Any:
        try:
            tree = ast.parse(expr, mode='eval')
        except SyntaxError:
            raise ValueError(f"Invalid syntax in date expression: {expr}")

        def _eval(node):
            if isinstance(node, ast.Expression):
                return _eval(node.body)
            elif isinstance(node, ast.BinOp):
                left = _eval(node.left)
                right = _eval(node.right)
                if isinstance(node.op, ast.Add):
                    return left + right
                elif isinstance(node.op, ast.Sub):
                    return left - right
                raise ValueError(f"Unsupported operation: {type(node.op)}")
            elif isinstance(node, ast.Call):
                func = _eval(node.func)
                args = [_eval(arg) for arg in node.args]
                kwargs = {kw.arg: _eval(kw.value) for kw in node.keywords}
                return func(*args, **kwargs)
            elif isinstance(node, ast.Attribute):
                value = _eval(node.value)
                if node.attr in ['strftime', 'year', 'month', 'day', 'date', 'timedelta']:
                    return getattr(value, node.attr)
                raise ValueError(f"Unsupported attribute: {node.attr}")
            elif isinstance(node, ast.Name):
                if node.id in context:
                    return context[node.id]
                raise ValueError(f"Unsupported name: {node.id}")
            elif isinstance(node, ast.Constant):
                return node.value
            else:
                raise ValueError(f"Unsupported node type: {type(node)}")

        return _eval(tree.body)

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
            optimized_format = None

            if self.source_data_format:
                try:
                    tree = ast.parse(self.source_data_format, mode='eval')
                    if isinstance(tree.body, ast.Call) and \
                       isinstance(tree.body.func, ast.Attribute) and \
                       tree.body.func.attr == 'strftime' and \
                       isinstance(tree.body.func.value, ast.Name) and \
                       tree.body.func.value.id == 'dt':
                        args = tree.body.args
                        if len(args) == 1:
                            if hasattr(ast, 'Constant') and isinstance(args[0], ast.Constant):
                                optimized_format = args[0].value
                            elif hasattr(ast, 'Str') and isinstance(args[0], ast.Str):
                                optimized_format = args[0].s
                except Exception:
                    pass

            for dt in dates:
                if self.source_data_format:
                    if optimized_format:
                        try:
                            formatted = dt.strftime(optimized_format)
                        except Exception as e:
                            logger.warning("Failed to strftime source_data_format: {}. Error: {}".format(self.source_data_format, e))
                            formatted = str(dt)
                    else:
                        # Safe(r) eval or raw strftime
                        try:
                            if any(x in self.source_data_format for x in ['dt', 'date', 'timedelta']):
                                formatted = self._safe_eval(self.source_data_format, {"dt": dt, "date": date, "timedelta": timedelta})
                            else:
                                if '%' in self.source_data_format:
                                    formatted = dt.strftime(self.source_data_format)
                                else:
                                    formatted = self.source_data_format
                        except Exception as eval_e:
                            if '%' in self.source_data_format:
                                try:
                                    formatted = dt.strftime(self.source_data_format)
                                except Exception:
                                    logger.warning("Failed to eval or strftime source_data_format: {}. Error: {}".format(self.source_data_format, eval_e))
                                    formatted = str(dt)
                            else:
                                logger.warning("Failed to eval source_data_format: {}. Error: {}".format(self.source_data_format, eval_e))
                                formatted = self.source_data_format
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
