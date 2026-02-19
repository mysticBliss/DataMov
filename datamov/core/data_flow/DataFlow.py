import json
import calendar
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
        year = d.year + y
        month = m + 1
        _, days_in_month = calendar.monthrange(year, month)
        day = min(d.day, days_in_month)
        return date(year, month, day)

    @classmethod
    def generate_tracking_id(cls) -> str:
        return "{}-{}".format(uuid.uuid4(), str(int(time.time())))


    def to_dict(self) -> Dict[str, Any]:
        _dict = vars(self)
        logger.debug("DataFlow to Dictionary: {}".format(_dict))
        return _dict


    @property
    def generate_paths(self) -> List[str]:
        if self.source_execution_date is None:
            today = date.today()
            if self.source_frequency_value is None:
                # If no frequency is provided, we can't generate date-based paths unless default behavior is needed.
                # Returning empty list or maybe raising error?
                # Original code would crash or behave weirdly.
                return []

            dates: List[date] = []
            if self.source_frequency_unit == 'days':
                dates = [today - timedelta(days=x + 1)
                        for x in range(self.source_frequency_value)]

                logger.debug("Generated Dates: {}".format(dates))

            elif self.source_frequency_unit == 'months':
                dates = [self._subtract_month(today, x + 1)
                        for x in range(self.source_frequency_value)]

                logger.debug("Generated Dates: {}".format(dates))
            else:
                raise ValueError("Invalid frequency unit provided")

            paths = []
            for dt in dates:
                if self.source_data_format:
                    # Safe(r) eval
                    try:
                        formatted = eval(self.source_data_format, {"dt": dt, "date": date, "timedelta": timedelta})
                    except Exception as e:
                        logger.warning("Failed to eval source_data_format: {}. Error: {}".format(self.source_data_format, e))
                        formatted = str(dt)
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
