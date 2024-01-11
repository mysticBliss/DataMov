import json
from datetime import date, timedelta
import time
import uuid

from ...core.logger import Logger

logger = Logger().get_logger()

class DataFlow:
    def __init__(self, **kwargs):
        self.source_execution_date = None
        for key, value in kwargs.items():
            if value:
                setattr(self, key, value)

    def _subtract_month(d, months):
        y, m = divmod(d.month - months - 1, 12)
        return date(d.year - y, m + 1, d.day)

    @classmethod
    def generate_tracking_id(cls):
        return "{}-{}".format(uuid.uuid4(), str(int(time.time())))
    
    
    def to_dict(self):
        _dict = vars(self)
        logger.debug("DataFlow to Dictionary: {}".format(_dict))
        return _dict


    @property
    def generate_paths(self):
        if self.source_execution_date is None:
            today = date.today()
            if self.source_frequency_unit == 'days':
                dates = [today - timedelta(days=x + 1)
                        for x in range(self.source_frequency_value)]
                
                logger.debug("Genrated Dates: {}".format(dates))

            elif self.source_frequency_unit == 'months':
                dates = [self._subtract_month(today, x + 1)
                        for x in range(self.source_frequency_value)]
                
                logger.debug("Genrated Dates: {}".format(dates))
            else:
                raise ValueError("Invalid frequency unit provided")

            return [self.source_fs_path.format(data_format=eval(self.source_data_format)) for dt in dates]
        else:
            return [self.source_fs_path.format(data_format=self.source_execution_date)]

    def __repr__(self):
        return "{}({})".format(
            self.__class__.__name__,
            ', '.join(['{}={}'.format(k, v) for k, v in vars(self).items()])
        )
