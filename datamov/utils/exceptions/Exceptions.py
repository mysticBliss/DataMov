class CreateTrackingDB(Exception):
    """Exception to create DB datamov_monitoring_db"""

    def __init__(self, message="Create DB datamov_monitoring_db before proceeding..."):
        self.message = message
        super().__init__(message)


class FlowTypeException(Exception):
    """exception for invalid flow type"""

    def __init__(self, message="Flow should be an instance of DataFlow"):
        self.message = message
        super().__init__(message)


class EnvTypeException(Exception):
    """exception for invalid environment type"""

    def __init__(self, message="env should be an instance of EnvironmentConfig"):
        self.message = message
        super().__init__(message)


class PathNotFoundException(Exception):
    """Exception for PathNotFound"""

    def __init__(self, message="Path not found"):
        self.message = message
        super().__init__(message)


class SqlNotFound(Exception):
    """exception for no value for source sql"""

    def __init__(self, message="Field source_sql needs to be defined in DataFlow Configuration"):
        self.message = message
        super().__init__(message)
