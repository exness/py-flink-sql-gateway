"""PEP 249 exception hierarchy for the Flink SQL Gateway driver."""


class Error(Exception):
    """Base exception for the flink_gateway package."""


class InterfaceError(Error):
    """Exception for errors related to the database interface.

    Raised for issues with the driver itself, such as invalid
    arguments to driver methods.
    """


class DatabaseError(Error):
    """Exception for errors related to the database."""


class OperationalError(DatabaseError):
    """Exception for errors related to the database's operation.

    Raised for connection failures, statement execution errors, etc.
    """


class FlinkSqlGatewayError(OperationalError):
    """Raised when a Flink SQL Gateway REST call fails."""


class ProgrammingError(DatabaseError):
    """Exception for programming errors.

    Raised for invalid SQL, using a closed cursor, etc.
    """


class NotSupportedError(Error):
    """Exception for unsupported operations.

    Raised when attempting transactions or other features Flink
    SQL Gateway does not support.
    """
