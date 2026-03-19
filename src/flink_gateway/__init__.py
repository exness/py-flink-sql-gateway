"""Flink SQL Gateway — Python client library.

Implements PEP 249 (DB-API 2.0) on top of the Flink SQL Gateway REST API.
"""

from flink_gateway.client import FlinkSqlGatewayClient
from flink_gateway.connection import Connection, connect
from flink_gateway.cursor import Cursor
from flink_gateway.exceptions import (
    DatabaseError,
    Error,
    FlinkSqlGatewayError,
    InterfaceError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    TimeoutError,
)
from flink_gateway.models import (
    ColumnInfo,
    CompleteStatementRequest,
    ConfigureSessionRequest,
    ExecuteStatementRequest,
    FetchResultsResponse,
    InfoResponse,
    LogicalType,
    OpenSessionRequest,
    RefreshMaterializedTableRequest,
    ResultKind,
    ResultSet,
    ResultType,
    RowData,
)
from flink_gateway.types import (
    BINARY,
    DATETIME,
    NUMBER,
    ROWID,
    STRING,
    Binary,
    Date,
    DateFromTicks,
    DBAPITypeObject,
    FlinkType,
    Time,
    TimeFromTicks,
    Timestamp,
    TimestampFromTicks,
)

# PEP 249 module-level globals
apilevel = "2.0"
threadsafety = 1  # Threads may share the module but not connections.
paramstyle = "qmark"

__all__ = [
    # PEP 249
    "apilevel",
    "threadsafety",
    "paramstyle",
    "connect",
    "Connection",
    "Cursor",
    # Exceptions
    "Error",
    "InterfaceError",
    "DatabaseError",
    "OperationalError",
    "TimeoutError",
    "FlinkSqlGatewayError",
    "ProgrammingError",
    "NotSupportedError",
    # Type objects
    "STRING",
    "BINARY",
    "NUMBER",
    "DATETIME",
    "ROWID",
    "DBAPITypeObject",
    "FlinkType",
    # PEP 249 constructors
    "Date",
    "Time",
    "Timestamp",
    "DateFromTicks",
    "TimeFromTicks",
    "TimestampFromTicks",
    "Binary",
    # Low-level client
    "FlinkSqlGatewayClient",
    "FlinkSqlGatewayError",
    # Models
    "ColumnInfo",
    "CompleteStatementRequest",
    "ConfigureSessionRequest",
    "ExecuteStatementRequest",
    "FetchResultsResponse",
    "InfoResponse",
    "LogicalType",
    "OpenSessionRequest",
    "RefreshMaterializedTableRequest",
    "ResultKind",
    "ResultSet",
    "ResultType",
    "RowData",
]
