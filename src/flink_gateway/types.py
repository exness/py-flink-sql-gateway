"""Flink SQL type normalization and decoding."""

from __future__ import annotations

import datetime
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from flink_gateway.models import LogicalType


class FlinkType(StrEnum):
    """Normalized Flink SQL logical type aliases."""

    TINYINT = "TINYINT"
    SMALLINT = "SMALLINT"
    INTEGER = "INTEGER"
    BIGINT = "BIGINT"
    INTERVAL = "INTERVAL"
    FLOAT = "FLOAT"
    DOUBLE = "DOUBLE"
    BOOLEAN = "BOOLEAN"
    CHAR = "CHAR"
    VARCHAR = "VARCHAR"
    DECIMAL = "DECIMAL"
    DATE = "DATE"
    TIME = "TIME"
    TIMESTAMP = "TIMESTAMP"
    TIMESTAMP_WITH_TIME_ZONE = "TIMESTAMP_WITH_TIME_ZONE"
    TIMESTAMP_LTZ = "TIMESTAMP_LTZ"
    INTERVAL_YEAR_MONTH = "INTERVAL_YEAR_MONTH"
    INTERVAL_DAY_TIME = "INTERVAL_DAY_TIME"
    ARRAY = "ARRAY"
    MAP = "MAP"
    ROW = "ROW"
    MULTISET = "MULTISET"
    BINARY = "BINARY"
    VARBINARY = "VARBINARY"


_TYPE_MAP: dict[str, FlinkType] = {
    "TINYINT": FlinkType.TINYINT,
    "SMALLINT": FlinkType.SMALLINT,
    "INTEGER": FlinkType.INTEGER,
    "INT": FlinkType.INTEGER,
    "BIGINT": FlinkType.BIGINT,
    "INTERVAL": FlinkType.INTERVAL,
    "FLOAT": FlinkType.FLOAT,
    "DOUBLE": FlinkType.DOUBLE,
    "BOOLEAN": FlinkType.BOOLEAN,
    "CHAR": FlinkType.CHAR,
    "VARCHAR": FlinkType.VARCHAR,
    "STRING": FlinkType.VARCHAR,
    "DECIMAL": FlinkType.DECIMAL,
    "DEC": FlinkType.DECIMAL,
    "NUMERIC": FlinkType.DECIMAL,
    "DATE": FlinkType.DATE,
    "TIME": FlinkType.TIME,
    "TIME_WITHOUT_TIME_ZONE": FlinkType.TIME,
    "TIMESTAMP": FlinkType.TIMESTAMP,
    "TIMESTAMP_WITHOUT_TIME_ZONE": FlinkType.TIMESTAMP,
    "TIMESTAMP_LTZ": FlinkType.TIMESTAMP_LTZ,
    "TIMESTAMP_WITH_LOCAL_TIME_ZONE": FlinkType.TIMESTAMP_LTZ,
    "TIMESTAMP_WITH_TIME_ZONE": FlinkType.TIMESTAMP_WITH_TIME_ZONE,
    "INTERVAL_YEAR_MONTH": FlinkType.INTERVAL_YEAR_MONTH,
    "INTERVAL_DAY_TIME": FlinkType.INTERVAL_DAY_TIME,
    "ARRAY": FlinkType.ARRAY,
    "MAP": FlinkType.MAP,
    "ROW": FlinkType.ROW,
    "MULTISET": FlinkType.MULTISET,
    "BINARY": FlinkType.BINARY,
    "VARBINARY": FlinkType.VARBINARY,
}


# ── PEP 249 Type Objects ──────────────────────────────────────────────
#
# Per PEP 249, type objects compare equal to the type_code values used
# in cursor.description.  A single type object (e.g. NUMBER) compares
# equal to ALL type codes in its group (INTEGER, BIGINT, FLOAT, …).
#
# cursor.description[i][1] returns the specific FlinkType string
# (e.g. "INTEGER"), and  ``desc[1] == NUMBER``  evaluates to True.


class DBAPITypeObject:
    """PEP 249 type object that compares equal to a set of type codes."""

    def __init__(self, *values: str) -> None:
        self._values = frozenset(values)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, DBAPITypeObject):
            return self._values == other._values
        return other in self._values

    def __ne__(self, other: object) -> bool:
        return not self.__eq__(other)

    def __hash__(self) -> int:
        return hash(self._values)

    def __repr__(self) -> str:
        return f"DBAPITypeObject({', '.join(sorted(self._values))})"


STRING = DBAPITypeObject(
    FlinkType.CHAR,
    FlinkType.VARCHAR,
)

BINARY = DBAPITypeObject(
    FlinkType.BINARY,
    FlinkType.VARBINARY,
)

NUMBER = DBAPITypeObject(
    FlinkType.TINYINT,
    FlinkType.SMALLINT,
    FlinkType.INTEGER,
    FlinkType.BIGINT,
    FlinkType.FLOAT,
    FlinkType.DOUBLE,
    FlinkType.DECIMAL,
    FlinkType.BOOLEAN,
    FlinkType.INTERVAL,
    FlinkType.INTERVAL_YEAR_MONTH,
    FlinkType.INTERVAL_DAY_TIME,
)

DATETIME = DBAPITypeObject(
    FlinkType.DATE,
    FlinkType.TIME,
    FlinkType.TIMESTAMP,
    FlinkType.TIMESTAMP_LTZ,
    FlinkType.TIMESTAMP_WITH_TIME_ZONE,
)

ROWID = DBAPITypeObject()


# ── PEP 249 Constructor Functions ─────────────────────────────────────


def Date(year: int, month: int, day: int) -> datetime.date:
    """Construct a date value."""
    return datetime.date(year, month, day)


def Time(hour: int, minute: int, second: int) -> datetime.time:
    """Construct a time value."""
    return datetime.time(hour, minute, second)


def Timestamp(
    year: int,
    month: int,
    day: int,
    hour: int,
    minute: int,
    second: int,
) -> datetime.datetime:
    """Construct a timestamp value."""
    return datetime.datetime(year, month, day, hour, minute, second)


def DateFromTicks(ticks: float) -> datetime.date:
    """Construct a date from ticks (seconds since epoch)."""
    return datetime.date.fromtimestamp(ticks)


def TimeFromTicks(ticks: float) -> datetime.time:
    """Construct a time from ticks (seconds since epoch)."""
    return datetime.datetime.fromtimestamp(ticks).time()


def TimestampFromTicks(ticks: float) -> datetime.datetime:
    """Construct a timestamp from ticks (seconds since epoch)."""
    return datetime.datetime.fromtimestamp(ticks)


def Binary(string: bytes | str) -> bytes:
    """Construct a binary value."""
    if isinstance(string, str):
        return string.encode("utf-8")
    return bytes(string)


# ── Public helpers ────────────────────────────────────────────────────


def normalize_flink_type(raw: str) -> FlinkType | None:
    """Normalize a raw Flink type string to a ``FlinkType`` enum value."""
    return _TYPE_MAP.get(raw.upper())


def type_code_for(flink_type: FlinkType | None) -> str:
    """Return the PEP 249 type_code for *cursor.description*.

    Returns the specific ``FlinkType`` string (e.g. ``"INTEGER"``).
    Compare against the type objects (``NUMBER``, ``STRING``, etc.)
    to check the type group::

        desc = cursor.description
        if desc[0][1] == NUMBER:
            ...
    """
    if flink_type is None:
        return FlinkType.VARCHAR
    return flink_type.value


def decode_field(
    value: Any,
    flink_type: FlinkType | None,
    precision: int | None = None,
    logical_type: LogicalType | None = None,
) -> Any:
    """Decode a single field value from the Flink JSON response.

    Args:
        value: The raw JSON value from the gateway response.
        flink_type: The normalized Flink type.
        precision: Optional precision from column metadata.
        logical_type: Optional full ``LogicalType`` with children for
            recursive decoding of ROW / MAP types.

    Returns:
        A native Python value, or ``None`` for SQL NULLs.
    """
    if value is None:
        return None

    if flink_type is None:
        return value

    match flink_type:
        # ── Integers ───────────────────────────────────────────────
        case (
            FlinkType.TINYINT
            | FlinkType.SMALLINT
            | FlinkType.INTEGER
            | FlinkType.BIGINT
            | FlinkType.INTERVAL
            | FlinkType.INTERVAL_YEAR_MONTH
            | FlinkType.INTERVAL_DAY_TIME
        ):
            return int(value)

        # ── Floats ─────────────────────────────────────────────────
        case FlinkType.FLOAT | FlinkType.DOUBLE:
            return float(value)

        # ── Boolean ────────────────────────────────────────────────
        case FlinkType.BOOLEAN:
            if isinstance(value, bool):
                return value
            return str(value).lower() in ("true", "1")

        # ── Strings ────────────────────────────────────────────────
        case FlinkType.CHAR | FlinkType.VARCHAR:
            return str(value)

        # ── Decimal ────────────────────────────────────────────────
        case FlinkType.DECIMAL:
            try:
                return Decimal(str(value))
            except InvalidOperation:
                return str(value)

        # ── Date ───────────────────────────────────────────────────
        case FlinkType.DATE:
            if isinstance(value, str):
                return datetime.date.fromisoformat(value)
            return value

        # ── Time ───────────────────────────────────────────────────
        case FlinkType.TIME:
            if isinstance(value, str):
                return datetime.time.fromisoformat(value)
            return value

        # ── Timestamps ─────────────────────────────────────────────
        case FlinkType.TIMESTAMP | FlinkType.TIMESTAMP_LTZ:
            if isinstance(value, str):
                # Flink uses 'T' separator; fromisoformat handles it.
                return datetime.datetime.fromisoformat(value)
            return value

        # ── Timestamp with time zone — keep as string ──────────────
        case FlinkType.TIMESTAMP_WITH_TIME_ZONE:
            return str(value)

        # ── ARRAY — recursively decode each element ────────────────
        case FlinkType.ARRAY:
            return _decode_array_value(value, logical_type)

        # ── ROW — recursively decode each field ────────────────────
        case FlinkType.ROW:
            return _decode_row_value(value, logical_type)

        # ── MAP — recursively decode keys and values ───────────────
        case FlinkType.MAP:
            return _decode_map_value(value, logical_type)

        # ── Other complex / binary types — pass through ────────────
        case _:
            return value


def _decode_array_value(
    value: Any,
    logical_type: LogicalType | None,
) -> Any:
    """Decode an ARRAY value using its element child schema."""
    if not isinstance(value, list) or logical_type is None or not logical_type.children:
        return value

    elem_child = logical_type.children[0]
    elem_ft = normalize_flink_type(elem_child.logical_type.type)
    return [
        decode_field(
            item,
            elem_ft,
            precision=elem_child.logical_type.precision,
            logical_type=elem_child.logical_type,
        )
        for item in value
    ]


def _decode_row_value(
    value: Any,
    logical_type: LogicalType | None,
) -> Any:
    """Decode a ROW value using its child schema."""
    if not isinstance(value, dict) or logical_type is None or not logical_type.children:
        return value

    children_by_name = {child.name: child for child in logical_type.children}
    decoded: dict[str, Any] = {}
    for key, raw_val in value.items():
        child = children_by_name.get(key)
        if child is not None:
            child_ft = normalize_flink_type(child.logical_type.type)
            decoded[key] = decode_field(
                raw_val,
                child_ft,
                precision=child.logical_type.precision,
                logical_type=child.logical_type,
            )
        else:
            decoded[key] = raw_val
    return decoded


def _decode_map_value(
    value: Any,
    logical_type: LogicalType | None,
) -> Any:
    """Decode a MAP value using its key/value child schemas."""
    if (
        not isinstance(value, dict)
        or logical_type is None
        or len(logical_type.children) < 2
    ):
        return value

    key_child = logical_type.children[0]
    val_child = logical_type.children[1]
    key_ft = normalize_flink_type(key_child.logical_type.type)
    val_ft = normalize_flink_type(val_child.logical_type.type)

    decoded: dict[Any, Any] = {}
    for raw_key, raw_val in value.items():
        dk = decode_field(
            raw_key,
            key_ft,
            precision=key_child.logical_type.precision,
            logical_type=key_child.logical_type,
        )
        dv = decode_field(
            raw_val,
            val_ft,
            precision=val_child.logical_type.precision,
            logical_type=val_child.logical_type,
        )
        decoded[dk] = dv
    return decoded
