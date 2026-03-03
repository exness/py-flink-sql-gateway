"""Tests for the PEP 249 DB-API 2.0 layer.

Uses a mock FlinkSqlGatewayClient to test Connection, Cursor,
type decoding, and error handling without a real gateway.
"""

from __future__ import annotations

import datetime
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

import flink_gateway
from flink_gateway import Connection, Cursor, NotSupportedError, ProgrammingError
from flink_gateway.connection import connect
from flink_gateway.models import (
    ColumnInfo,
    FetchResultsResponse,
    LogicalType,
    ResultKind,
    ResultSet,
    ResultType,
    RowData,
)
from flink_gateway.types import (
    DATETIME,
    NUMBER,
    STRING,
    FlinkType,
    decode_field,
    normalize_flink_type,
    type_code_for,
)

# ── Helpers ────────────────────────────────────────────────────────────


def _mock_client() -> MagicMock:
    """Return a MagicMock with the FlinkSqlGatewayClient interface."""
    client = MagicMock()
    client.open_session.return_value = "test-session"
    return client


def _make_connection(client: MagicMock | None = None) -> Connection:
    """Create a Connection backed by a mock client."""
    c = client or _mock_client()
    return Connection(c, "test-session", _owns_client=False)


def _columns(*specs: tuple[str, str, bool]) -> list[ColumnInfo]:
    """Build ColumnInfo list from (name, type, nullable) tuples."""
    return [
        ColumnInfo(
            name=name,
            logical_type=LogicalType(type=typ, nullable=nullable),
        )
        for name, typ, nullable in specs
    ]


def _ready_result(
    columns: list[ColumnInfo],
    data: list[list],
    *,
    is_query: bool = True,
    next_uri: str = "",
) -> FetchResultsResponse:
    """Build a FetchResultsResponse that is ready with data."""
    return FetchResultsResponse(
        result_type=ResultType.PAYLOAD,
        result_kind=ResultKind.SUCCESS_WITH_CONTENT if is_query else ResultKind.SUCCESS,
        is_query_result=is_query,
        next_result_uri=next_uri,
        results=ResultSet(
            columns=columns,
            data=[RowData(kind="INSERT", fields=row) for row in data],
        ),
    )


def _eos_result() -> FetchResultsResponse:
    return FetchResultsResponse(
        result_type=ResultType.EOS,
        results=ResultSet(),
    )


# ── Module-level PEP 249 globals ──────────────────────────────────────


class TestModuleGlobals:
    def test_apilevel(self):
        assert flink_gateway.apilevel == "2.0"

    def test_threadsafety(self):
        assert flink_gateway.threadsafety == 1

    def test_paramstyle(self):
        assert flink_gateway.paramstyle == "qmark"


# ── Connection ─────────────────────────────────────────────────────────


class TestConnection:
    def test_cursor_returns_cursor(self):
        conn = _make_connection()
        cur = conn.cursor()
        assert isinstance(cur, Cursor)

    def test_close_calls_close_session(self):
        client = _mock_client()
        conn = _make_connection(client)
        conn.close()
        client.close_session.assert_called_once_with("test-session")

    def test_double_close_is_safe(self):
        client = _mock_client()
        conn = _make_connection(client)
        conn.close()
        conn.close()  # should not raise
        assert client.close_session.call_count == 1

    def test_context_manager(self):
        client = _mock_client()
        with Connection(client, "test-session", _owns_client=False) as conn:
            cur = conn.cursor()
            assert isinstance(cur, Cursor)
        client.close_session.assert_called_once()

    def test_commit_is_noop(self):
        conn = _make_connection()
        conn.commit()  # should not raise

    def test_rollback_raises(self):
        conn = _make_connection()
        with pytest.raises(NotSupportedError):
            conn.rollback()

    def test_cursor_on_closed_connection(self):
        conn = _make_connection()
        conn.close()
        with pytest.raises(ProgrammingError, match="connection is closed"):
            conn.cursor()


# ── Cursor lifecycle ──────────────────────────────────────────────────


class TestCursorLifecycle:
    def test_close_cursor(self):
        conn = _make_connection()
        cur = conn.cursor()
        cur._operation_handle = "op-1"
        cur.close()
        assert cur._closed
        conn.client.close_operation.assert_called_once_with("test-session", "op-1")

    def test_fetchone_without_execute(self):
        conn = _make_connection()
        cur = conn.cursor()
        with pytest.raises(ProgrammingError, match="no query executed"):
            cur.fetchone()

    def test_fetchall_without_execute(self):
        conn = _make_connection()
        cur = conn.cursor()
        with pytest.raises(ProgrammingError, match="no query executed"):
            cur.fetchall()

    def test_operations_on_closed_cursor(self):
        conn = _make_connection()
        cur = conn.cursor()
        cur.close()
        with pytest.raises(ProgrammingError, match="cursor is closed"):
            cur.execute("SELECT 1")

    def test_context_manager(self):
        conn = _make_connection()
        with conn.cursor() as cur:
            cur._operation_handle = "op-2"
        assert cur._closed


# ── Cursor execute + fetch ─────────────────────────────────────────────


class TestCursorExecuteFetch:
    def _setup_query(self, client: MagicMock, columns, data, next_uri=""):
        """Configure mock client for a query that returns one page of results."""
        result = _ready_result(columns, data, next_uri=next_uri)
        client.execute_statement.return_value = "op-1"
        client.fetch_results.return_value = result

    def test_simple_select(self):
        client = _mock_client()
        cols = _columns(("id", "INTEGER", False), ("name", "VARCHAR", True))
        self._setup_query(client, cols, [[1, "Alice"], [2, None]])

        # Second fetch_results call (for pagination) returns EOS.
        client.fetch_results.side_effect = [
            _ready_result(cols, [[1, "Alice"], [2, None]]),
            _eos_result(),
        ]

        conn = _make_connection(client)
        cur = conn.cursor()
        cur.execute("SELECT id, name FROM users")

        assert cur.description is not None
        assert len(cur.description) == 2
        assert cur.description[0][0] == "id"
        assert cur.description[0][1] == NUMBER
        assert cur.description[0][6] is False  # null_ok
        assert cur.description[1][0] == "name"
        assert cur.description[1][1] == STRING
        assert cur.description[1][6] is True  # null_ok

        rows = cur.fetchall()
        assert rows == [(1, "Alice"), (2, None)]

    def test_fetchone(self):
        client = _mock_client()
        cols = _columns(("val", "BIGINT", False))
        client.execute_statement.return_value = "op-1"
        client.fetch_results.side_effect = [
            _ready_result(cols, [[10], [20], [30]]),
            _eos_result(),
        ]

        conn = _make_connection(client)
        cur = conn.cursor()
        cur.execute("SELECT val FROM t")

        assert cur.fetchone() == (10,)
        assert cur.fetchone() == (20,)
        assert cur.fetchone() == (30,)
        assert cur.fetchone() is None

    def test_fetchmany(self):
        client = _mock_client()
        cols = _columns(("x", "INTEGER", False))
        client.execute_statement.return_value = "op-1"
        client.fetch_results.side_effect = [
            _ready_result(cols, [[1], [2], [3], [4], [5]]),
            _eos_result(),
        ]

        conn = _make_connection(client)
        cur = conn.cursor()
        cur.execute("SELECT x FROM t")

        batch = cur.fetchmany(3)
        assert batch == [(1,), (2,), (3,)]
        batch = cur.fetchmany(3)
        assert batch == [(4,), (5,)]

    def test_iterator_protocol(self):
        client = _mock_client()
        cols = _columns(("n", "INTEGER", False))
        client.execute_statement.return_value = "op-1"
        client.fetch_results.side_effect = [
            _ready_result(cols, [[1], [2]]),
            _eos_result(),
        ]

        conn = _make_connection(client)
        cur = conn.cursor()
        cur.execute("SELECT n FROM t")
        assert list(cur) == [(1,), (2,)]

    def test_pagination(self):
        """Verify the cursor fetches multiple pages."""
        client = _mock_client()
        cols = _columns(("v", "INTEGER", False))
        client.execute_statement.return_value = "op-1"
        client.fetch_results.side_effect = [
            # Page 1 — initial fetch_until_ready
            _ready_result(cols, [[1], [2]], next_uri="/v3/.../result/1"),
            # Page 2 — fetched by _iter_rows
            _ready_result(cols, [[3], [4]], next_uri="/v3/.../result/2"),
            # EOS
            _eos_result(),
        ]

        conn = _make_connection(client)
        cur = conn.cursor()
        cur.execute("SELECT v FROM t")
        rows = cur.fetchall()
        assert rows == [(1,), (2,), (3,), (4,)]

    def test_exec_non_query(self):
        """Non-query statements should not produce rows."""
        client = _mock_client()
        cols = _columns(("result", "VARCHAR", False))
        result = FetchResultsResponse(
            result_type=ResultType.PAYLOAD,
            result_kind=ResultKind.SUCCESS,
            is_query_result=False,
            results=ResultSet(
                columns=cols, data=[RowData(kind="INSERT", fields=["OK"])]
            ),
        )
        client.execute_statement.return_value = "op-1"
        client.fetch_results.return_value = result

        conn = _make_connection(client)
        cur = conn.cursor()
        cur.execute("CREATE TABLE t (id INT)")
        # close_operation should have been called.
        client.close_operation.assert_called()

    def test_not_ready_polling(self):
        """Verify polling loop handles NOT_READY before PAYLOAD."""
        client = _mock_client()
        cols = _columns(("x", "INTEGER", False))
        client.execute_statement.return_value = "op-1"
        client.fetch_results.side_effect = [
            FetchResultsResponse(result_type=ResultType.NOT_READY, results=ResultSet()),
            FetchResultsResponse(result_type=ResultType.NOT_READY, results=ResultSet()),
            _ready_result(cols, [[42]]),
            _eos_result(),
        ]

        conn = _make_connection(client)
        cur = conn.cursor()

        with patch("flink_gateway.cursor.time.sleep"):  # skip actual sleeps
            cur.execute("SELECT 42")

        assert cur.fetchone() == (42,)
        # fetch_results was called 3 times during polling
        # (2 NOT_READY + 1 PAYLOAD).
        # The row data is in the initial page, so fetchone
        # doesn't trigger another fetch.
        assert client.fetch_results.call_count == 3


# ── Type decoding ──────────────────────────────────────────────────────


class TestTypeDecoding:
    def test_integer_types(self):
        for t in ("TINYINT", "SMALLINT", "INTEGER", "INT", "BIGINT"):
            ft = normalize_flink_type(t)
            assert decode_field(42, ft) == 42
            assert isinstance(decode_field(42, ft), int)

    def test_float_types(self):
        for t in ("FLOAT", "DOUBLE"):
            ft = normalize_flink_type(t)
            assert decode_field(3.14, ft) == pytest.approx(3.14)
            assert isinstance(decode_field(3.14, ft), float)

    def test_boolean(self):
        ft = normalize_flink_type("BOOLEAN")
        assert decode_field(True, ft) is True
        assert decode_field(False, ft) is False

    def test_string_types(self):
        for t in ("CHAR", "VARCHAR", "STRING"):
            ft = normalize_flink_type(t)
            assert decode_field("hello", ft) == "hello"

    def test_decimal(self):
        ft = normalize_flink_type("DECIMAL")
        result = decode_field("123.456", ft)
        assert result == Decimal("123.456")
        assert isinstance(result, Decimal)

    def test_decimal_from_number(self):
        ft = normalize_flink_type("DECIMAL")
        result = decode_field(99, ft)
        assert result == Decimal("99")

    def test_date(self):
        ft = normalize_flink_type("DATE")
        result = decode_field("2024-06-15", ft)
        assert result == datetime.date(2024, 6, 15)

    def test_time(self):
        ft = normalize_flink_type("TIME")
        result = decode_field("14:30:00", ft)
        assert result == datetime.time(14, 30, 0)

    def test_time_with_fractional(self):
        ft = normalize_flink_type("TIME")
        result = decode_field("14:30:00.123456", ft)
        assert result == datetime.time(14, 30, 0, 123456)

    def test_timestamp(self):
        ft = normalize_flink_type("TIMESTAMP")
        result = decode_field("2024-06-15T14:30:00.123456", ft)
        assert result == datetime.datetime(2024, 6, 15, 14, 30, 0, 123456)

    def test_timestamp_ltz(self):
        ft = normalize_flink_type("TIMESTAMP_LTZ")
        result = decode_field("2024-06-15T14:30:00", ft)
        assert result == datetime.datetime(2024, 6, 15, 14, 30, 0)

    def test_timestamp_with_tz_stays_string(self):
        ft = normalize_flink_type("TIMESTAMP_WITH_TIME_ZONE")
        result = decode_field("2024-06-15T14:30:00+02:00", ft)
        assert isinstance(result, str)

    def test_interval_types(self):
        for t in ("INTERVAL_YEAR_MONTH", "INTERVAL_DAY_TIME"):
            ft = normalize_flink_type(t)
            assert decode_field(12, ft) == 12

    def test_null_value(self):
        ft = normalize_flink_type("INTEGER")
        assert decode_field(None, ft) is None

    def test_complex_types_passthrough(self):
        ft = normalize_flink_type("MULTISET")
        data = {"key": "value"}
        assert decode_field(data, ft) == data


class TestRecursiveDecoding:
    """Tests for recursive ROW and MAP decoding."""

    def test_row_decodes_children(self):
        """A ROW with INT + STRING fields should decode field values."""
        from flink_gateway.models import ColumnInfo, LogicalType

        lt = LogicalType(
            type="ROW",
            fields=[
                ColumnInfo(name="score", logical_type=LogicalType(type="INTEGER")),
                ColumnInfo(name="label", logical_type=LogicalType(type="VARCHAR")),
            ],
        )
        ft = normalize_flink_type("ROW")
        result = decode_field({"score": "42", "label": 123}, ft, logical_type=lt)
        assert result == {"score": 42, "label": "123"}

    def test_row_nested(self):
        """A ROW inside a ROW should decode at all levels."""
        from flink_gateway.models import ColumnInfo, LogicalType

        inner_lt = LogicalType(
            type="ROW",
            fields=[
                ColumnInfo(name="x", logical_type=LogicalType(type="DOUBLE")),
            ],
        )
        outer_lt = LogicalType(
            type="ROW",
            fields=[
                ColumnInfo(name="id", logical_type=LogicalType(type="INTEGER")),
                ColumnInfo(name="nested", logical_type=inner_lt),
            ],
        )
        ft = normalize_flink_type("ROW")
        result = decode_field(
            {"id": "7", "nested": {"x": "3.14"}},
            ft,
            logical_type=outer_lt,
        )
        assert result == {"id": 7, "nested": {"x": 3.14}}

    def test_map_decodes_values(self):
        """A MAP<STRING, INT> should decode string values to int."""
        from flink_gateway.models import LogicalType

        lt = LogicalType(
            type="MAP",
            key_type=LogicalType(type="VARCHAR"),
            value_type=LogicalType(type="INTEGER"),
        )
        ft = normalize_flink_type("MAP")
        result = decode_field({"apples": "3", "bananas": "5"}, ft, logical_type=lt)
        assert result == {"apples": 3, "bananas": 5}

    def test_map_with_row_values(self):
        """A MAP<STRING, ROW<score INT>> should recursively decode."""
        from flink_gateway.models import ColumnInfo, LogicalType

        row_lt = LogicalType(
            type="ROW",
            fields=[
                ColumnInfo(name="score", logical_type=LogicalType(type="INTEGER")),
            ],
        )
        lt = LogicalType(
            type="MAP",
            key_type=LogicalType(type="VARCHAR"),
            value_type=row_lt,
        )
        ft = normalize_flink_type("MAP")
        result = decode_field(
            {"player1": {"score": "100"}, "player2": {"score": "200"}},
            ft,
            logical_type=lt,
        )
        assert result == {"player1": {"score": 100}, "player2": {"score": 200}}

    def test_row_without_children_passes_through(self):
        """Without child schema, ROW falls back to passthrough."""
        from flink_gateway.models import LogicalType

        lt = LogicalType(type="ROW")
        ft = normalize_flink_type("ROW")
        data = {"a": 1, "b": "two"}
        assert decode_field(data, ft, logical_type=lt) == data

    def test_map_without_children_passes_through(self):
        """Without child schema, MAP falls back to passthrough."""
        from flink_gateway.models import LogicalType

        lt = LogicalType(type="MAP")
        ft = normalize_flink_type("MAP")
        data = {"k": "v"}
        assert decode_field(data, ft, logical_type=lt) == data

    def test_row_null_value(self):
        """NULL ROW values should return None."""
        from flink_gateway.models import ColumnInfo, LogicalType

        lt = LogicalType(
            type="ROW",
            fields=[
                ColumnInfo(name="x", logical_type=LogicalType(type="INTEGER")),
            ],
        )
        ft = normalize_flink_type("ROW")
        assert decode_field(None, ft, logical_type=lt) is None

    def test_array_decodes_elements(self):
        """An ARRAY<INTEGER> should decode each element."""
        from flink_gateway.models import LogicalType

        lt = LogicalType(
            type="ARRAY",
            element_type=LogicalType(type="INTEGER"),
        )
        ft = normalize_flink_type("ARRAY")
        result = decode_field(["1", "2", "3"], ft, logical_type=lt)
        assert result == [1, 2, 3]

    def test_array_of_rows(self):
        """An ARRAY<ROW<name STRING, score INT>> should recursively decode."""
        from flink_gateway.models import ColumnInfo, LogicalType

        row_lt = LogicalType(
            type="ROW",
            fields=[
                ColumnInfo(name="name", logical_type=LogicalType(type="VARCHAR")),
                ColumnInfo(name="score", logical_type=LogicalType(type="INTEGER")),
            ],
        )
        lt = LogicalType(
            type="ARRAY",
            element_type=row_lt,
        )
        ft = normalize_flink_type("ARRAY")
        result = decode_field(
            [{"name": "Alice", "score": "100"}, {"name": "Bob", "score": "200"}],
            ft,
            logical_type=lt,
        )
        assert result == [
            {"name": "Alice", "score": 100},
            {"name": "Bob", "score": 200},
        ]

    def test_array_without_children_passes_through(self):
        """Without child schema, ARRAY falls back to passthrough."""
        from flink_gateway.models import LogicalType

        lt = LogicalType(type="ARRAY")
        ft = normalize_flink_type("ARRAY")
        data = [1, 2, 3]
        assert decode_field(data, ft, logical_type=lt) == data


# ── Type normalization ─────────────────────────────────────────────────


class TestTypeNormalization:
    def test_known_types(self):
        assert normalize_flink_type("INT") == FlinkType.INTEGER
        assert normalize_flink_type("STRING") == FlinkType.VARCHAR
        assert normalize_flink_type("DEC") == FlinkType.DECIMAL
        assert normalize_flink_type("NUMERIC") == FlinkType.DECIMAL
        assert normalize_flink_type("TIME_WITHOUT_TIME_ZONE") == FlinkType.TIME
        assert (
            normalize_flink_type("TIMESTAMP_WITH_LOCAL_TIME_ZONE")
            == FlinkType.TIMESTAMP_LTZ
        )

    def test_case_insensitive(self):
        assert normalize_flink_type("integer") == FlinkType.INTEGER
        assert normalize_flink_type("VarChar") == FlinkType.VARCHAR

    def test_unknown_type(self):
        assert normalize_flink_type("UNKNOWN_TYPE") is None


# ── Type codes ─────────────────────────────────────────────────────────


class TestTypeCodes:
    def test_number_types(self):
        for t in (
            FlinkType.INTEGER,
            FlinkType.BIGINT,
            FlinkType.FLOAT,
            FlinkType.DECIMAL,
        ):
            assert type_code_for(t) == NUMBER

    def test_string_types(self):
        for t in (FlinkType.CHAR, FlinkType.VARCHAR):
            assert type_code_for(t) == STRING

    def test_datetime_types(self):
        for t in (FlinkType.DATE, FlinkType.TIME, FlinkType.TIMESTAMP):
            assert type_code_for(t) == DATETIME

    def test_none_type(self):
        assert type_code_for(None) == STRING


# ── connect() function ─────────────────────────────────────────────────


class TestConnectFunction:
    def test_connect_creates_session(self):
        with patch("flink_gateway.connection.FlinkSqlGatewayClient") as MockClient:
            mock_instance = MagicMock()
            mock_instance.open_session.return_value = "sess-new"
            MockClient.return_value = mock_instance

            conn = connect("http://localhost:8083", properties={"key": "val"})

            MockClient.assert_called_once_with(
                "http://localhost:8083", api_version="v3"
            )
            mock_instance.open_session.assert_called_once()
            assert conn.session_handle == "sess-new"
            conn.close()

    def test_connect_cleans_up_on_failure(self):
        with patch("flink_gateway.connection.FlinkSqlGatewayClient") as MockClient:
            mock_instance = MagicMock()
            mock_instance.open_session.side_effect = RuntimeError("connection refused")
            MockClient.return_value = mock_instance

            with pytest.raises(RuntimeError, match="connection refused"):
                connect("http://localhost:8083")

            mock_instance.close.assert_called_once()
