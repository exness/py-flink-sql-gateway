"""PEP 249 Cursor implementation for the Flink SQL Gateway."""

from __future__ import annotations

import enum
import time
from typing import TYPE_CHECKING, Any, Iterator, Sequence, Union

from flink_gateway.exceptions import InterfaceError, ProgrammingError, TimeoutError
from flink_gateway.models import (
    ColumnInfo,
    ExecuteStatementRequest,
    FetchResultsResponse,
    ResultKind,
    ResultType,
    RowData,
)
from flink_gateway.types import decode_field, normalize_flink_type, type_code_for

if TYPE_CHECKING:
    from flink_gateway.connection import Connection

_POLL_INTERVAL = 1.0  # seconds
_RESULTS_MIN_BACKOFF = 0.1  # seconds
_RESULTS_MAX_BACKOFF = 1.0  # seconds

# PEP 249 description row: (name, type_code, display_size, internal_size,
#                           precision, scale, null_ok)
_DescriptionRow = tuple[str, str, None, None, int | None, int | None, bool | None]


class Cursor:
    """PEP 249 Cursor for executing Flink SQL statements.

    Do not instantiate directly; use :meth:`Connection.cursor`.
    """

    arraysize: int = 1

    def __init__(
        self,
        connection: Connection,
        *,
        query_timeout: float | None = None,
        idle_timeout: float | None = None,
    ) -> None:
        self._connection = connection
        self._closed = False
        self._description: list[_DescriptionRow] | None = None
        self._rowcount = -1
        self._operation_handle: str | None = None
        self._rows_iterator: Iterator[tuple[Any, ...]] | None = None
        self._columns: list[ColumnInfo] = []
        self._query_timeout = query_timeout
        self._idle_timeout = idle_timeout
        self._effective_query_timeout: float | None = query_timeout
        self._effective_idle_timeout: float | None = idle_timeout
        self._query_deadline: float = float("inf")

    # ── PEP 249 attributes ─────────────────────────────────────────

    @property
    def description(
        self,
    ) -> list[_DescriptionRow] | None:
        """Column metadata for the last executed query.

        Each entry is a 7-tuple:
        ``(name, type_code, display_size, internal_size, precision, scale, null_ok)``
        """
        return self._description

    @property
    def rowcount(self) -> int:
        """Number of rows affected.  Always ``-1`` for Flink."""
        return self._rowcount

    # ── Lifecycle ──────────────────────────────────────────────────

    def close(self) -> None:
        """Close the cursor and release the underlying operation."""
        if not self._closed:
            self._closed = True
            self._rows_iterator = None
            self._close_current_operation()

    def __enter__(self) -> Cursor:
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    def _check_open(self) -> None:
        if self._closed:
            raise ProgrammingError("cursor is closed")
        if self._connection.closed:
            raise ProgrammingError("connection is closed")

    # ── Execution ──────────────────────────────────────────────────

    class _Sentinel(enum.Enum):
        """Sentinel value to distinguish 'not provided' from ``None``."""

        MISSING = enum.auto()

    _MISSING: _Sentinel = _Sentinel.MISSING

    def execute(
        self,
        operation: str,
        parameters: Sequence[Any] | None = None,
        *,
        query_timeout: Union[float, None, _Sentinel] = _MISSING,
        idle_timeout: Union[float, None, _Sentinel] = _MISSING,
    ) -> Cursor:
        """Execute a SQL statement.

        Args:
            operation: The SQL string.
            parameters: Not yet supported.
            query_timeout: Maximum seconds this query may run before raising
                :class:`~flink_gateway.TimeoutError`.  Overrides the
                connection-level default for this execution only.
                Pass ``None`` to explicitly disable the timeout.
                Omit (or leave as default) to use the connection-level value.
            idle_timeout: Maximum seconds to wait between rows during
                streaming iteration before raising
                :class:`~flink_gateway.TimeoutError`.  Overrides the
                connection-level default for this execution only.
                Pass ``None`` to explicitly disable the timeout.
                Omit (or leave as default) to use the connection-level value.

        Returns:
            self (for chaining).
        """
        self._check_open()

        # Close any previous operation.
        self._close_current_operation()

        # Resolve effective timeouts: per-execute overrides > connection defaults.
        eff_qt: float | None = (
            self._query_timeout
            if isinstance(query_timeout, self._Sentinel)
            else query_timeout
        )
        eff_it: float | None = (
            self._idle_timeout
            if isinstance(idle_timeout, self._Sentinel)
            else idle_timeout
        )
        self._effective_query_timeout = eff_qt
        self._effective_idle_timeout = eff_it

        # Reset state.
        self._description = None
        self._rowcount = -1
        self._rows_iterator = None
        self._columns = []
        self._query_deadline = (
            time.monotonic() + eff_qt if eff_qt is not None else float("inf")
        )

        if parameters is not None:
            raise InterfaceError("parameterized queries are not yet supported")

        client = self._connection.client
        session = self._connection.session_handle

        # Submit statement.
        op_handle = client.execute_statement(
            session,
            ExecuteStatementRequest(statement=operation),
        )
        self._operation_handle = op_handle

        # Poll until results are ready.
        result = self._fetch_until_ready(op_handle)

        # Build description from column metadata.
        self._columns = result.results.columns
        self._build_description()

        # Set up the row iterator if this is a query.
        if (
            result.is_query_result
            or result.result_kind == ResultKind.SUCCESS_WITH_CONTENT
        ):
            self._rows_iterator = self._iter_rows(
                result.results.data,
                result.next_token(),
                op_handle,
            )
        else:
            # Not a query — close the operation now.
            self._close_current_operation()

        return self

    def executemany(
        self,
        operation: str,
        seq_of_parameters: Sequence[Sequence[Any]],
    ) -> None:
        """Execute a statement multiple times with different parameters."""
        self._check_open()
        for params in seq_of_parameters:
            self.execute(operation, params)

    # ── Fetching ───────────────────────────────────────────────────

    def fetchone(self) -> tuple[Any, ...] | None:
        """Fetch the next row, or ``None`` if exhausted."""
        self._check_open()
        if self._rows_iterator is None:
            raise ProgrammingError("no query executed")
        try:
            return next(self._rows_iterator)
        except StopIteration:
            return None

    def fetchmany(self, size: int | None = None) -> list[tuple[Any, ...]]:
        """Fetch up to *size* rows."""
        self._check_open()
        if self._rows_iterator is None:
            raise ProgrammingError("no query executed")
        if size is None:
            size = self.arraysize
        rows: list[tuple[Any, ...]] = []
        for _ in range(size):
            row = self.fetchone()
            if row is None:
                break
            rows.append(row)
        return rows

    def fetchall(self) -> list[tuple[Any, ...]]:
        """Fetch all remaining rows."""
        self._check_open()
        if self._rows_iterator is None:
            raise ProgrammingError("no query executed")
        return list(self._rows_iterator)

    def __iter__(self) -> Iterator[tuple[Any, ...]]:
        return self

    def __next__(self) -> tuple[Any, ...]:
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row

    # ── Not supported ──────────────────────────────────────────────

    def setinputsizes(self, sizes: Any) -> None:
        """No-op (PEP 249 compliance)."""

    def setoutputsize(self, size: Any, column: int = 0) -> None:
        """No-op (PEP 249 compliance)."""

    # ── Internal ───────────────────────────────────────────────────

    def _close_current_operation(self) -> None:
        """Close the current operation handle, ignoring errors."""
        if self._operation_handle:
            try:
                self._connection.client.close_operation(
                    self._connection.session_handle,
                    self._operation_handle,
                )
            except Exception:
                pass
            self._operation_handle = None

    def _fetch_until_ready(self, op_handle: str) -> FetchResultsResponse:
        """Poll ``fetch_results`` until the result is no longer NOT_READY."""
        client = self._connection.client
        session = self._connection.session_handle
        while True:
            result = client.fetch_results(session, op_handle, "0", "json")
            if result.result_type != ResultType.NOT_READY:
                return result
            if time.monotonic() > self._query_deadline:
                raise TimeoutError(
                    f"query timed out after {self._effective_query_timeout}s"
                )
            time.sleep(_POLL_INTERVAL)

    def _build_description(self) -> None:
        """Populate ``self._description`` from ``self._columns``."""
        if not self._columns:
            self._description = None
            return
        desc: list[_DescriptionRow] = []
        for col in self._columns:
            ft = normalize_flink_type(col.logical_type.type)
            tc = type_code_for(ft)
            desc.append(
                (
                    col.name,
                    tc,
                    None,  # display_size
                    None,  # internal_size
                    col.logical_type.precision,
                    col.logical_type.scale,
                    col.logical_type.nullable,
                )
            )
        self._description = desc

    def _iter_rows(
        self,
        initial_data: list[RowData],
        next_token: str,
        op_handle: str,
    ) -> Iterator[tuple[Any, ...]]:
        """Yield decoded row tuples, fetching pages as needed."""
        client = self._connection.client
        session = self._connection.session_handle

        effective_idle_timeout = self._effective_idle_timeout
        idle_deadline: float = (
            time.monotonic() + effective_idle_timeout
            if effective_idle_timeout is not None
            else float("inf")
        )

        data = initial_data
        token = next_token
        pos = 0
        backoff = _RESULTS_MIN_BACKOFF

        while True:
            if pos < len(data):
                row = data[pos]
                pos += 1
                yield self._decode_row(row)
                if time.monotonic() >= self._query_deadline:
                    raise TimeoutError(
                        f"streaming iteration stopped: query_timeout of "
                        f"{self._effective_query_timeout}s exceeded"
                    )
                continue

            # Fetch next page.
            if not token:
                return
            response = client.fetch_results(session, op_handle, token, "")
            if response.result_type == ResultType.EOS:
                return

            data = response.results.data
            token = response.next_token()
            pos = 0

            if not data:
                now = time.monotonic()
                if now >= self._query_deadline:
                    raise TimeoutError(
                        f"streaming iteration stopped: query_timeout of "
                        f"{self._effective_query_timeout}s exceeded"
                    )
                if now >= idle_deadline:
                    raise TimeoutError(
                        f"streaming iteration stopped: idle_timeout of "
                        f"{effective_idle_timeout}s exceeded"
                    )
                sleep_dur = min(
                    backoff, self._query_deadline - now, idle_deadline - now
                )
                time.sleep(sleep_dur)
                backoff = min(backoff * 2, _RESULTS_MAX_BACKOFF)
            else:
                backoff = _RESULTS_MIN_BACKOFF
                if effective_idle_timeout is not None:
                    idle_deadline = time.monotonic() + effective_idle_timeout

    def _decode_row(self, row: RowData) -> tuple[Any, ...]:
        """Decode a single RowData into a Python tuple."""
        values: list[Any] = []
        for i, raw in enumerate(row.fields):
            col = self._columns[i]
            ft = normalize_flink_type(col.logical_type.type)
            values.append(
                decode_field(raw, ft, col.logical_type.precision, col.logical_type)
            )
        return tuple(values)
