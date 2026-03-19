"""PEP 249 Connection implementation for the Flink SQL Gateway."""

from __future__ import annotations

from typing import Any

import httpx

from flink_gateway.client import FlinkSqlGatewayClient
from flink_gateway.cursor import Cursor
from flink_gateway.exceptions import NotSupportedError, ProgrammingError
from flink_gateway.models import OpenSessionRequest


class Connection:
    """PEP 249 Connection to a Flink SQL Gateway.

    Do not instantiate directly; use :func:`flink_gateway.connect`.
    """

    def __init__(
        self,
        client: FlinkSqlGatewayClient,
        session_handle: str,
        *,
        query_timeout: float | None = None,
        idle_timeout: float | None = None,
        _owns_client: bool = True,
    ) -> None:
        self._client = client
        self._session_handle = session_handle
        self._query_timeout = query_timeout
        self._idle_timeout = idle_timeout
        self._owns_client = _owns_client
        self._closed = False

    # ── Public read-only properties ────────────────────────────────

    @property
    def client(self) -> FlinkSqlGatewayClient:
        """The underlying REST client."""
        return self._client

    @property
    def session_handle(self) -> str:
        """The session handle for this connection."""
        return self._session_handle

    @property
    def closed(self) -> bool:
        """Whether this connection has been closed."""
        return self._closed

    # ── Context manager ────────────────────────────────────────────

    def __enter__(self) -> Connection:
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    # ── PEP 249 interface ──────────────────────────────────────────

    def close(self) -> None:
        """Close the session and release resources."""
        if not self._closed:
            self._closed = True
            try:
                self._client.close_session(self._session_handle)
            except Exception:
                pass
            if self._owns_client:
                self._client.close()

    def commit(self) -> None:
        """No-op — Flink SQL Gateway does not support transactions."""

    def rollback(self) -> None:
        """Not supported — Flink SQL Gateway does not support transactions."""
        raise NotSupportedError("Flink SQL Gateway does not support transactions")

    def cursor(self) -> Cursor:
        """Create a new Cursor bound to this connection."""
        if self._closed:
            raise ProgrammingError("connection is closed")
        return Cursor(
            self,
            query_timeout=self._query_timeout,
            idle_timeout=self._idle_timeout,
        )


def connect(
    url: str,
    *,
    properties: dict[str, str] | None = None,
    api_version: str = "v3",
    http_client: httpx.Client | None = None,
    query_timeout: float | None = None,
    idle_timeout: float | None = None,
) -> Connection:
    """Open a connection to a Flink SQL Gateway.

    This is the PEP 249 module-level ``connect()`` function.

    Args:
        url: Gateway URL, e.g. ``"http://localhost:8083"``.
        properties: Optional session properties.
        api_version: REST API version (default ``"v3"``).
        http_client: Optional pre-configured :class:`httpx.Client`
            for custom SSL, timeouts, authentication, etc.
        query_timeout: Maximum seconds a query may run
            before raising :class:`~flink_gateway.TimeoutError`.
            ``None`` means no limit.
        idle_timeout: Maximum seconds to wait between rows during streaming
            iteration before raising :class:`~flink_gateway.TimeoutError`.
            ``None`` means no limit.

    Returns:
        A :class:`Connection` instance.
    """
    client = FlinkSqlGatewayClient(
        url, http_client=http_client, api_version=api_version
    )
    try:
        session_handle = client.open_session(
            OpenSessionRequest(properties=properties) if properties else None
        )
    except Exception:
        client.close()
        raise
    return Connection(
        client,
        session_handle,
        query_timeout=query_timeout,
        idle_timeout=idle_timeout,
        _owns_client=True,
    )
