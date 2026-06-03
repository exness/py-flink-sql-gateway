"""Flink SQL Gateway REST API client.

This module provides a low-level HTTP client that wraps every endpoint of
the Apache Flink SQL Gateway REST API.
"""

from __future__ import annotations

from typing import Any

import httpx

from flink_gateway.exceptions import FlinkSqlGatewayError
from flink_gateway.models import (
    CompleteStatementRequest,
    ConfigureSessionRequest,
    ExecuteStatementRequest,
    FetchResultsResponse,
    InfoResponse,
    OpenSessionRequest,
    RefreshMaterializedTableRequest,
)


class FlinkSqlGatewayClient:
    """Low-level client for the Flink SQL Gateway REST API.

    Args:
        base_url: Root URL of the SQL Gateway, e.g. ``http://localhost:8083``.
        http_client: Optional pre-configured ``httpx.Client``.  When *None*, a
            default client is created with a 30-second timeout.
        api_version: REST API version prefix (``"v1"``, ``"v2"``, or ``"v3"``).
            Defaults to ``"v3"``.
    """

    def __init__(
        self,
        base_url: str,
        http_client: httpx.Client | None = None,
        api_version: str = "v3",
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._api_version = api_version or "v3"
        self._owns_client = http_client is None
        self._client = http_client or httpx.Client(timeout=30.0)

    # ── Context manager ────────────────────────────────────────────────

    def __enter__(self) -> FlinkSqlGatewayClient:
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    def close(self) -> None:
        """Close the underlying HTTP client if we own it."""
        if self._owns_client:
            self._client.close()

    # ── URL helpers ────────────────────────────────────────────────────

    def _build_endpoint(self, *segments: str) -> str:
        parts = [self._base_url, self._api_version, *segments]
        return "/".join(parts)

    # ── Metadata ───────────────────────────────────────────────────────

    def get_info(self) -> InfoResponse:
        """GET /info — cluster metadata."""
        url = self._build_endpoint("info")
        resp = self._client.get(url)
        self._check_response(resp, "get info")
        data = resp.json()
        return InfoResponse(
            product_name=data.get("productName", ""),
            version=data.get("version", ""),
        )

    def get_api_versions(self) -> list[str]:
        """GET /api_versions — supported REST API versions."""
        url = self._build_endpoint("api_versions")
        resp = self._client.get(url)
        self._check_response(resp, "get api versions")
        return resp.json().get("versions", [])

    # ── Session management ─────────────────────────────────────────────

    def open_session(
        self,
        request: OpenSessionRequest | None = None,
    ) -> str:
        """POST /sessions — open a new session.

        Returns:
            The session handle string.
        """
        url = self._build_endpoint("sessions")
        body = request.to_dict() if request else {}
        resp = self._client.post(url, json=body)
        self._check_response(resp, "open session")
        return resp.json()["sessionHandle"]

    def close_session(self, session_handle: str) -> None:
        """DELETE /sessions/{session_handle}."""
        url = self._build_endpoint("sessions", session_handle)
        resp = self._client.delete(url)
        self._check_response(resp, "close session")

    def get_session_config(self, session_handle: str) -> dict[str, str]:
        """GET /sessions/{session_handle} — current session properties."""
        url = self._build_endpoint("sessions", session_handle)
        resp = self._client.get(url)
        self._check_response(resp, "get session config")
        return resp.json().get("properties", {})

    def configure_session(
        self,
        session_handle: str,
        request: ConfigureSessionRequest,
    ) -> None:
        """POST /sessions/{session_handle}/configure-session."""
        url = self._build_endpoint("sessions", session_handle, "configure-session")
        resp = self._client.post(url, json=request.to_dict())
        self._check_response(resp, "configure session")

    def heartbeat(self, session_handle: str) -> None:
        """POST /sessions/{session_handle}/heartbeat."""
        url = self._build_endpoint("sessions", session_handle, "heartbeat")
        resp = self._client.post(url, json={})
        self._check_response(resp, "heartbeat")

    def complete_statement(
        self,
        session_handle: str,
        request: CompleteStatementRequest,
    ) -> list[str]:
        """GET /sessions/{session_handle}/complete-statement.

        Returns:
            List of completion candidates.
        """
        url = self._build_endpoint("sessions", session_handle, "complete-statement")
        # The Flink Gateway uses GET with a JSON body for this endpoint.
        resp = self._client.request("GET", url, json=request.to_dict())
        self._check_response(resp, "complete statement")
        return resp.json().get("candidates", [])

    # ── Statement execution ────────────────────────────────────────────

    def execute_statement(
        self,
        session_handle: str,
        request: ExecuteStatementRequest,
    ) -> str:
        """POST /sessions/{session_handle}/statements.

        Returns:
            The operation handle string.
        """
        url = self._build_endpoint("sessions", session_handle, "statements")
        resp = self._client.post(url, json=request.to_dict())
        self._check_response(resp, "execute statement")
        return resp.json()["operationHandle"]

    def fetch_results(
        self,
        session_handle: str,
        operation_handle: str,
        token: str,
        row_format: str = "",
    ) -> FetchResultsResponse:
        """GET /sessions/{sh}/operations/{oh}/result/{token}.

        Args:
            session_handle: Session handle.
            operation_handle: Operation handle.
            token: Pagination token (``"0"`` for the first batch).
            row_format: Optional row format (e.g. ``"json"``).

        Returns:
            Parsed :class:`FetchResultsResponse`.
        """
        url = self._build_endpoint(
            "sessions",
            session_handle,
            "operations",
            operation_handle,
            "result",
            token,
        )
        params: dict[str, str] = {}
        if row_format:
            params["rowFormat"] = row_format
        resp = self._client.get(url, params=params)
        self._check_response(resp, "fetch results")
        return FetchResultsResponse.from_dict(resp.json())

    # ── Operation management ───────────────────────────────────────────

    def get_operation_status(
        self,
        session_handle: str,
        operation_handle: str,
    ) -> str:
        """GET /sessions/{sh}/operations/{oh}/status.

        Returns:
            The status string (e.g. ``"RUNNING"``, ``"FINISHED"``).
        """
        url = self._build_endpoint(
            "sessions",
            session_handle,
            "operations",
            operation_handle,
            "status",
        )
        resp = self._client.get(url)
        self._check_response(resp, "get operation status")
        return resp.json()["status"]

    def cancel_operation(
        self,
        session_handle: str,
        operation_handle: str,
    ) -> str:
        """POST /sessions/{sh}/operations/{oh}/cancel.

        Returns:
            The operation status after cancellation.
        """
        url = self._build_endpoint(
            "sessions",
            session_handle,
            "operations",
            operation_handle,
            "cancel",
        )
        resp = self._client.post(url, json={})
        self._check_response(resp, "cancel operation")
        return resp.json()["status"]

    def close_operation(
        self,
        session_handle: str,
        operation_handle: str,
    ) -> str:
        """DELETE /sessions/{sh}/operations/{oh}/close.

        Returns:
            The operation status after closure.
        """
        url = self._build_endpoint(
            "sessions",
            session_handle,
            "operations",
            operation_handle,
            "close",
        )
        resp = self._client.delete(url)
        self._check_response(resp, "close operation")
        return resp.json()["status"]

    # ── Materialized tables ────────────────────────────────────────────

    def refresh_materialized_table(
        self,
        session_handle: str,
        identifier: str,
        request: RefreshMaterializedTableRequest | None = None,
    ) -> str:
        """POST /sessions/{sh}/materialized-tables/{id}/refresh.

        Returns:
            The operation handle string.
        """
        url = self._build_endpoint(
            "sessions",
            session_handle,
            "materialized-tables",
            identifier,
            "refresh",
        )
        body = request.to_dict() if request else {}
        resp = self._client.post(url, json=body)
        self._check_response(resp, "refresh materialized table")
        return resp.json()["operationHandle"]

    # ── Internal helpers ───────────────────────────────────────────────

    @staticmethod
    def _check_response(resp: httpx.Response, action: str) -> None:
        if resp.status_code == 200:
            return
        detail = ""
        try:
            errors = resp.json().get("errors", [])
            if errors:
                detail = ": " + "; ".join(errors)
        except (ValueError, KeyError):
            pass
        raise FlinkSqlGatewayError(
            f"{action} failed: {resp.status_code} {resp.reason_phrase}{detail}"
        )
