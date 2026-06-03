"""Tests for the Flink SQL Gateway REST client.

Uses httpx.MockTransport to simulate the gateway HTTP responses.
"""

from __future__ import annotations

import json

import httpx
import pytest

from flink_gateway import (
    CompleteStatementRequest,
    ConfigureSessionRequest,
    ExecuteStatementRequest,
    FetchResultsResponse,
    FlinkSqlGatewayClient,
    FlinkSqlGatewayError,
    OpenSessionRequest,
    RefreshMaterializedTableRequest,
    ResultKind,
    ResultType,
)

# ── Helpers ────────────────────────────────────────────────────────────


def _mock_transport(handler):
    """Create a httpx.MockTransport from a handler function."""
    return httpx.MockTransport(handler)


def _json_response(data: dict, status_code: int = 200) -> httpx.Response:
    return httpx.Response(
        status_code,
        json=data,
    )


# ── get_info ───────────────────────────────────────────────────────────


class TestGetInfo:
    def test_success(self):
        def handler(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/v3/info"
            assert request.method == "GET"
            return _json_response({"productName": "Apache Flink", "version": "1.20.0"})

        transport = _mock_transport(handler)
        client = httpx.Client(transport=transport)
        gw = FlinkSqlGatewayClient("http://localhost:8083", http_client=client)
        info = gw.get_info()
        assert info.product_name == "Apache Flink"
        assert info.version == "1.20.0"

    def test_failure(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(500, json={})

        transport = _mock_transport(handler)
        client = httpx.Client(transport=transport)
        gw = FlinkSqlGatewayClient("http://localhost:8083", http_client=client)
        with pytest.raises(FlinkSqlGatewayError, match="get info failed"):
            gw.get_info()


# ── get_api_versions ───────────────────────────────────────────────────


class TestGetAPIVersions:
    def test_success(self):
        def handler(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/v3/api_versions"
            return _json_response({"versions": ["v1", "v2", "v3"]})

        transport = _mock_transport(handler)
        client = httpx.Client(transport=transport)
        gw = FlinkSqlGatewayClient("http://localhost:8083", http_client=client)
        versions = gw.get_api_versions()
        assert versions == ["v1", "v2", "v3"]


# ── open_session / close_session ───────────────────────────────────────


class TestSessionManagement:
    def test_open_session(self):
        def handler(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/v3/sessions"
            assert request.method == "POST"
            body = json.loads(request.content)
            assert body["properties"]["key1"] == "val1"
            return _json_response({"sessionHandle": "sess-abc-123"})

        transport = _mock_transport(handler)
        client = httpx.Client(transport=transport)
        gw = FlinkSqlGatewayClient("http://localhost:8083", http_client=client)
        handle = gw.open_session(OpenSessionRequest(properties={"key1": "val1"}))
        assert handle == "sess-abc-123"

    def test_open_session_empty(self):
        def handler(request: httpx.Request) -> httpx.Response:
            body = json.loads(request.content)
            assert body == {}
            return _json_response({"sessionHandle": "sess-empty"})

        transport = _mock_transport(handler)
        client = httpx.Client(transport=transport)
        gw = FlinkSqlGatewayClient("http://localhost:8083", http_client=client)
        handle = gw.open_session()
        assert handle == "sess-empty"

    def test_close_session(self):
        def handler(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/v3/sessions/sess-abc-123"
            assert request.method == "DELETE"
            return _json_response({})

        transport = _mock_transport(handler)
        client = httpx.Client(transport=transport)
        gw = FlinkSqlGatewayClient("http://localhost:8083", http_client=client)
        gw.close_session("sess-abc-123")  # should not raise

    def test_open_session_failure(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(500, json={})

        transport = _mock_transport(handler)
        client = httpx.Client(transport=transport)
        gw = FlinkSqlGatewayClient("http://localhost:8083", http_client=client)
        with pytest.raises(FlinkSqlGatewayError, match="open session failed"):
            gw.open_session()


# ── get_session_config ─────────────────────────────────────────────────


class TestGetSessionConfig:
    def test_success(self):
        def handler(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/v3/sessions/sess-1"
            assert request.method == "GET"
            return _json_response(
                {"properties": {"execution.runtime-mode": "STREAMING"}}
            )

        transport = _mock_transport(handler)
        client = httpx.Client(transport=transport)
        gw = FlinkSqlGatewayClient("http://localhost:8083", http_client=client)
        config = gw.get_session_config("sess-1")
        assert config == {"execution.runtime-mode": "STREAMING"}


# ── configure_session ──────────────────────────────────────────────────


class TestConfigureSession:
    def test_success(self):
        def handler(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/v3/sessions/sess-1/configure-session"
            assert request.method == "POST"
            body = json.loads(request.content)
            assert body["statement"] == "SET 'key' = 'val'"
            return _json_response({})

        transport = _mock_transport(handler)
        client = httpx.Client(transport=transport)
        gw = FlinkSqlGatewayClient("http://localhost:8083", http_client=client)
        gw.configure_session(
            "sess-1",
            ConfigureSessionRequest(statement="SET 'key' = 'val'"),
        )


# ── heartbeat ──────────────────────────────────────────────────────────


class TestHeartbeat:
    def test_success(self):
        def handler(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/v3/sessions/sess-1/heartbeat"
            assert request.method == "POST"
            return _json_response({})

        transport = _mock_transport(handler)
        client = httpx.Client(transport=transport)
        gw = FlinkSqlGatewayClient("http://localhost:8083", http_client=client)
        gw.heartbeat("sess-1")

    def test_failure(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(404, json={})

        transport = _mock_transport(handler)
        client = httpx.Client(transport=transport)
        gw = FlinkSqlGatewayClient("http://localhost:8083", http_client=client)
        with pytest.raises(FlinkSqlGatewayError, match="heartbeat failed"):
            gw.heartbeat("sess-1")


# ── execute_statement ──────────────────────────────────────────────────


class TestExecuteStatement:
    def test_success(self):
        def handler(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/v3/sessions/sess-1/statements"
            assert request.method == "POST"
            body = json.loads(request.content)
            assert body["statement"] == "SELECT 1"
            return _json_response({"operationHandle": "op-xyz"})

        transport = _mock_transport(handler)
        client = httpx.Client(transport=transport)
        gw = FlinkSqlGatewayClient("http://localhost:8083", http_client=client)
        handle = gw.execute_statement(
            "sess-1",
            ExecuteStatementRequest(statement="SELECT 1"),
        )
        assert handle == "op-xyz"

    def test_with_config(self):
        def handler(request: httpx.Request) -> httpx.Response:
            body = json.loads(request.content)
            assert body["executionConfig"]["pipeline.name"] == "test-job"
            assert body["executionTimeout"] == 30
            return _json_response({"operationHandle": "op-cfg"})

        transport = _mock_transport(handler)
        client = httpx.Client(transport=transport)
        gw = FlinkSqlGatewayClient("http://localhost:8083", http_client=client)
        handle = gw.execute_statement(
            "sess-1",
            ExecuteStatementRequest(
                statement="SELECT 1",
                execution_config={"pipeline.name": "test-job"},
                execution_timeout=30,
            ),
        )
        assert handle == "op-cfg"


# ── fetch_results ──────────────────────────────────────────────────────


class TestFetchResults:
    def test_payload(self):
        def handler(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/v3/sessions/s/operations/op/result/0"
            assert request.url.params.get("rowFormat") == "json"
            return _json_response(
                {
                    "jobID": "job-1",
                    "nextResultUri": "/v3/sessions/s/operations/op/result/1",
                    "isQueryResult": True,
                    "resultKind": "SUCCESS_WITH_CONTENT",
                    "resultType": "PAYLOAD",
                    "results": {
                        "columns": [
                            {
                                "name": "id",
                                "logicalType": {"type": "INTEGER", "nullable": False},
                            }
                        ],
                        "data": [
                            {"kind": "INSERT", "fields": [42]},
                        ],
                    },
                }
            )

        transport = _mock_transport(handler)
        client = httpx.Client(transport=transport)
        gw = FlinkSqlGatewayClient("http://localhost:8083", http_client=client)
        result = gw.fetch_results("s", "op", "0", row_format="json")

        assert result.job_id == "job-1"
        assert result.result_type == ResultType.PAYLOAD
        assert result.result_kind == ResultKind.SUCCESS_WITH_CONTENT
        assert result.is_query_result is True
        assert len(result.results.columns) == 1
        assert result.results.columns[0].name == "id"
        assert result.results.columns[0].logical_type.nullable is False
        assert len(result.results.data) == 1
        assert result.results.data[0].fields == [42]
        assert result.next_token() == "1"

    def test_eos(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return _json_response(
                {
                    "resultType": "EOS",
                    "resultKind": "SUCCESS_WITH_CONTENT",
                    "isQueryResult": True,
                    "results": {"columns": [], "data": []},
                }
            )

        transport = _mock_transport(handler)
        client = httpx.Client(transport=transport)
        gw = FlinkSqlGatewayClient("http://localhost:8083", http_client=client)
        result = gw.fetch_results("s", "op", "5")
        assert result.result_type == ResultType.EOS

    def test_not_ready(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return _json_response(
                {
                    "resultType": "NOT_READY",
                    "results": {"columns": [], "data": []},
                }
            )

        transport = _mock_transport(handler)
        client = httpx.Client(transport=transport)
        gw = FlinkSqlGatewayClient("http://localhost:8083", http_client=client)
        result = gw.fetch_results("s", "op", "0")
        assert result.result_type == ResultType.NOT_READY

    def test_error_with_details(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                500,
                json={"errors": ["table not found", "catalog missing"]},
            )

        transport = _mock_transport(handler)
        client = httpx.Client(transport=transport)
        gw = FlinkSqlGatewayClient("http://localhost:8083", http_client=client)
        with pytest.raises(FlinkSqlGatewayError, match="table not found"):
            gw.fetch_results("s", "op", "0")

    def test_error_without_details(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(500, text="Internal Server Error")

        transport = _mock_transport(handler)
        client = httpx.Client(transport=transport)
        gw = FlinkSqlGatewayClient("http://localhost:8083", http_client=client)
        with pytest.raises(FlinkSqlGatewayError, match="500"):
            gw.fetch_results("s", "op", "0")


# ── operation management ───────────────────────────────────────────────


class TestOperationManagement:
    def test_get_operation_status(self):
        def handler(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/v3/sessions/s/operations/op/status"
            assert request.method == "GET"
            return _json_response({"status": "FINISHED"})

        transport = _mock_transport(handler)
        client = httpx.Client(transport=transport)
        gw = FlinkSqlGatewayClient("http://localhost:8083", http_client=client)
        status = gw.get_operation_status("s", "op")
        assert status == "FINISHED"

    def test_cancel_operation(self):
        def handler(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/v3/sessions/s/operations/op/cancel"
            assert request.method == "POST"
            return _json_response({"status": "CANCELLED"})

        transport = _mock_transport(handler)
        client = httpx.Client(transport=transport)
        gw = FlinkSqlGatewayClient("http://localhost:8083", http_client=client)
        status = gw.cancel_operation("s", "op")
        assert status == "CANCELLED"

    def test_close_operation(self):
        def handler(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/v3/sessions/s/operations/op/close"
            assert request.method == "DELETE"
            return _json_response({"status": "CLOSED"})

        transport = _mock_transport(handler)
        client = httpx.Client(transport=transport)
        gw = FlinkSqlGatewayClient("http://localhost:8083", http_client=client)
        status = gw.close_operation("s", "op")
        assert status == "CLOSED"


# ── refresh_materialized_table ─────────────────────────────────────────


class TestRefreshMaterializedTable:
    def test_success(self):
        def handler(request: httpx.Request) -> httpx.Response:
            assert "/materialized-tables/my_table/refresh" in request.url.path
            assert request.method == "POST"
            return _json_response({"operationHandle": "op-refresh"})

        transport = _mock_transport(handler)
        client = httpx.Client(transport=transport)
        gw = FlinkSqlGatewayClient("http://localhost:8083", http_client=client)
        handle = gw.refresh_materialized_table(
            "s",
            "my_table",
            RefreshMaterializedTableRequest(is_periodic=True),
        )
        assert handle == "op-refresh"


# ── complete_statement ─────────────────────────────────────────────────


class TestCompleteStatement:
    def test_success(self):
        def handler(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/v3/sessions/s/complete-statement"
            return _json_response({"candidates": ["SELECT", "SET"]})

        transport = _mock_transport(handler)
        client = httpx.Client(transport=transport)
        gw = FlinkSqlGatewayClient("http://localhost:8083", http_client=client)
        candidates = gw.complete_statement(
            "s",
            CompleteStatementRequest(statement="SEL", position=3),
        )
        assert candidates == ["SELECT", "SET"]


# ── next_token extraction ─────────────────────────────────────────────


class TestNextToken:
    def test_extracts_last_segment(self):
        resp = FetchResultsResponse(
            next_result_uri="/v3/sessions/s/operations/op/result/42"
        )
        assert resp.next_token() == "42"

    def test_empty_uri(self):
        resp = FetchResultsResponse(next_result_uri="")
        assert resp.next_token() == ""

    def test_full_url(self):
        resp = FetchResultsResponse(
            next_result_uri="http://localhost:8083/v3/sessions/s/operations/op/result/7"
        )
        assert resp.next_token() == "7"


# ── API version ────────────────────────────────────────────────────────


class TestAPIVersion:
    def test_custom_version(self):
        def handler(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/v1/info"
            return _json_response({"productName": "Flink", "version": "1.18.0"})

        transport = _mock_transport(handler)
        client = httpx.Client(transport=transport)
        gw = FlinkSqlGatewayClient(
            "http://localhost:8083",
            http_client=client,
            api_version="v1",
        )
        info = gw.get_info()
        assert info.version == "1.18.0"

    def test_trailing_slash_stripped(self):
        def handler(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/v3/info"
            return _json_response({"productName": "Flink", "version": "1.20.0"})

        transport = _mock_transport(handler)
        client = httpx.Client(transport=transport)
        gw = FlinkSqlGatewayClient(
            "http://localhost:8083/",
            http_client=client,
        )
        gw.get_info()


# ── context manager ────────────────────────────────────────────────────


class TestContextManager:
    def test_closes_owned_client(self):
        with FlinkSqlGatewayClient("http://localhost:8083") as gw:
            assert gw._client is not None
        # After exiting, the owned client should be closed.
        assert gw._client.is_closed

    def test_does_not_close_external_client(self):
        ext_client = httpx.Client()
        with FlinkSqlGatewayClient(
            "http://localhost:8083", http_client=ext_client
        ) as _gw:
            pass
        # External client should NOT be closed by the gateway.
        assert not ext_client.is_closed
        ext_client.close()
