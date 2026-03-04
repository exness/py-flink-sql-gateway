"""Data models for the Flink SQL Gateway REST API."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any
from urllib.parse import urlparse


class ResultType(StrEnum):
    NOT_READY = "NOT_READY"
    EOS = "EOS"
    PAYLOAD = "PAYLOAD"


class ResultKind(StrEnum):
    SUCCESS = "SUCCESS"
    SUCCESS_WITH_CONTENT = "SUCCESS_WITH_CONTENT"


# ── Request / Response models ──────────────────────────────────────────


@dataclass
class InfoResponse:
    product_name: str
    version: str


@dataclass
class OpenSessionRequest:
    properties: dict[str, str] | None = None
    session_name: str | None = None

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {}
        if self.properties:
            d["properties"] = self.properties
        if self.session_name:
            d["sessionName"] = self.session_name
        return d


@dataclass
class CompleteStatementRequest:
    statement: str
    position: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {"statement": self.statement, "position": self.position}


@dataclass
class ConfigureSessionRequest:
    statement: str
    execution_timeout: int = 0

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {"statement": self.statement}
        if self.execution_timeout:
            d["executionTimeout"] = self.execution_timeout
        return d


@dataclass
class ExecuteStatementRequest:
    statement: str
    execution_config: dict[str, str] | None = None
    execution_timeout: int = 0

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {"statement": self.statement}
        if self.execution_config:
            d["executionConfig"] = self.execution_config
        if self.execution_timeout:
            d["executionTimeout"] = self.execution_timeout
        return d


@dataclass
class RefreshMaterializedTableRequest:
    dynamic_options: dict[str, str] | None = None
    execution_config: dict[str, str] | None = None
    is_periodic: bool = False
    schedule_time: str | None = None
    static_partitions: dict[str, str] | None = None

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {}
        if self.dynamic_options:
            d["dynamicOptions"] = self.dynamic_options
        if self.execution_config:
            d["executionConfig"] = self.execution_config
        if self.is_periodic:
            d["isPeriodic"] = self.is_periodic
        if self.schedule_time:
            d["scheduleTime"] = self.schedule_time
        if self.static_partitions:
            d["staticPartitions"] = self.static_partitions
        return d


# ── Column / Row metadata ─────────────────────────────────────────────


@dataclass
class LogicalType:
    type: str
    nullable: bool = True
    length: int | None = None
    precision: int | None = None
    scale: int | None = None
    resolution: str | None = None
    # ARRAY element type
    element_type: LogicalType | None = None
    # MAP key/value types
    key_type: LogicalType | None = None
    value_type: LogicalType | None = None
    # ROW field descriptors
    fields: list[ColumnInfo] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> LogicalType:
        element_type = None
        key_type = None
        value_type = None
        fields: list[ColumnInfo] = []

        if "elementType" in data:
            element_type = LogicalType.from_dict(data["elementType"])
        if "keyType" in data:
            key_type = LogicalType.from_dict(data["keyType"])
        if "valueType" in data:
            value_type = LogicalType.from_dict(data["valueType"])
        if "fields" in data:
            for f in data["fields"]:
                fields.append(
                    ColumnInfo(
                        name=f.get("name", ""),
                        logical_type=LogicalType.from_dict(f.get("fieldType", {})),
                    )
                )

        return cls(
            type=data.get("type", ""),
            nullable=data.get("nullable", True),
            length=data.get("length"),
            precision=data.get("precision"),
            scale=data.get("scale"),
            resolution=data.get("resolution"),
            element_type=element_type,
            key_type=key_type,
            value_type=value_type,
            fields=fields,
        )


@dataclass
class ColumnInfo:
    name: str
    logical_type: LogicalType
    comment: str = ""

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ColumnInfo:
        return cls(
            name=data.get("name", ""),
            logical_type=LogicalType.from_dict(data.get("logicalType", {})),
            comment=data.get("comment", ""),
        )


@dataclass
class RowData:
    kind: str
    fields: list[Any]

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> RowData:
        return cls(
            kind=data.get("kind", ""),
            fields=data.get("fields", []),
        )


@dataclass
class ResultSet:
    columns: list[ColumnInfo] = field(default_factory=list)
    row_format: str = ""
    data: list[RowData] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ResultSet:
        return cls(
            columns=[ColumnInfo.from_dict(c) for c in data.get("columns", [])],
            row_format=data.get("rowFormat", ""),
            data=[RowData.from_dict(r) for r in data.get("data", [])],
        )


@dataclass
class FetchResultsResponse:
    job_id: str = ""
    next_result_uri: str = ""
    is_query_result: bool = False
    result_kind: ResultKind = ResultKind.SUCCESS
    result_type: ResultType = ResultType.NOT_READY
    results: ResultSet = field(default_factory=ResultSet)

    def next_token(self) -> str:
        """Extract the pagination token from next_result_uri.

        The token is the last path segment of the URI.
        """
        if not self.next_result_uri:
            return ""
        parsed = urlparse(self.next_result_uri)
        segments = [s for s in parsed.path.strip("/").split("/") if s]
        if not segments:
            return ""
        return segments[-1]

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> FetchResultsResponse:
        rk = data.get("resultKind", "SUCCESS")
        rt = data.get("resultType", "NOT_READY")
        return cls(
            job_id=data.get("jobID", ""),
            next_result_uri=data.get("nextResultUri", ""),
            is_query_result=data.get("isQueryResult", False),
            result_kind=ResultKind(rk) if rk else ResultKind.SUCCESS,
            result_type=ResultType(rt) if rt else ResultType.NOT_READY,
            results=ResultSet.from_dict(data.get("results", {})),
        )
