# py-flink-sql-gateway

[![CI](https://github.com/exness/py-flink-sql-gateway/actions/workflows/ci.yml/badge.svg)](https://github.com/exness/py-flink-sql-gateway/actions/workflows/ci.yml)
[![PyPI version](https://img.shields.io/pypi/v/py-flink-sql-gateway)](https://pypi.org/project/py-flink-sql-gateway/)
[![Python](https://img.shields.io/pypi/pyversions/py-flink-sql-gateway)](https://pypi.org/project/py-flink-sql-gateway/)

A lightweight Python driver for the **Apache Flink SQL Gateway**, implementing [PEP 249 (DB-API 2.0)](https://peps.python.org/pep-0249/).

## Installation

```bash
pip install py-flink-sql-gateway
```

## Quick start

```python
from flink_gateway import connect

with connect("http://localhost:8083") as conn:
    # Create a streaming source
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE orders (
                id INT NOT NULL,
                item STRING NOT NULL,
                created_at TIMESTAMP(6) NOT NULL,
                customer ROW<
                    first_name STRING NOT NULL,
                    last_name STRING NOT NULL,
                    age INT NOT NULL
                > NOT NULL,
                notes STRING
            ) WITH (
                'connector' = 'datagen',
                'rows-per-second' = '5',
                'fields.id.kind' = 'sequence',
                'fields.id.start' = '1',
                'fields.id.end' = '100',
                'fields.item.length' = '12',
                'fields.customer.first_name.length' = '8',
                'fields.customer.last_name.length' = '10',
                'fields.customer.age.min' = '21',
                'fields.customer.age.max' = '65',
                'fields.notes.length' = '12'
            )
        """)

    # Query and iterate
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, item, created_at, customer, notes AS note
            FROM orders
        """)
        for i, row in enumerate(cur):
            id_, item, created_at, customer, note = row
            print(
                f"{id_}\t{item}\t{created_at.isoformat()}\t"
                f"{customer['first_name']} {customer['last_name']} ({customer['age']})\t"
                f"{note or ''}"
            )
            if i >= 4:
                break
```

> **Tip:** `ROW` and `MAP` types arrive as Python `dict`, `ARRAY` arrives as `list`. Binary data is passed through as-is.

### Connection options

```python
# Pass Flink session properties
with connect("http://localhost:8083", properties={"pipeline.name": "my-job"}) as conn:
    ...

# Set a query timeout (default: 300s)
with conn.cursor(query_timeout=60.0) as cur:
    cur.execute("SELECT ...")
```

### Low-level REST access

If you need features beyond DB-API, use the exported client directly:

```python
from flink_gateway import FlinkSqlGatewayClient

with FlinkSqlGatewayClient("http://localhost:8083") as client:
    status = client.get_operation_status("session-handle", "operation-handle")
    print("current status:", status)
```

## Type mapping

Python uses `None` for SQL NULLs natively — no wrapper types needed.

| Flink Type                | Python Type        |
|---------------------------|:------------------:|
| TINYINT / SMALLINT / INT  | `int`              |
| BIGINT / INTERVAL         | `int`              |
| FLOAT / DOUBLE            | `float`            |
| BOOLEAN                   | `bool`             |
| CHAR / VARCHAR / STRING   | `str`              |
| DECIMAL                   | `decimal.Decimal`  |
| DATE                      | `datetime.date`    |
| TIME                      | `datetime.time`    |
| TIMESTAMP / TIMESTAMP_LTZ | `datetime.datetime`|
| BINARY / VARBINARY        | raw (passthrough)  |
| ROW                       | `dict`             |
| MAP                       | `dict`             |
| ARRAY                     | `list`             |

Nested complex types (e.g. `MAP<STRING, ROW<..., MAP<STRING, ROW<TIMESTAMP>>>>`) are recursively decoded to the correct Python types.

## Exceptions

The driver follows the [PEP 249 exception hierarchy](https://peps.python.org/pep-0249/#exceptions):

| Exception                | When raised                                      |
|--------------------------|--------------------------------------------------|
| `InterfaceError`         | Misuse of the driver (e.g. unsupported parameters)|
| `OperationalError`       | Server-side or network errors                    |
| `FlinkSqlGatewayError`   | Flink SQL Gateway specific errors (extends `OperationalError`) |

---

## Development & tests

Requires Python 3.11+ and [uv](https://docs.astral.sh/uv/).

```bash
# Install dependencies
uv sync --group dev

# Set up pre-commit hooks (runs on every push)
pre-commit install --hook-type pre-push

# Run unit tests (no Docker needed)
uv run pytest tests/test_client.py tests/test_dbapi.py -v

# Run integration tests (requires Docker)
uv run pytest tests/test_integration.py -v -s -m integration

# Run all pre-commit checks manually
pre-commit run --all-files
```

Integration tests spin up a Flink cluster (JobManager + TaskManager + SQL Gateway) via [testcontainers-python](https://testcontainers-python.readthedocs.io/).

---

## License

MIT
