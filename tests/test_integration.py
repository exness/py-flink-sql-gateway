"""End-to-end integration tests for the Flink SQL Gateway Python driver.

These tests require Docker and take ~2 minutes for container startup.

Run with:
    uv run pytest tests/test_integration.py -v -s
"""

from __future__ import annotations

import datetime
import time
from decimal import Decimal

import pytest

from flink_gateway import connect

# ── Helpers ────────────────────────────────────────────────────────────


def _exec(conn, sql: str) -> None:
    """Execute a DDL/DML statement."""
    with conn.cursor() as cur:
        cur.execute(sql)


def _query_rows(conn, sql: str, timeout: float = 10.0) -> list[tuple]:
    """Execute a query and return all rows within timeout."""
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
    return rows


def _await_job_finished(
    conn,
    timeout: float = 120.0,
    interval: float = 2.0,
) -> str | None:
    """Poll SHOW JOBS until a FINISHED job is observed."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with conn.cursor() as cur:
                cur.execute("SHOW JOBS")
                jobs = cur.fetchall()
            for row in jobs:
                # SHOW JOBS returns (job_id, job_name, status, start_time)
                job_id = row[0]
                status = str(row[2]).upper() if row[2] else ""
                if status == "FINISHED":
                    return job_id
        except Exception:
            pass
        time.sleep(interval)
    raise TimeoutError(f"No FINISHED job observed within {timeout}s")


# ── Test: All Types NOT NULL ──────────────────────────────────────────


@pytest.mark.integration
def test_select_all_types_not_null(flink_gateway_url: str):
    """Verify Python type mapping for NOT NULL Flink columns."""
    with connect(flink_gateway_url) as conn:
        _exec(
            conn,
            """
                    CREATE TABLE test_types_nn
                    (
                        bool_col          BOOLEAN   NOT NULL,
                        tinyint_col       TINYINT   NOT NULL,
                        smallint_col      SMALLINT  NOT NULL,
                        int_col           INTEGER   NOT NULL,
                        bigint_col        BIGINT    NOT NULL,
                        float_col         FLOAT     NOT NULL,
                        double_col DOUBLE NOT NULL,
                        char_col          CHAR      NOT NULL,
                        varchar_col       VARCHAR   NOT NULL,
                        decimal_col       DECIMAL   NOT NULL,
                        date_col          DATE      NOT NULL,
                        time_col          TIME      NOT NULL,
                        timestamp_col     TIMESTAMP NOT NULL,
                        timestamp_ltz_col TIMESTAMP_LTZ(3) NOT NULL,
                        row_col           ROW<score INT,
                        label             STRING> NOT NULL,
                        map_col           MAP<STRING,
                        INT> NOT NULL
                    ) WITH (
                          'connector' = 'datagen',
                          'rows-per-second' = '5'
                          )
                    """,
        )

        with conn.cursor() as cur:
            cur.execute("SELECT * FROM test_types_nn")
            row = cur.fetchone()

        assert row is not None, "Expected at least one row from datagen"

        # Validate Python types
        expected_types = {
            0: bool,  # bool_col
            1: int,  # tinyint_col
            2: int,  # smallint_col
            3: int,  # int_col
            4: int,  # bigint_col
            5: float,  # float_col
            6: float,  # double_col
            7: str,  # char_col
            8: str,  # varchar_col
            9: Decimal,  # decimal_col
            10: datetime.date,  # date_col
            11: datetime.time,  # time_col
            12: datetime.datetime,  # timestamp_col
            13: datetime.datetime,  # timestamp_ltz_col
            14: dict,
            15: dict,
        }

        for idx, expected_type in expected_types.items():
            actual = row[idx]
            assert isinstance(actual, expected_type), (
                f"Column {idx}: expected {expected_type.__name__}, "
                f"got {type(actual).__name__} (value={actual!r})"
            )


# ── Test: All Types NULLABLE ──────────────────────────────────────────


@pytest.mark.integration
def test_select_all_types_nullable(flink_gateway_url: str):
    """Verify Python type mapping for NULLABLE Flink columns."""
    with connect(flink_gateway_url) as conn:
        _exec(
            conn,
            """
                    CREATE TABLE test_types_nullable
                    (
                        bool_col          BOOLEAN,
                        tinyint_col       TINYINT,
                        smallint_col      SMALLINT,
                        int_col           INTEGER,
                        bigint_col        BIGINT,
                        float_col         FLOAT,
                        double_col DOUBLE,
                        char_col          CHAR,
                        varchar_col       VARCHAR,
                        decimal_col       DECIMAL,
                        date_col          DATE,
                        time_col          TIME,
                        timestamp_col     TIMESTAMP,
                        timestamp_ltz_col TIMESTAMP_LTZ(3),
                        row_col           ROW<score INT,
                        label             STRING>,
                        map_col           MAP<STRING,
                        INT>
                    ) WITH (
                          'connector' = 'datagen',
                          'rows-per-second' = '5'
                          )
                    """,
        )

        with conn.cursor() as cur:
            cur.execute("SELECT * FROM test_types_nullable")
            row = cur.fetchone()

        assert row is not None, "Expected at least one row from datagen"

        # For nullable columns datagen still produces non-null values,
        # but the type mapping should still work. The key test is that
        # the driver handles nullable column metadata without errors.
        for idx in range(len(row)):
            # Each value should be either None or a valid Python type.
            val = row[idx]
            assert val is None or isinstance(
                val,
                (
                    bool,
                    int,
                    float,
                    str,
                    Decimal,
                    datetime.date,
                    datetime.time,
                    datetime.datetime,
                    dict,
                    list,
                ),
            ), f"Column {idx}: unexpected type {type(val).__name__}"


# ── Test: Filesystem INSERT + SELECT ──────────────────────────────────


@pytest.mark.integration
def test_filesystem_insert_select(flink_gateway_url: str):
    """INSERT rows via filesystem connector, then SELECT and verify."""
    table_dir = f"/shared/test_table_{int(time.time() * 1000)}"

    with connect(flink_gateway_url) as conn:
        _exec(
            conn,
            f"""
            CREATE TABLE test_fs_table (
                id BIGINT,
                val INT,
                str STRING,
                timestamp1 TIMESTAMP(0),
                timestamp2 TIMESTAMP_LTZ(3),
                time_data TIME,
                date_data DATE
            ) WITH (
                'connector' = 'filesystem',
                'format' = 'csv',
                'path' = 'file://{table_dir}'
            )
        """,
        )

        _exec(
            conn,
            """
                    INSERT INTO test_fs_table
                    VALUES (1, 11, '111', TIMESTAMP '2021-04-15 23:18:36',
                            TO_TIMESTAMP_LTZ(400000000000, 3), TIME '12:32:00',
                            DATE '2023-11-02'),
                           (3, 33, '333', TIMESTAMP '2021-04-16 23:18:36',
                            TO_TIMESTAMP_LTZ(500000000000, 3), TIME '13:32:00',
                            DATE '2023-12-02'),
                           (2, 22, '222', TIMESTAMP '2021-04-17 23:18:36',
                            TO_TIMESTAMP_LTZ(600000000000, 3), TIME '14:32:00',
                            DATE '2023-01-02'),
                           (4, 44, '444', TIMESTAMP '2021-04-18 23:18:36',
                            TO_TIMESTAMP_LTZ(700000000000, 3), TIME '15:32:00',
                            DATE '2023-02-02')
                    """,
        )

        _await_job_finished(conn)

        rows = _query_rows(conn, "SELECT * FROM test_fs_table")

    assert len(rows) == 4, f"Expected 4 rows, got {len(rows)}"

    # Compare all columns as a set (order may vary).
    got = {
        (
            r[0],  # id
            r[1],  # val
            r[2],  # str
            r[3].strftime("%Y-%m-%d %H:%M:%S"),  # timestamp1
            r[4].strftime("%Y-%m-%d %H:%M:%S"),  # timestamp2 (TIMESTAMP_LTZ)
            r[5].strftime("%H:%M:%S"),  # time_data
            r[6].strftime("%Y-%m-%d"),  # date_data
        )
        for r in rows
    }

    want = {
        (
            1,
            11,
            "111",
            "2021-04-15 23:18:36",
            datetime.datetime.fromtimestamp(
                400000000, tz=datetime.timezone.utc
            ).strftime("%Y-%m-%d %H:%M:%S"),
            "12:32:00",
            "2023-11-02",
        ),
        (
            3,
            33,
            "333",
            "2021-04-16 23:18:36",
            datetime.datetime.fromtimestamp(
                500000000, tz=datetime.timezone.utc
            ).strftime("%Y-%m-%d %H:%M:%S"),
            "13:32:00",
            "2023-12-02",
        ),
        (
            2,
            22,
            "222",
            "2021-04-17 23:18:36",
            datetime.datetime.fromtimestamp(
                600000000, tz=datetime.timezone.utc
            ).strftime("%Y-%m-%d %H:%M:%S"),
            "14:32:00",
            "2023-01-02",
        ),
        (
            4,
            44,
            "444",
            "2021-04-18 23:18:36",
            datetime.datetime.fromtimestamp(
                700000000, tz=datetime.timezone.utc
            ).strftime("%Y-%m-%d %H:%M:%S"),
            "15:32:00",
            "2023-02-02",
        ),
    }

    assert got == want, f"Row mismatch:\n  got:  {got}\n  want: {want}"


# ── Test: ROW Complex Type ────────────────────────────────────────────


@pytest.mark.integration
def test_filesystem_row_complex_type(flink_gateway_url: str):
    """INSERT and SELECT rows with complex nested columns.

    Covers: ROW, nested ROW-in-ROW, MAP, ROW as MAP value,
    ARRAY of timestamps, ARRAY of rows.
    """
    table_dir = f"/shared/json_complex_{int(time.time() * 1000)}"

    with connect(flink_gateway_url) as conn:
        _exec(
            conn,
            f"""
            CREATE TABLE json_complex (
                id          BIGINT NOT NULL,
                name        STRING NOT NULL,
                info        ROW<score INT, label STRING> NOT NULL,
                nested_row  ROW<outer_val INT,
                            inner_row ROW<x DOUBLE, y DOUBLE>> NOT NULL,
                tags        MAP<STRING, INT> NOT NULL,
                player_map  MAP<STRING, ROW<score INT, level STRING>> NOT NULL,
                ts_array    ARRAY<TIMESTAMP(3)> NOT NULL,
                row_array   ARRAY<ROW<name STRING, val INT>> NOT NULL,
                deep_map    MAP<STRING, ROW<
                    label STRING,
                    nested_map MAP<STRING, ROW<
                        ts TIMESTAMP(3)
                    >>
                >> NOT NULL
            ) WITH (
                'connector' = 'filesystem',
                'format' = 'json',
                'path' = 'file://{table_dir}'
            )
        """,
        )

        _exec(
            conn,
            """
            INSERT INTO json_complex
            VALUES
                (1, 'alpha',
                 ROW(10, 'A'),
                 ROW(1, ROW(1.5, 2.5)),
                 MAP['a', 1, 'b', 2],
                 MAP['p1', ROW(100, 'gold')],
                 ARRAY[
                     TIMESTAMP '2024-01-01 10:00:00.000',
                     TIMESTAMP '2024-06-15 14:30:00.000'
                 ],
                 ARRAY[ROW('x', 10), ROW('y', 20)],
                 MAP[
                     'env1',
                     ROW(
                         'prod',
                         MAP[
                             'svc1',
                             ROW(TIMESTAMP '2024-03-15 09:30:00.000')
                         ]
                     )
                 ]
                ),
                (2, 'beta',
                 ROW(20, 'B'),
                 ROW(2, ROW(3.0, 4.0)),
                 MAP['c', 3],
                 MAP['p2', ROW(200, 'silver'), 'p3', ROW(300, 'bronze')],
                 ARRAY[TIMESTAMP '2025-12-25 00:00:00.000'],
                 ARRAY[ROW('z', 30)],
                 MAP[
                     'env2',
                     ROW(
                         'staging',
                         MAP[
                             'svc2',
                             ROW(TIMESTAMP '2025-06-01 12:00:00.000'),
                             'svc3',
                             ROW(TIMESTAMP '2025-07-04 18:45:00.000')
                         ]
                     )
                 ]
                )
        """,
        )

        _await_job_finished(conn)

        rows = _query_rows(conn, "SELECT * FROM json_complex")

    assert len(rows) == 2, f"Expected 2 rows, got {len(rows)}"

    by_id = {row[0]: row for row in rows}

    # ── Row 1 ──────────────────────────────────────────────────────
    r = by_id[1]
    assert r[1] == "alpha"

    # Simple ROW
    assert r[2] == {"score": 10, "label": "A"}

    # Nested ROW-in-ROW: inner values decoded to float
    assert r[3]["outer_val"] == 1
    assert r[3]["inner_row"]["x"] == 1.5
    assert r[3]["inner_row"]["y"] == 2.5

    # MAP<STRING, INT>
    assert r[4] == {"a": 1, "b": 2}

    # MAP<STRING, ROW<score INT, level STRING>>
    assert r[5]["p1"]["score"] == 100
    assert r[5]["p1"]["level"] == "gold"

    # ARRAY<TIMESTAMP>: elements decoded to datetime
    assert r[6][0] == datetime.datetime(2024, 1, 1, 10, 0, 0)
    assert r[6][1] == datetime.datetime(2024, 6, 15, 14, 30, 0)

    # ARRAY<ROW<name STRING, val INT>>
    arr = r[7]
    assert len(arr) == 2
    assert arr[0] == {"name": "x", "val": 10}
    assert arr[1] == {"name": "y", "val": 20}

    # ── Row 2 ──────────────────────────────────────────────────────
    r = by_id[2]
    assert r[1] == "beta"
    assert r[2] == {"score": 20, "label": "B"}
    assert r[3]["inner_row"]["x"] == 3.0
    assert r[4] == {"c": 3}
    assert r[5]["p2"]["score"] == 200
    assert r[5]["p3"]["level"] == "bronze"
    assert r[6][0] == datetime.datetime(2025, 12, 25, 0, 0, 0)
    assert r[7][0] == {"name": "z", "val": 30}

    # ── deep_map assertions ────────────────────────────────────────
    # Row 1
    r = by_id[1]
    dm = r[8]
    assert dm["env1"]["label"] == "prod"
    assert dm["env1"]["nested_map"]["svc1"]["ts"] == datetime.datetime(
        2024, 3, 15, 9, 30, 0
    )

    # Row 2
    r = by_id[2]
    dm = r[8]
    assert dm["env2"]["label"] == "staging"
    assert dm["env2"]["nested_map"]["svc2"]["ts"] == datetime.datetime(
        2025, 6, 1, 12, 0, 0
    )
    assert dm["env2"]["nested_map"]["svc3"]["ts"] == datetime.datetime(
        2025, 7, 4, 18, 45, 0
    )
