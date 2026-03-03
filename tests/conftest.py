"""Pytest fixtures for Flink SQL Gateway integration tests.

Spins up a three-container Flink cluster (JobManager, TaskManager,
SQL Gateway) via testcontainers-python.
"""

from __future__ import annotations

import os
import tempfile
import time

import httpx
import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.network import Network

FLINK_IMAGE = "flink:2.2.0"


def _wait_for_http(url: str, timeout: float = 120.0, interval: float = 2.0) -> None:
    """Poll an HTTP endpoint until it returns 200."""
    deadline = time.time() + timeout
    last_err = None
    while time.time() < deadline:
        try:
            resp = httpx.get(url, timeout=5.0)
            if resp.status_code == 200:
                return
        except Exception as e:
            last_err = e
        time.sleep(interval)
    raise TimeoutError(
        f"HTTP endpoint {url} did not become ready "
        f"within {timeout}s. Last error: {last_err}"
    )


@pytest.fixture(scope="session")
def flink_gateway_url():
    """Start a Flink cluster and SQL Gateway, yield the gateway URL.

    Lifecycle:
        1. Create a Docker network
        2. Start JobManager (port 8081)
        3. Start TaskManager
        4. Start SQL Gateway (port 8083)
        5. Yield gateway URL
        6. Tear down all containers
    """
    shared_dir = tempfile.mkdtemp(prefix="flink-e2e-")
    os.chmod(shared_dir, 0o777)

    flink_props = "jobmanager.rpc.address: jobmanager"

    network = Network()
    network.__enter__()

    jobmanager = (
        DockerContainer(FLINK_IMAGE)
        .with_network(network)
        .with_env("FLINK_PROPERTIES", flink_props)
        .with_exposed_ports(8081)
        .with_command("jobmanager")
        .with_volume_mapping(shared_dir, "/shared", "rw")
        .with_kwargs(hostname="jobmanager")
    )
    jobmanager.start()

    taskmanager = (
        DockerContainer(FLINK_IMAGE)
        .with_network(network)
        .with_env("FLINK_PROPERTIES", flink_props)
        .with_command("taskmanager")
        .with_volume_mapping(shared_dir, "/shared", "rw")
        .with_kwargs(hostname="taskmanager")
    )
    taskmanager.start()

    sql_gateway = (
        DockerContainer(FLINK_IMAGE)
        .with_network(network)
        .with_env("FLINK_PROPERTIES", flink_props)
        .with_exposed_ports(8083)
        .with_command(
            "/opt/flink/bin/sql-gateway.sh start-foreground "
            "-Dsql-gateway.endpoint.rest.address=localhost "
            "-Drest.address=jobmanager"
        )
        .with_volume_mapping(shared_dir, "/shared", "rw")
    )
    sql_gateway.start()

    host = sql_gateway.get_container_host_ip()
    port = sql_gateway.get_exposed_port(8083)
    gateway_url = f"http://{host}:{port}"

    # Wait for SQL Gateway /info endpoint to be available.
    _wait_for_http(f"{gateway_url}/v3/info", timeout=120.0)

    yield gateway_url

    # Teardown
    sql_gateway.stop()
    taskmanager.stop()
    jobmanager.stop()
    network.__exit__(None, None, None)

    try:
        import shutil

        shutil.rmtree(shared_dir, ignore_errors=True)
    except Exception:
        pass
