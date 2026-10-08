"""Prometheus metrics of the worker pool (``WORKER_PROMETHEUS_PORT``).

Off unless that port is set; the cloud's workers never set it, and then this
module (and prometheus_client) is never imported. On-prem the metrics store
scrapes ``http://worker:9100/metrics`` on the internal network.

The supervisor exports what only it knows: processes alive, the pool size it
is aiming for, and processes that died and were replaced. Runs, queue depth
and busy workers are the backend's metrics (it sees every worker).
"""

from __future__ import annotations

import logging
import os

from prometheus_client import CollectorRegistry, Counter, Gauge, start_http_server

logger = logging.getLogger("airforge.prometheus")


class PoolExporter:
    def __init__(self, port: int) -> None:
        self.registry = CollectorRegistry()
        self.processes = Gauge("airforge_worker_processes", "Worker processes alive", registry=self.registry)
        self.target = Gauge("airforge_worker_pool_target", "Pool size the supervisor aims for", registry=self.registry)
        self.crashes = Counter(
            "airforge_worker_process_crashes", "Worker processes that died and were replaced", registry=self.registry
        )
        Gauge("airforge_worker_build_info", "The running build", ["version"], registry=self.registry).labels(
            os.environ.get("AIRFORGE_VERSION", "dev")
        ).set(1)
        start_http_server(port, addr="0.0.0.0", registry=self.registry)
        logger.info("Metrics on port %d (/metrics)", port)

    def observe(self, alive: int, target: int) -> None:
        self.processes.set(alive)
        self.target.set(target)

    def crashed(self) -> None:
        self.crashes.inc()
