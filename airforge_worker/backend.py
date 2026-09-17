"""The HTTP client for the backend's worker protocol.

One thin wrapper per endpoint, all under ``/api/v1/worker``, authenticated with
the ``X-Worker-Token`` shared secret. Synchronous on purpose: each worker is a
plain process whose only concurrency is a heartbeat thread and the run's log
readers.
"""

from __future__ import annotations

import logging
from typing import Any

import httpx

from airforge_worker.config import Config

logger = logging.getLogger("airforge.backend")


class BackendClient:
    def __init__(self, config: Config) -> None:
        self._config = config
        self._http = httpx.Client(
            base_url=f"{config.backend_url}/api/v1/worker",
            headers={"X-Worker-Token": config.worker_token},
            # Long-poll claims hold the request open ~20s; leave headroom past
            # whatever wait we ask for.
            timeout=httpx.Timeout(10.0, read=60.0),
        )

    def close(self) -> None:
        self._http.close()

    # ── Lifecycle ─────────────────────────────────────────────────────────────
    def register(self, name: str, meta: dict[str, Any]) -> dict[str, Any]:
        # queue_id names the one queue this process drains (WORKER_QUEUE_ID);
        # null joins the shared Playground pool serving every project's
        # system queue.
        resp = self._http.post(
            "/register",
            json={
                "name": name,
                "meta": meta,
                "queue_id": self._config.queue_id or None,
            },
        )
        resp.raise_for_status()
        return resp.json()

    def heartbeat(self, worker_id: str) -> None:
        resp = self._http.post(f"/{worker_id}/heartbeat")
        resp.raise_for_status()

    def deregister(self, worker_id: str) -> None:
        resp = self._http.post(f"/{worker_id}/deregister")
        resp.raise_for_status()

    # ── Work ──────────────────────────────────────────────────────────────────
    def claim(self, worker_id: str, wait_seconds: float) -> dict[str, Any] | None:
        """One long-poll attempt. None means "no work within the window"."""
        resp = self._http.post(
            "/claim", json={"worker_id": worker_id, "wait_seconds": wait_seconds}
        )
        if resp.status_code == 204:
            return None
        resp.raise_for_status()
        return resp.json()

    def append_logs(
        self,
        run_id: str,
        entries: list[dict[str, Any]],
        samples: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """Ship a batch of lines (and any resource samples taken since the last
        batch); the response carries the cancel flag."""
        body: dict[str, Any] = {"entries": entries}
        if samples:
            body["samples"] = samples
        resp = self._http.post(f"/runs/{run_id}/logs", json=body)
        resp.raise_for_status()
        return resp.json()

    def run_control(self, run_id: str) -> dict[str, Any]:
        resp = self._http.get(f"/runs/{run_id}/control")
        resp.raise_for_status()
        return resp.json()

    def complete(
        self,
        run_id: str,
        *,
        status: str,
        exit_code: int | None,
        error: str | None,
        result: Any,
        metrics: dict[str, Any] | None = None,
    ) -> None:
        body: dict[str, Any] = {
            "status": status,
            "exit_code": exit_code,
            "error": error,
            "result": result,
        }
        if metrics is not None:
            # Per-run resource summary (see metrics.py). Optional on the wire.
            body["metrics"] = metrics
        resp = self._http.post(f"/runs/{run_id}/complete", json=body)
        resp.raise_for_status()

    def queue_stats(self) -> dict[str, Any]:
        resp = self._http.get("/queue-stats")
        resp.raise_for_status()
        return resp.json()
