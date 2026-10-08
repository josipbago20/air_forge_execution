"""The pool supervisor: keeps N warm workers alive, restarts crashed ones, and
(optionally) scales the pool with queue depth.

Sizing modes:

* **Fixed** (default): exactly ``WORKER_POOL_SIZE`` workers.
* **Autoscale** (``WORKER_AUTOSCALE=true``): between ``WORKER_MIN_WORKERS`` and
  ``WORKER_MAX_WORKERS``. Scale-up is immediate — one new worker per queued run
  that has no one to serve it. Scale-down is deliberately lazy: only after the
  queue has been empty for ``WORKER_SCALE_DOWN_IDLE_SECONDS``, one worker at a
  time, so a bursty queue doesn't thrash the pool.

Scaling down sends SIGTERM, which workers treat as "finish what you're doing,
then leave" — a mid-run worker is never killed by the autoscaler.
"""

from __future__ import annotations

import logging
import multiprocessing as mp
import signal
import time

from airforge_worker import sandbox
from airforge_worker.backend import BackendClient
from airforge_worker.config import Config
from airforge_worker.worker import worker_process_main

logger = logging.getLogger("airforge.supervisor")


class Supervisor:
    def __init__(self, config: Config) -> None:
        self._config = config
        self._ctx = mp.get_context("spawn")
        self._procs: dict[int, mp.process.BaseProcess] = {}
        self._next_slot = 1
        self._stopping = False
        self._last_nonempty_queue = time.monotonic()
        self._stats_client: BackendClient | None = None
        self._exporter = None  # PoolExporter when WORKER_PROMETHEUS_PORT is set

    # ── Pool primitives ───────────────────────────────────────────────────────
    def _spawn(self) -> None:
        slot = self._next_slot
        self._next_slot += 1
        proc = self._ctx.Process(
            target=worker_process_main, args=(slot,), name=f"airforge-worker-{slot}"
        )
        proc.start()
        self._procs[slot] = proc
        logger.info("Spawned worker w%d (pid %s) — pool size %d", slot, proc.pid, len(self._procs))

    def _retire_one(self) -> None:
        """Ask the newest worker to leave once it finishes its current run."""
        if not self._procs:
            return
        slot = max(self._procs)
        proc = self._procs[slot]
        if proc.is_alive():
            proc.terminate()  # SIGTERM → graceful in the worker
        logger.info("Retiring worker w%d — pool size will drop to %d", slot, len(self._procs) - 1)

    def _reap_and_restart(self) -> None:
        """Bury exited processes; if we're below target and not stopping, refill."""
        for slot, proc in list(self._procs.items()):
            if not proc.is_alive():
                proc.join(timeout=0)
                exit_code = proc.exitcode
                del self._procs[slot]
                if not self._stopping and exit_code not in (0, -signal.SIGTERM):
                    logger.warning(
                        "Worker w%d died (exit %s) — replacing it", slot, exit_code
                    )
                    if self._exporter is not None:
                        self._exporter.crashed()
                    self._spawn()

    # ── Autoscaling ───────────────────────────────────────────────────────────
    def _desired_size(self) -> int:
        cfg = self._config
        if not cfg.autoscale:
            return cfg.pool_size

        if self._stats_client is None:
            self._stats_client = BackendClient(cfg)
        try:
            stats = self._stats_client.queue_stats()
        except Exception as exc:  # noqa: BLE001
            logger.warning("Queue stats unavailable (%s); holding pool size", exc)
            return len(self._procs)

        queued = int(stats.get("queued", 0))
        running = int(stats.get("running", 0))
        now = time.monotonic()
        if queued > 0:
            self._last_nonempty_queue = now

        current = len(self._procs)
        # Enough hands for everything in flight plus everything waiting.
        needed = max(cfg.min_workers, min(cfg.max_workers, running + queued))

        if needed > current:
            return needed
        idle_for = now - self._last_nonempty_queue
        if current > cfg.min_workers and idle_for >= cfg.scale_down_idle_seconds:
            return current - 1  # one at a time, and only after sustained quiet
        return current

    # ── Main loop ─────────────────────────────────────────────────────────────
    def run(self) -> None:
        cfg = self._config

        # Fail closed and fail loud: never fall back to running untrusted code
        # on the bare host because the sandbox wasn't ready.
        reason = sandbox.preflight(cfg)
        if reason is not None:
            logger.error("Sandbox preflight failed: %s", reason)
            logger.error(
                "Refusing to start. Fix the container runtime (see deploy/DEPLOY.md), "
                "or set WORKER_SANDBOX=false for unsandboxed local development."
            )
            return
        sandbox.cleanup_stale(cfg)
        if cfg.prometheus_port > 0:
            from airforge_worker.prometheus_exporter import PoolExporter

            self._exporter = PoolExporter(cfg.prometheus_port)

        initial = cfg.pool_size if not cfg.autoscale else max(cfg.min_workers, cfg.pool_size)
        logger.info(
            "Starting pool: %d worker(s)%s → %s",
            initial,
            (
                f" (autoscale {cfg.min_workers}–{cfg.max_workers})"
                if cfg.autoscale
                else ""
            ),
            cfg.backend_url,
        )
        for _ in range(initial):
            self._spawn()

        def handle_stop(signum, frame):  # noqa: ANN001 - signal signature
            if self._stopping:
                logger.warning("Second signal — killing workers")
                for proc in self._procs.values():
                    proc.kill()
                return
            logger.info("Shutting down — workers will finish their current runs")
            self._stopping = True
            for proc in self._procs.values():
                if proc.is_alive():
                    proc.terminate()

        signal.signal(signal.SIGTERM, handle_stop)
        signal.signal(signal.SIGINT, handle_stop)

        last_scale_check = 0.0
        target = initial
        while True:
            self._reap_and_restart()
            if self._exporter is not None:
                alive = sum(1 for proc in self._procs.values() if proc.is_alive())
                self._exporter.observe(alive, 0 if self._stopping else target)

            if self._stopping:
                if not self._procs:
                    break
                time.sleep(0.5)
                continue

            now = time.monotonic()
            if now - last_scale_check >= cfg.autoscale_interval_seconds:
                last_scale_check = now
                desired = self._desired_size()
                target = desired
                current = len(self._procs)
                if desired > current:
                    for _ in range(desired - current):
                        self._spawn()
                elif desired < current:
                    self._retire_one()

            time.sleep(1.0)

        if self._stats_client is not None:
            self._stats_client.close()
        logger.info("All workers stopped. Bye.")
