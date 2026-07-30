"""One warm worker process.

Lifecycle: register → (heartbeat thread) → loop(claim → execute) → deregister.

The claim is a long-poll, so an idle worker costs one cheap HTTP request every
~20 seconds yet picks new work up in well under a second. Signals stop the
worker *gracefully*: a first SIGTERM/SIGINT lets the current run finish and
then exits; a second one abandons ship (the backend's reaper will fail the run
as worker-lost).
"""

from __future__ import annotations

import logging
import signal
import socket
import sys
import threading
import time

from airforge_worker.backend import BackendClient
from airforge_worker.config import Config
from airforge_worker.jobrunner import execute_run

logger = logging.getLogger("airforge.worker")


class Worker:
    def __init__(self, config: Config, slot: int) -> None:
        self._config = config
        self._slot = slot
        self._client = BackendClient(config)
        self._stop = threading.Event()
        self._busy = threading.Event()
        self._worker_id: str | None = None
        self._heartbeat_interval = 10.0
        self._poll_wait = 20.0
        self.name = f"{socket.gethostname()}-{slot}-{int(time.time()) % 100000}"

    # ── Signals ───────────────────────────────────────────────────────────────
    def install_signal_handlers(self) -> None:
        def handle(signum, frame):  # noqa: ANN001 - signal signature
            if self._stop.is_set():
                logger.warning("Second signal — exiting immediately")
                sys.exit(1)
            logger.info(
                "Stop requested%s",
                " — finishing the current run first" if self._busy.is_set() else "",
            )
            self._stop.set()

        signal.signal(signal.SIGTERM, handle)
        signal.signal(signal.SIGINT, handle)

    # ── Registration and liveness ─────────────────────────────────────────────
    def _register(self) -> bool:
        """Keep trying until the backend is reachable or we're told to stop."""
        meta = {
            "host": socket.gethostname(),
            "pid": None,
            "slot": self._slot,
            "python": sys.version.split()[0],
        }
        import os

        meta["pid"] = os.getpid()

        delay = 1.0
        while not self._stop.is_set():
            try:
                data = self._client.register(self.name, meta)
                self._worker_id = str(data["worker_id"])
                self._heartbeat_interval = float(data["heartbeat_interval_seconds"])
                self._poll_wait = float(data["poll_wait_seconds"])
                logger.info("Registered as %s (%s)", self.name, self._worker_id)
                return True
            except Exception as exc:  # noqa: BLE001
                logger.warning("Register failed (%s); retrying in %.0fs", exc, delay)
                if self._stop.wait(delay):
                    break
                delay = min(delay * 2, 30.0)
        return False

    def _heartbeat_loop(self) -> None:
        while not self._stop.wait(self._heartbeat_interval):
            if self._worker_id is None:
                continue
            try:
                self._client.heartbeat(self._worker_id)
            except Exception as exc:  # noqa: BLE001
                # A 404 means the backend pruned us (long netsplit): reregister.
                logger.warning("Heartbeat failed: %s", exc)
                try:
                    import httpx

                    if isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code == 404:
                        self._register()
                except Exception:  # noqa: BLE001 - pure best-effort
                    pass

    # ── Main loop ─────────────────────────────────────────────────────────────
    def run_forever(self) -> None:
        if not self._register():
            return
        heartbeat = threading.Thread(target=self._heartbeat_loop, daemon=True)
        heartbeat.start()

        logger.info("Warm and waiting for work")
        error_delay = 1.0
        while not self._stop.is_set():
            try:
                claimed = self._client.claim(self._worker_id, self._poll_wait)
                error_delay = 1.0
            except Exception as exc:  # noqa: BLE001
                logger.warning("Claim failed (%s); backing off %.0fs", exc, error_delay)
                if self._stop.wait(error_delay):
                    break
                error_delay = min(error_delay * 2, 30.0)
                continue

            if claimed is None:
                continue

            self._busy.set()
            try:
                execute_run(self._client, self._config, claimed)
            finally:
                self._busy.clear()

        try:
            if self._worker_id is not None:
                self._client.deregister(self._worker_id)
                logger.info("Deregistered cleanly")
        except Exception:  # noqa: BLE001 - the reaper covers a rude exit
            pass
        self._client.close()


def worker_process_main(slot: int) -> None:
    """Entry point of one pool member (runs in its own OS process)."""
    logging.basicConfig(
        level=logging.INFO,
        format=f"%(asctime)s %(levelname)-8s [w{slot}] %(name)s: %(message)s",
    )
    from airforge_worker.config import load_config

    worker = Worker(load_config(), slot)
    worker.install_signal_handlers()
    worker.run_forever()
