"""Executes one claimed run.

The claimed bundle arrives with the pipeline's whole folder inlined (base64 per
file). It is materialised into a fresh temp directory, so multi-file pipelines
work exactly like a local project: ``main.py`` can ``import helpers`` from the
file next to it.

Two execution shapes, chosen by the run's *trigger*:

* ``manual`` / ``schedule`` — run the entrypoint as a script
  (``python -u main.py``). If the run carries a payload it is written next to
  the code as ``payload.json`` and pointed at via ``AIRFORGE_PAYLOAD_PATH``.
* ``api`` — a tiny bootstrap imports the entrypoint, calls
  ``handler(payload)``, and writes the return value to a result file, which is
  reported back as the invocation's response.

Output is streamed: reader threads capture stdout/stderr line by line, and the
main thread ships batches to the backend on a short interval. The log-append
response doubles as the control channel — it says whether the user asked the
run to stop — and a run that prints nothing is still covered by a periodic
control poll. Timeouts are enforced here with SIGTERM, then SIGKILL.
"""

from __future__ import annotations

import base64
import json
import logging
import os
import queue
import subprocess
import tempfile
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from airforge_worker.backend import BackendClient
from airforge_worker.config import Config

logger = logging.getLogger("airforge.jobrunner")

_MAX_LINE_CHARS = 10_000
_MAX_RESULT_BYTES = 512_000

# Bootstrap for API invocations. Kept dependency-free and written into the run
# directory so user code sees a perfectly ordinary import of their entrypoint.
_API_RUNNER = """\
import importlib.util
import json
import sys

entry, payload_path, result_path = sys.argv[1], sys.argv[2], sys.argv[3]

spec = importlib.util.spec_from_file_location("__airforge_entry__", entry)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

handler = getattr(module, "handler", None)
if handler is None:
    print(
        f"AirForge: {entry} defines no handler(payload) function, "
        "which API pipelines require.",
        file=sys.stderr,
    )
    sys.exit(3)

with open(payload_path) as f:
    payload = json.load(f)

result = handler(payload)

with open(result_path, "w") as f:
    json.dump(result, f, default=str)
"""


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_rel_path(path: str) -> Path:
    """Defence in depth against a hostile bundle: no absolute paths, no
    escaping components. (The backend sanitises writes already.)"""
    p = Path(path)
    if p.is_absolute() or any(part in ("..", "") for part in p.parts):
        raise ValueError(f"unsafe path in bundle: {path!r}")
    return p


class _LogPump:
    """Collects subprocess output and ships it to the backend in batches."""

    def __init__(self, client: BackendClient, config: Config, run_id: str) -> None:
        self._client = client
        self._config = config
        self._run_id = run_id
        self._queue: queue.Queue[tuple[str, str]] = queue.Queue()
        self.cancel_requested = False
        self._last_contact = 0.0

    def attach(self, proc: subprocess.Popen) -> list[threading.Thread]:
        threads = [
            threading.Thread(
                target=self._read, args=(proc.stdout, "stdout"), daemon=True
            ),
            threading.Thread(
                target=self._read, args=(proc.stderr, "stderr"), daemon=True
            ),
        ]
        for t in threads:
            t.start()
        return threads

    def _read(self, pipe, stream: str) -> None:
        try:
            for raw in iter(pipe.readline, ""):
                line = raw.rstrip("\n")
                self._queue.put((stream, line[:_MAX_LINE_CHARS]))
        finally:
            pipe.close()

    def system_line(self, line: str) -> None:
        self._queue.put(("system", line))

    def _drain(self) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        while len(out) < self._config.log_batch_max:
            try:
                stream, line = self._queue.get_nowait()
            except queue.Empty:
                break
            out.append({"stream": stream, "line": line, "ts": _utcnow_iso()})
        return out

    def pump_once(self, *, force_control: bool = False) -> None:
        """Ship one batch if there is output; otherwise poll control if it has
        been quiet long enough. Network hiccups are logged and survived — the
        run must not die because a log batch didn't land."""
        batch = self._drain()
        now = time.monotonic()
        try:
            if batch:
                control = self._client.append_logs(self._run_id, batch)
                self._last_contact = now
            elif force_control or now - self._last_contact >= self._config.control_poll_seconds:
                control = self._client.run_control(self._run_id)
                self._last_contact = now
            else:
                return
            if control.get("cancel_requested"):
                self.cancel_requested = True
        except Exception as exc:  # noqa: BLE001 - keep the run alive
            logger.warning("Log/control call failed for run %s: %s", self._run_id, exc)

    def flush_all(self) -> None:
        """Final drain after the process exited: everything must go, batch by
        batch, with a couple of retries per batch."""
        while True:
            batch = self._drain()
            if not batch:
                break
            for attempt in range(3):
                try:
                    self._client.append_logs(self._run_id, batch)
                    break
                except Exception:  # noqa: BLE001
                    if attempt == 2:
                        logger.error("Dropped %d log line(s) for run %s", len(batch), self._run_id)
                    else:
                        time.sleep(1.5 * (attempt + 1))


def _terminate(proc: subprocess.Popen, grace: float) -> None:
    if proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=grace)
    except subprocess.TimeoutExpired:
        proc.kill()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:  # pragma: no cover - kernel refused SIGKILL
            logger.error("Process %s ignored SIGKILL", proc.pid)


def _job_env(
    config: Config, run: dict[str, Any], payload_path: Path | None
) -> dict[str, str]:
    """The subprocess environment: the worker's own env minus its secrets, plus
    run metadata pipeline code may want."""
    env = dict(os.environ)
    env.pop("WORKER_API_TOKEN", None)
    env.update(
        {
            "PYTHONUNBUFFERED": "1",
            "AIRFORGE_RUN_ID": str(run["run_id"]),
            "AIRFORGE_RUN_NUMBER": str(run["run_number"]),
            "AIRFORGE_PIPELINE_ID": str(run["pipeline_id"]),
            "AIRFORGE_PIPELINE_NAME": str(run["pipeline_name"]),
            "AIRFORGE_TRIGGER": str(run["trigger"]),
        }
    )
    if payload_path is not None:
        env["AIRFORGE_PAYLOAD_PATH"] = str(payload_path)
    # Data source access without baked-in credentials: the run-scoped token the
    # backend minted into the claim, and where its /runtime API lives. The
    # token authenticates only this run and expires with it — unlike the
    # worker's own token, it is safe to hand to user code.
    if run.get("runtime_token"):
        env["AIRFORGE_RUN_TOKEN"] = str(run["runtime_token"])
        env["AIRFORGE_API_URL"] = config.backend_url
    return env


def execute_run(client: BackendClient, config: Config, run: dict[str, Any]) -> None:
    """Materialise, execute, stream, and complete one claimed run."""
    run_id = str(run["run_id"])
    logger.info(
        "Run %s (#%s of %s, trigger=%s) starting",
        run_id,
        run["run_number"],
        run["pipeline_name"],
        run["trigger"],
    )

    status = "failed"
    exit_code: int | None = None
    error: str | None = None
    result: Any = None

    try:
        with tempfile.TemporaryDirectory(prefix="airforge-run-") as tmp:
            workdir = Path(tmp)

            # ── Materialise the bundle ────────────────────────────────────────
            for f in run["files"]:
                rel = _safe_rel_path(f["path"])
                target = workdir / rel
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(base64.b64decode(f["content_b64"]))

            entrypoint = workdir / _safe_rel_path(run["entrypoint"])
            if not entrypoint.is_file():
                raise FileNotFoundError(
                    f"entrypoint {run['entrypoint']!r} is not in the pipeline folder"
                )

            payload_path = workdir / "payload.json"
            payload_path.write_text(json.dumps(run.get("payload")))

            # ── Build the command ─────────────────────────────────────────────
            if run["trigger"] == "api":
                runner = workdir / "_airforge_api_runner.py"
                runner.write_text(_API_RUNNER)
                result_path = workdir / "_airforge_result.json"
                cmd = [
                    config.job_python,
                    "-u",
                    str(runner),
                    str(entrypoint),
                    str(payload_path),
                    str(result_path),
                ]
            else:
                result_path = None
                cmd = [config.job_python, "-u", str(entrypoint)]

            # ── Run it ────────────────────────────────────────────────────────
            pump = _LogPump(client, config, run_id)
            started = time.monotonic()
            timeout = float(run["timeout_seconds"])

            proc = subprocess.Popen(
                cmd,
                cwd=workdir,
                env=_job_env(config, run, payload_path),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
            )
            readers = pump.attach(proc)

            timed_out = False
            while proc.poll() is None:
                pump.pump_once()
                if pump.cancel_requested:
                    pump.system_line("[cancel requested — stopping the run]")
                    _terminate(proc, config.kill_grace_seconds)
                    break
                if time.monotonic() - started > timeout:
                    timed_out = True
                    pump.system_line(
                        f"[timed out after {int(timeout)}s — stopping the run]"
                    )
                    _terminate(proc, config.kill_grace_seconds)
                    break
                time.sleep(config.log_flush_interval_seconds)

            exit_code = proc.wait()
            for t in readers:
                t.join(timeout=5)
            pump.flush_all()

            # ── Decide the outcome ────────────────────────────────────────────
            if pump.cancel_requested:
                status, error = "cancelled", "Cancelled while running"
            elif timed_out:
                status, error = "failed", f"Timed out after {int(timeout)} seconds"
            elif exit_code == 0:
                status = "succeeded"
                if result_path is not None:
                    try:
                        raw = result_path.read_bytes()
                        if len(raw) > _MAX_RESULT_BYTES:
                            status, error = "failed", (
                                f"Result too large ({len(raw)} bytes > {_MAX_RESULT_BYTES})"
                            )
                        else:
                            result = json.loads(raw)
                    except FileNotFoundError:
                        status, error = "failed", (
                            "handler() finished but produced no result file"
                        )
                    except json.JSONDecodeError as exc:
                        status, error = "failed", f"handler() result is not valid JSON: {exc}"
            else:
                status, error = "failed", f"Exited with code {exit_code}"

    except Exception as exc:  # noqa: BLE001 - report, never crash the worker
        logger.exception("Run %s blew up in the worker", run_id)
        status, error = "failed", f"Worker error: {exc}"

    # ── Report, insistently ───────────────────────────────────────────────────
    # Losing the completion would leave the run RUNNING until the reaper calls
    # it lost; a few retries make that a rare event rather than a common one.
    for attempt in range(5):
        try:
            client.complete(
                run_id, status=status, exit_code=exit_code, error=error, result=result
            )
            break
        except Exception as exc:  # noqa: BLE001
            if attempt == 4:
                logger.error("Could not report completion of run %s: %s", run_id, exc)
            else:
                time.sleep(2.0 * (attempt + 1))

    logger.info("Run %s finished: %s", run_id, status)
