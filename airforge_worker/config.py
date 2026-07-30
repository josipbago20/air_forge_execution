"""Worker configuration, read once from the environment (and a local .env).

Everything tunable lives here so the worker and supervisor never touch
``os.environ`` directly.
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass, field
from pathlib import Path


def _load_dotenv() -> None:
    """Best-effort .env loading; the worker must also run with plain env vars."""
    try:
        from dotenv import load_dotenv

        load_dotenv(Path(__file__).resolve().parent.parent / ".env")
    except ImportError:  # pragma: no cover - dotenv is in requirements
        pass


def _bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    return int(raw) if raw else default


def _float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    return float(raw) if raw else default


@dataclass(frozen=True)
class Config:
    # Where the AirForge backend lives; the worker talks to its /worker API.
    backend_url: str = "http://127.0.0.1:8001"
    # Shared secret; must match the backend's WORKER_API_TOKEN.
    worker_token: str = "dev-worker-token-change-me-4b8a17c2"

    # ── Pool sizing ───────────────────────────────────────────────────────────
    # Fixed pool size when autoscale is off; the floor when it is on.
    pool_size: int = 2
    autoscale: bool = False
    min_workers: int = 1
    max_workers: int = 8
    # How often the supervisor samples queue depth for scaling decisions.
    autoscale_interval_seconds: float = 5.0
    # How long the queue must stay empty before one worker is retired.
    scale_down_idle_seconds: float = 90.0

    # ── Execution ─────────────────────────────────────────────────────────────
    # Interpreter that runs pipeline code. Defaults to this worker's own.
    job_python: str = sys.executable
    # How often buffered output is shipped to the backend while a run executes.
    log_flush_interval_seconds: float = 0.7
    log_batch_max: int = 200
    # How often a silent run checks whether it was cancelled.
    control_poll_seconds: float = 2.0
    # Grace between SIGTERM and SIGKILL when stopping a run.
    kill_grace_seconds: float = 5.0

    env: dict[str, str] = field(default_factory=dict, repr=False)


def load_config() -> Config:
    _load_dotenv()
    return Config(
        backend_url=os.environ.get("BACKEND_URL", Config.backend_url).rstrip("/"),
        worker_token=os.environ.get("WORKER_API_TOKEN", Config.worker_token),
        pool_size=_int("WORKER_POOL_SIZE", Config.pool_size),
        autoscale=_bool("WORKER_AUTOSCALE", Config.autoscale),
        min_workers=_int("WORKER_MIN_WORKERS", Config.min_workers),
        max_workers=_int("WORKER_MAX_WORKERS", Config.max_workers),
        autoscale_interval_seconds=_float(
            "WORKER_AUTOSCALE_INTERVAL_SECONDS", Config.autoscale_interval_seconds
        ),
        scale_down_idle_seconds=_float(
            "WORKER_SCALE_DOWN_IDLE_SECONDS", Config.scale_down_idle_seconds
        ),
        job_python=os.environ.get("WORKER_JOB_PYTHON", Config.job_python),
        log_flush_interval_seconds=_float(
            "WORKER_LOG_FLUSH_INTERVAL_SECONDS", Config.log_flush_interval_seconds
        ),
        log_batch_max=_int("WORKER_LOG_BATCH_MAX", Config.log_batch_max),
        control_poll_seconds=_float("WORKER_CONTROL_POLL_SECONDS", Config.control_poll_seconds),
        kill_grace_seconds=_float("WORKER_KILL_GRACE_SECONDS", Config.kill_grace_seconds),
    )
