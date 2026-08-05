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
    # Interpreter that runs pipeline code in *unsandboxed* mode (local dev).
    # In sandboxed mode the interpreter is the job image's own Python.
    job_python: str = sys.executable
    # How often buffered output is shipped to the backend while a run executes.
    log_flush_interval_seconds: float = 0.7
    log_batch_max: int = 200
    # How often a silent run checks whether it was cancelled.
    control_poll_seconds: float = 2.0
    # Grace between SIGTERM and SIGKILL when stopping a run.
    kill_grace_seconds: float = 5.0

    # ── Sandboxing (untrusted user code) ───────────────────────────────────────
    # Runs execute inside a gVisor-isolated container by default: fail closed so
    # a misconfigured deploy never runs public code on the bare host. Local dev
    # sets WORKER_SANDBOX=false to run directly with ``job_python``.
    sandbox: bool = True
    # The container CLI and the sandbox runtime. ``runsc`` is gVisor; set the
    # runtime to "" to use the container engine's default (e.g. for a dev box
    # with Docker but no gVisor).
    container_cmd: str = "docker"
    container_runtime: str = "runsc"
    # Image that runs user code (and builds dependency layers). Must ship the
    # same Python the dependency layers are built against — see deploy/.
    job_image: str = "airforge/job-base:latest"
    # Per-run resource ceilings, enforced by the container cgroup. RAM is a hard
    # cap (the run is OOM-killed inside its cgroup, never the host); CPUs is a
    # fraction/multiple of a core; pids guards against fork bombs.
    job_memory: str = "1g"
    job_cpus: str = "1"
    job_pids_limit: int = 256
    # Container network for runs. "bridge" gives user code full internet egress
    # (playground pipelines call public APIs); "none" would cut it off.
    container_network: str = "bridge"
    # Optional parent cgroup (a systemd slice, e.g. "airforge-jobs.slice") that
    # every job and build container is placed under. The per-run caps above bound
    # a single run; this slice's own MemoryMax/CPUQuota bound the *sum*, so the
    # pool can never exceed the box. Empty ⇒ the engine's default parent.
    container_cgroup_parent: str = ""

    # ── Per-pipeline dependencies ───────────────────────────────────────────────
    # A bundle carrying this file gets its packages installed into a cached layer
    # (one per unique requirements hash), mounted read-only into the run.
    requirements_filename: str = "requirements.txt"
    # Where built dependency layers live on the host (bind-mounted into runs).
    deps_cache_dir: str = "/var/lib/airforge/deps"
    # Ceiling on a single dependency build (pip/uv install).
    dep_install_timeout_seconds: float = 600.0
    # Soft cap on the layer cache; the least-recently-used layers are pruned back
    # under this after each build. 0 disables pruning.
    deps_cache_max_gb: float = 20.0

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
        sandbox=_bool("WORKER_SANDBOX", Config.sandbox),
        container_cmd=os.environ.get("WORKER_CONTAINER_CMD", Config.container_cmd),
        container_runtime=os.environ.get(
            "WORKER_CONTAINER_RUNTIME", Config.container_runtime
        ),
        job_image=os.environ.get("WORKER_JOB_IMAGE", Config.job_image),
        job_memory=os.environ.get("WORKER_JOB_MEMORY", Config.job_memory),
        job_cpus=os.environ.get("WORKER_JOB_CPUS", Config.job_cpus),
        job_pids_limit=_int("WORKER_JOB_PIDS_LIMIT", Config.job_pids_limit),
        container_network=os.environ.get(
            "WORKER_CONTAINER_NETWORK", Config.container_network
        ),
        container_cgroup_parent=os.environ.get(
            "WORKER_CONTAINER_CGROUP_PARENT", Config.container_cgroup_parent
        ),
        requirements_filename=os.environ.get(
            "WORKER_REQUIREMENTS_FILENAME", Config.requirements_filename
        ),
        deps_cache_dir=os.environ.get("WORKER_DEPS_CACHE_DIR", Config.deps_cache_dir),
        dep_install_timeout_seconds=_float(
            "WORKER_DEP_INSTALL_TIMEOUT_SECONDS", Config.dep_install_timeout_seconds
        ),
        deps_cache_max_gb=_float("WORKER_DEPS_CACHE_MAX_GB", Config.deps_cache_max_gb),
    )
