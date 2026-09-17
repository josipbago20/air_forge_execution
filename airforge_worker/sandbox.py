"""Sandboxed execution of untrusted pipeline code.

Playground runs are arbitrary Python from the public, so the worker never runs
them in-process. It prepares a run directory (and, when the bundle declares one,
a dependency layer) on the host, then hands both to a throwaway **gVisor**
container (``runsc``) with a hard memory / CPU / PID budget. gVisor gives us a
real syscall boundary; the cgroup caps give us the resource ceiling; the network
is left on so playground code can call public APIs.

Dependency layers are cached by a hash of the bundle's ``requirements.txt`` and
built exactly once — a cross-process file lock serialises builders, so a warm
pool installs a given dependency set a single time and every later run just
mounts it read-only. The build runs *inside the sandbox too*: ``pip`` / ``uv``
execute arbitrary package build code, so it earns the same isolation and caps as
user code, differing only in that it may write the layer it is populating and
reach PyPI.
"""

from __future__ import annotations

import fcntl
import hashlib
import logging
import os
import shutil
import subprocess
import time
from pathlib import Path
from typing import Callable, Optional

from airforge_worker.config import Config

logger = logging.getLogger("airforge.sandbox")

LogSink = Callable[[str], None]

_RUN_NAME_PREFIX = "airforge-run-"

# Container-internal mount points. User code sees an ordinary project at /work
# with its dependencies importable from /deps.
_WORK_MOUNT = "/work"
_DEPS_MOUNT = "/deps"


def container_name(run_id: str) -> str:
    """A stable name per run so timeouts/cancels can target the container."""
    return f"{_RUN_NAME_PREFIX}{run_id}"


def _uv_cache_dir(config: Config) -> Path:
    """Shared wheel cache, sibling of the layer cache. Makes a brand-new layer
    that overlaps existing ones cost nothing to download."""
    return Path(config.deps_cache_dir).parent / "uv-cache"


def _runtime_flag(config: Config) -> list[str]:
    """gVisor when configured; the engine default (runc) when the runtime is
    blank — handy on a dev box that has Docker but not gVisor."""
    return [f"--runtime={config.container_runtime}"] if config.container_runtime else []


def _user_flag() -> list[str]:
    """Run as the worker's own uid/gid. The bind-mounted run directory and cache
    layers are created by the worker, so matching uids keeps everything the
    container writes worker-owned — which cleanup (temp dir) and pruning (cache)
    depend on being able to delete afterwards."""
    return ["--user", f"{os.getuid()}:{os.getgid()}"]


def _cgroup_parent_flag(config: Config) -> list[str]:
    """Place the container under a shared parent cgroup (slice) so the pool has
    a hard aggregate ceiling on top of each run's own caps."""
    if config.container_cgroup_parent:
        return ["--cgroup-parent", config.container_cgroup_parent]
    return []


# ── Dependencies ──────────────────────────────────────────────────────────────
def _requirements(config: Config, workdir: Path) -> Optional[bytes]:
    """The bundle's requirements file, or None when it is absent or has nothing
    but blanks and comments (an empty file should cost nothing)."""
    req = workdir / config.requirements_filename
    if not req.is_file():
        return None
    raw = req.read_bytes()
    meaningful = any(
        line.strip() and not line.strip().startswith("#")
        for line in raw.decode("utf-8", "replace").splitlines()
    )
    return raw if meaningful else None


def resolve_deps(
    config: Config, workdir: Path, log: Optional[LogSink] = None
) -> Optional[Path]:
    """Ensure this bundle's dependency layer exists and return the host dir to
    mount read-only at ``/deps`` — or None when the bundle declares no deps.

    Keyed by the requirements *contents*, so every pipeline (and version) with
    the same dependencies shares one layer and pays the install once.
    """
    raw = _requirements(config, workdir)
    if raw is None:
        return None

    key = hashlib.sha256(raw).hexdigest()[:16]
    cache = Path(config.deps_cache_dir)
    cache.mkdir(parents=True, exist_ok=True)
    layer = cache / key
    site = layer / "site"
    ready = layer / ".ready"

    if ready.is_file():
        _touch(ready)
        return site

    # Serialise builders of this same layer across every worker process.
    with _FileLock(cache / f"{key}.lock"):
        if ready.is_file():  # someone built it while we waited on the lock
            _touch(ready)
            return site
        _build_layer(config, layer, workdir, log)
        ready.write_text(key)

    _prune_cache(config)
    return site


def _build_layer(
    config: Config, layer: Path, workdir: Path, log: Optional[LogSink]
) -> None:
    def emit(msg: str) -> None:
        logger.info(msg)
        if log is not None:
            log(msg)

    # A half-built layer from an earlier crash must never be trusted.
    if layer.exists():
        shutil.rmtree(layer, ignore_errors=True)
    site = layer / "site"
    site.mkdir(parents=True, exist_ok=True)
    uv_cache = _uv_cache_dir(config)
    uv_cache.mkdir(parents=True, exist_ok=True)

    emit(f"[env] installing dependencies from {config.requirements_filename} …")
    argv = [
        config.container_cmd,
        "run",
        "--rm",
        *_runtime_flag(config),
        *_cgroup_parent_flag(config),
        "--network",
        config.container_network,  # PyPI must be reachable
        "--memory",
        config.job_memory,
        "--memory-swap",
        config.job_memory,
        "--cpus",
        config.job_cpus,
        "--pids-limit",
        str(config.job_pids_limit),
        # Same uid as the worker so the layer it writes stays worker-owned —
        # cache pruning has to be able to delete these files later.
        *_user_flag(),
        "--tmpfs",
        "/tmp:rw,size=256m,mode=1777",
        "-e",
        "HOME=/tmp",
        "-v",
        f"{site}:/out:rw",
        "-v",
        f"{workdir}:/src:ro",
        "-v",
        f"{uv_cache}:/uv-cache:rw",
        "-e",
        "UV_CACHE_DIR=/uv-cache",
        "-w",
        "/src",
        config.job_image,
        "uv",
        "pip",
        "install",
        "--target",
        "/out",
        "-r",
        f"/src/{config.requirements_filename}",
    ]
    try:
        proc = subprocess.run(
            argv,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=config.dep_install_timeout_seconds,
        )
    except subprocess.TimeoutExpired:
        shutil.rmtree(layer, ignore_errors=True)
        raise RuntimeError(
            f"exceeded {int(config.dep_install_timeout_seconds)}s"
        ) from None
    for line in (proc.stdout or "").splitlines():
        emit(f"[env] {line}")
    if proc.returncode != 0:
        shutil.rmtree(layer, ignore_errors=True)
        raise RuntimeError(f"installer exited {proc.returncode}")
    emit("[env] dependencies ready")


# ── Running a job ─────────────────────────────────────────────────────────────
def run_argv(
    config: Config,
    *,
    name: str,
    workdir: Path,
    deps_site: Optional[Path],
    inside_cmd: list[str],
    job_env: dict[str, str],
    cidfile: Optional[Path] = None,
) -> list[str]:
    """The ``docker run`` command line for one sandboxed run: gVisor runtime,
    hard resource caps, a read-only root with only /work writable, the code and
    (optional) deps bind-mounted, and an explicit env — the host's own
    environment (and its worker token) never enters the container.

    ``cidfile`` makes Docker write the container's full id there at create
    time; the metrics sampler uses it to find the run's cgroup without a
    ``docker inspect`` round-trip."""
    argv = [
        config.container_cmd,
        "run",
        "--rm",
        "--name",
        name,
        *_runtime_flag(config),
        *_cgroup_parent_flag(config),
        "--network",
        config.container_network,
        "--memory",
        config.job_memory,
        "--memory-swap",
        config.job_memory,  # == memory ⇒ no swap headroom
        "--cpus",
        config.job_cpus,
        "--pids-limit",
        str(config.job_pids_limit),
        *_user_flag(),
        "--cap-drop",
        "ALL",
        "--security-opt",
        "no-new-privileges",
        "--read-only",  # immutable rootfs; only the mounts below are writable
        "--tmpfs",
        "/tmp:rw,size=256m,mode=1777",
        "-e",
        "HOME=/tmp",
        "-v",
        f"{workdir}:{_WORK_MOUNT}:rw",
        "-w",
        _WORK_MOUNT,
    ]
    if cidfile is not None:
        argv += ["--cidfile", str(cidfile)]
    if deps_site is not None:
        argv += ["-v", f"{deps_site}:{_DEPS_MOUNT}:ro"]
    for key, value in job_env.items():
        argv += ["-e", f"{key}={value}"]
    argv += [config.job_image, *inside_cmd]
    return argv


def stop_container(config: Config, name: str, grace: float) -> None:
    """Best-effort stop of a run's container: SIGTERM, then SIGKILL after the
    grace window. Used for both timeouts and user cancellations."""
    try:
        subprocess.run(
            [config.container_cmd, "stop", "-t", str(int(grace)), name],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=grace + 10,
        )
    except Exception as exc:  # noqa: BLE001 - best effort; the reaper is the backstop
        logger.warning("Could not stop container %s: %s", name, exc)


def cleanup_stale(config: Config) -> None:
    """Remove leftover run containers from a previous worker generation.

    Called at supervisor startup, before any worker exists — so anything whose
    name matches the run prefix is an orphan from a crash or SIGKILL (the CLI
    died; the container kept running). Left alone it would burn slice budget
    and collide with a future ``--name`` for the same run id."""
    if not config.sandbox:
        return
    try:
        out = subprocess.run(
            [config.container_cmd, "ps", "-aq", "--filter", f"name={_RUN_NAME_PREFIX}"],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=30,
        )
        ids = out.stdout.split()
        if ids:
            subprocess.run(
                [config.container_cmd, "rm", "-f", *ids],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=120,
            )
            logger.info("Removed %d stale run container(s)", len(ids))
    except Exception as exc:  # noqa: BLE001 - best effort
        logger.warning("Stale container cleanup failed: %s", exc)


def preflight(config: Config) -> Optional[str]:
    """Return a human-readable reason the sandbox can't run, or None if it can.
    Called once at worker startup so a missing engine/runtime fails loudly
    instead of turning every run into a cryptic failure."""
    if not config.sandbox:
        return None
    if shutil.which(config.container_cmd) is None:
        return f"container command {config.container_cmd!r} not found on PATH"
    probe = [config.container_cmd, "run", "--rm", *_runtime_flag(config), config.job_image, "true"]
    try:
        proc = subprocess.run(
            probe, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=120
        )
    except Exception as exc:  # noqa: BLE001
        return f"sandbox probe failed to launch: {exc}"
    if proc.returncode != 0:
        return f"sandbox probe failed (exit {proc.returncode}): {(proc.stdout or '').strip()[:400]}"
    return None


# ── Cache housekeeping ────────────────────────────────────────────────────────
def _touch(path: Path) -> None:
    try:
        os.utime(path, None)
    except OSError:
        pass


def _prune_cache(config: Config) -> None:
    """Keep the layer cache under its soft cap by evicting least-recently-used
    layers. Touch-on-use (above) keeps active layers at the young end, so an
    in-flight run's layer is not the one chosen for eviction."""
    cap_gb = config.deps_cache_max_gb
    if cap_gb <= 0:
        return
    cache = Path(config.deps_cache_dir)
    if not cache.exists():
        return

    layers: list[tuple[float, int, Path]] = []
    total = 0
    for layer in cache.iterdir():
        if not layer.is_dir():
            continue
        try:
            size = sum(f.stat().st_size for f in layer.rglob("*") if f.is_file())
            marker = layer / ".ready"
            mtime = marker.stat().st_mtime if marker.exists() else layer.stat().st_mtime
        except OSError:
            continue
        layers.append((mtime, size, layer))
        total += size

    cap_bytes = int(cap_gb * 1024**3)
    if total <= cap_bytes:
        return
    now = time.time()
    for mtime, size, layer in sorted(layers):  # oldest first
        if total <= cap_bytes:
            break
        # Touched within the last hour ⇒ plausibly mounted by a run in flight
        # right now (runs touch their layer at claim time). Never yank those.
        if now - mtime < 3600:
            continue
        shutil.rmtree(layer, ignore_errors=True)
        total -= size
        logger.info("Pruned dependency layer %s (%.1f MB)", layer.name, size / 1e6)


# ── File lock ─────────────────────────────────────────────────────────────────
class _FileLock:
    """A blocking, cross-process advisory lock (``flock``). Held only for the
    duration of a layer build, so waiters are workers that need the very same
    layer and would otherwise build it in parallel."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._fd: int | None = None

    def __enter__(self) -> "_FileLock":
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._fd = os.open(self._path, os.O_CREAT | os.O_RDWR, 0o644)
        fcntl.flock(self._fd, fcntl.LOCK_EX)
        return self

    def __exit__(self, *exc: object) -> None:
        if self._fd is not None:
            fcntl.flock(self._fd, fcntl.LOCK_UN)
            os.close(self._fd)
            self._fd = None
