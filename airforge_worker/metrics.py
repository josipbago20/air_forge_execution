"""Per-run resource accounting for sandboxed runs.

Every sandboxed run is one container in its own cgroup, so the kernel is
already counting its memory, CPU time and block I/O; this module just reads
those counters while the run executes. gVisor places its own runtime (the
Sentry) inside the container's cgroup, so the memory figure is the *whole
sandbox* — the same number the ``--memory`` cap is enforced against, which is
what makes "peak against limit" an honest OOM diagnosis.

Nothing here is required for a run to succeed. Unsandboxed mode has no cgroup,
an unexpected cgroup layout yields no samples, and a container that already
exited simply stops producing them; the summary says which of those happened.

Wire format (what the worker sends the backend; a backend that predates these
keys ignores them):

* ``POST /worker/runs/{id}/logs`` gains an optional ``samples`` list next to
  ``entries``. Each sample is ``{"t": <seconds since the container started>,
  "mem": <bytes>, "cpu": <cumulative CPU microseconds>, "rd": <bytes read>,
  "wr": <bytes written>}``; the last three are cumulative, like the kernel's.
* ``POST /worker/runs/{id}/complete`` gains an optional ``metrics`` object —
  see :meth:`RunMetrics.summary`.
"""

from __future__ import annotations

import glob
import logging
import re
import time
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger("airforge.metrics")

# Overridable for tests (a fake sysfs tree).
CGROUP_ROOT = Path("/sys/fs/cgroup")

# How many ticks to keep looking for the cgroup once the container id is known.
# Docker writes the cidfile at create time and the cgroup appears at start, so
# one or two ticks is normal; dozens means the layout is not what we expect.
_MAX_RESOLVE_ATTEMPTS = 30

_SIZE_RE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*([kmgt]?)b?\s*$", re.IGNORECASE)
_SIZE_MULT = {"": 1, "k": 1024, "m": 1024**2, "g": 1024**3, "t": 1024**4}


def parse_size(spec: str) -> Optional[int]:
    """Docker-style size (``512m``, ``1g``, ``2048k``, ``1.5G``) → bytes."""
    m = _SIZE_RE.match(spec or "")
    if not m:
        return None
    return int(float(m.group(1)) * _SIZE_MULT[m.group(2).lower()])


def fmt_bytes(n: float) -> str:
    """``536870912`` → ``512 MB``; ``222298112`` → ``212.0 MB``."""
    value = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if value < 1024 or unit == "TB":
            text = f"{value:.0f}" if unit == "B" else f"{value:.1f}"
            if text.endswith(".0"):
                text = text[:-2]
            return f"{text} {unit}"
        value /= 1024
    return f"{n} B"  # pragma: no cover


# ── sysfs readers ─────────────────────────────────────────────────────────────
def _read(path: Path) -> Optional[str]:
    try:
        return path.read_text()
    except OSError:
        return None


def _read_int(path: Path) -> Optional[int]:
    raw = _read(path)
    if raw is None:
        return None
    raw = raw.strip()
    if not raw or raw == "max":
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def _read_kv(path: Path) -> dict[str, int]:
    """A flat ``key value`` file such as ``cpu.stat`` or ``memory.events``."""
    out: dict[str, int] = {}
    raw = _read(path)
    if raw is None:
        return out
    for line in raw.splitlines():
        parts = line.split()
        if len(parts) == 2:
            try:
                out[parts[0]] = int(parts[1])
            except ValueError:
                continue
    return out


def _read_io(path: Path) -> tuple[int, int]:
    """Bytes read / written, summed over every device line of ``io.stat``."""
    rd = wr = 0
    raw = _read(path)
    if raw is None:
        return rd, wr
    for line in raw.splitlines():
        for field in line.split()[1:]:
            key, _, value = field.partition("=")
            if not value.isdigit():
                continue
            if key == "rbytes":
                rd += int(value)
            elif key == "wbytes":
                wr += int(value)
    return rd, wr


def find_cgroup(container_id: str, cgroup_parent: str) -> Optional[Path]:
    """The container's cgroup v2 directory, or None.

    Docker on the systemd cgroup driver names it ``docker-<id>.scope`` under
    the parent slice (``airforge-jobs.slice`` in production, ``system.slice``
    by default); on the cgroupfs driver it is ``docker/<id>``. The configured
    parent is tried first, then the defaults, then a shallow search.
    """
    candidates: list[Path] = []
    if cgroup_parent:
        candidates.append(CGROUP_ROOT / cgroup_parent / f"docker-{container_id}.scope")
        candidates.append(CGROUP_ROOT / cgroup_parent / container_id)
    candidates += [
        CGROUP_ROOT / "system.slice" / f"docker-{container_id}.scope",
        CGROUP_ROOT / "docker" / container_id,
    ]
    for c in candidates:
        if (c / "memory.current").is_file():
            return c
    for pattern in (
        f"*/docker-{container_id}.scope",
        f"*/*/docker-{container_id}.scope",
        f"*/{container_id}",
        f"*/*/{container_id}",
    ):
        for hit in glob.glob(str(CGROUP_ROOT / pattern)):
            p = Path(hit)
            if (p / "memory.current").is_file():
                return p
    return None


class RunMetrics:
    """Samples one run's cgroup and keeps its running summary.

    :meth:`sample` is called from the job runner's loop. It is cheap (a handful
    of small sysfs reads), rate-limited to the configured interval, and never
    raises. Samples accumulate until :meth:`take_pending` hands them to the log
    pump, which ships them with the next batch.
    """

    def __init__(
        self,
        *,
        cidfile: Path,
        cgroup_parent: str,
        memory_limit: str,
        cpu_limit: str,
        base_interval: float,
        max_samples: int,
    ) -> None:
        self._cidfile = cidfile
        self._cgroup_parent = cgroup_parent
        self._base_interval = max(0.1, base_interval)
        self._max_samples = max(1, max_samples)
        self._dir: Optional[Path] = None
        self._resolve_attempts = 0
        self._gave_up = False
        self._started: Optional[float] = None
        self._last_sample_at = float("-inf")
        self._pending: list[dict[str, Any]] = []

        self.sample_count = 0
        self.memory_peak_bytes: Optional[int] = None
        self.memory_limit_bytes = parse_size(memory_limit)
        self.cpu_usec: Optional[int] = None
        try:
            self.cpu_limit_cores: Optional[float] = float(cpu_limit) or None
        except (TypeError, ValueError):
            self.cpu_limit_cores = None
        self.oom_kills = 0
        self.io_read_bytes = 0
        self.io_write_bytes = 0

    @property
    def source(self) -> str:
        """``cgroup`` once samples flow; ``unavailable`` when the container
        existed but its cgroup never turned up; ``none`` before either."""
        if self._dir is not None:
            return "cgroup"
        if self._gave_up:
            return "unavailable"
        return "none"

    def start(self) -> None:
        """Mark the container's start: sample times are relative to it."""
        self._started = time.monotonic()

    # ── Discovery ─────────────────────────────────────────────────────────────
    def _resolve(self) -> bool:
        if self._dir is not None:
            return True
        if self._gave_up:
            return False
        cid = (_read(self._cidfile) or "").strip()
        if not cid:
            return False  # container not created yet
        self._resolve_attempts += 1
        found = find_cgroup(cid, self._cgroup_parent)
        if found is None:
            if self._resolve_attempts >= _MAX_RESOLVE_ATTEMPTS:
                self._gave_up = True
                logger.warning(
                    "No cgroup found for container %s under %s after %d attempts "
                    "(parent %r); resource metrics disabled for this run",
                    cid[:12],
                    CGROUP_ROOT,
                    self._resolve_attempts,
                    self._cgroup_parent,
                )
            return False
        self._dir = found
        limit = _read_int(found / "memory.max")
        if limit:
            self.memory_limit_bytes = limit
        logger.debug("Run cgroup: %s", found)
        return True

    # ── Sampling ──────────────────────────────────────────────────────────────
    def sample(self, *, force: bool = False) -> None:
        if self._started is None:
            return
        now = time.monotonic()
        elapsed = now - self._started
        # Past the soft cap the interval stretches, so a long run yields a
        # bounded number of samples rather than one per second forever.
        interval = max(self._base_interval, elapsed / self._max_samples)
        if not force and now - self._last_sample_at < interval:
            return
        try:
            self._sample_now(now, elapsed)
        except Exception as exc:  # noqa: BLE001 - metrics must never hurt a run
            logger.debug("Metrics sample failed: %s", exc)

    def _sample_now(self, now: float, elapsed: float) -> None:
        if not self._resolve():
            return
        d = self._dir
        assert d is not None
        mem = _read_int(d / "memory.current")
        if mem is None:
            return  # the cgroup is gone: the container has exited
        self._last_sample_at = now
        peak = _read_int(d / "memory.peak")  # kernel ≥ 5.19; else we track it
        cpu = _read_kv(d / "cpu.stat").get("usage_usec")
        oom = _read_kv(d / "memory.events").get("oom_kill", 0)
        rd, wr = _read_io(d / "io.stat")

        high = max(mem, peak or 0)
        if self.memory_peak_bytes is None or high > self.memory_peak_bytes:
            self.memory_peak_bytes = high
        if cpu is not None:
            self.cpu_usec = cpu
        self.oom_kills = max(self.oom_kills, oom)
        self.io_read_bytes, self.io_write_bytes = rd, wr
        self.sample_count += 1
        self._pending.append(
            {"t": round(elapsed, 1), "mem": mem, "cpu": cpu, "rd": rd, "wr": wr}
        )

    @property
    def pending_count(self) -> int:
        return len(self._pending)

    def take_pending(self) -> list[dict[str, Any]]:
        out, self._pending = self._pending, []
        return out

    # ── Reporting ─────────────────────────────────────────────────────────────
    def memory_ratio(self) -> Optional[float]:
        if self.memory_peak_bytes is None or not self.memory_limit_bytes:
            return None
        return self.memory_peak_bytes / self.memory_limit_bytes

    def summary(
        self, *, wall_seconds: float, workdir_bytes: Optional[int]
    ) -> dict[str, Any]:
        """The per-run summary sent with ``complete`` (and stored on the run)."""
        measured = self.source == "cgroup"
        return {
            "source": self.source,
            "sample_count": self.sample_count,
            "wall_seconds": round(wall_seconds, 3),
            "memory_peak_bytes": self.memory_peak_bytes,
            "memory_limit_bytes": self.memory_limit_bytes,
            "cpu_seconds": (
                round(self.cpu_usec / 1e6, 3) if self.cpu_usec is not None else None
            ),
            "cpu_limit_cores": self.cpu_limit_cores,
            "oom_kills": self.oom_kills,
            "io_read_bytes": self.io_read_bytes if measured else None,
            "io_write_bytes": self.io_write_bytes if measured else None,
            "workdir_bytes": workdir_bytes,
        }


def describe(summary: dict[str, Any]) -> str:
    """One human line for the run log, e.g.::

        [resources] peak memory 212 MB of 512 MB (41%) · cpu 3.42 s over
        10.1 s (0.34 of 1 core) · io 12 MB read / 3.1 MB written · workdir 0.4 MB
    """
    source = summary.get("source")
    if source == "unavailable":
        return (
            "[resources] unavailable — the run container's cgroup was not found "
            "(see the worker journal)"
        )
    if source != "cgroup":
        return "[resources] no samples — the run ended before the first sample"

    parts: list[str] = []
    peak = summary.get("memory_peak_bytes")
    limit = summary.get("memory_limit_bytes")
    if peak is not None:
        text = f"peak memory {fmt_bytes(peak)}"
        if limit:
            text += f" of {fmt_bytes(limit)} ({peak / limit:.0%})"
        parts.append(text)
    cpu = summary.get("cpu_seconds")
    wall = summary.get("wall_seconds")
    if cpu is not None:
        text = f"cpu {cpu:.2f} s"
        if wall:
            avg = cpu / wall
            cores = summary.get("cpu_limit_cores")
            if cores:
                unit = "core" if cores == 1 else "cores"
                text += f" over {wall:.1f} s ({avg:.2f} of {cores:g} {unit})"
            else:
                text += f" over {wall:.1f} s ({avg:.2f} cores avg)"
        parts.append(text)
    rd, wr = summary.get("io_read_bytes"), summary.get("io_write_bytes")
    if rd is not None and wr is not None:
        parts.append(f"io {fmt_bytes(rd)} read / {fmt_bytes(wr)} written")
    workdir = summary.get("workdir_bytes")
    if workdir is not None:
        parts.append(f"workdir {fmt_bytes(workdir)}")
    if summary.get("oom_kills"):
        parts.append("OOM-killed")
    return "[resources] " + " · ".join(parts)
