"""Gate: the worker's pool metrics (WORKER_PROMETHEUS_PORT) change nothing by default.

    .venv/bin/python scripts/verify_prometheus.py

1. Defaults (the cloud's workers): the port is 0, and loading the config and
   the supervisor imports neither the exporter nor prometheus_client.
2. With a port: the exporter serves the pool gauges and the crash counter.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PORT = int(os.environ.get("VERIFY_METRICS_PORT", "59103"))

DEFAULTS = """
import sys
from airforge_worker.config import load_config
from airforge_worker import supervisor
cfg = load_config()
print("PORT", cfg.prometheus_port)
print("EXPORTER", "airforge_worker.prometheus_exporter" in sys.modules)
print("PROMETHEUS", "prometheus_client" in sys.modules)
"""

ON = f"""
import urllib.request
from airforge_worker.config import load_config
from airforge_worker.prometheus_exporter import PoolExporter
cfg = load_config()
print("PORT", cfg.prometheus_port)
exporter = PoolExporter(cfg.prometheus_port)
exporter.observe(3, 4)
exporter.crashed()
body = urllib.request.urlopen("http://127.0.0.1:{PORT}/metrics", timeout=5).read().decode()
print("PROCESSES", "airforge_worker_processes 3.0" in body)
print("TARGET", "airforge_worker_pool_target 4.0" in body)
print("CRASHES", "airforge_worker_process_crashes_total 1.0" in body)
print("BUILD", 'airforge_worker_build_info{{version="' in body)
"""


def run(code: str, env: dict[str, str]) -> dict[str, str]:
    out = subprocess.run([sys.executable, "-c", code], cwd=ROOT, env=env, capture_output=True, text=True, timeout=60)
    values = dict(line.split(" ", 1) for line in out.stdout.splitlines() if " " in line)
    if out.returncode != 0:
        values["ERROR"] = out.stderr[-1000:]
    return values


def main() -> int:
    results = []

    def check(name: str, ok: bool, detail: object = "") -> None:
        results.append(ok)
        print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + ("" if ok else f" — {detail}"))

    base = {k: v for k, v in os.environ.items() if k != "WORKER_PROMETHEUS_PORT"}
    print("1. Defaults")
    d = run(DEFAULTS, base)
    check("config and supervisor load", "ERROR" not in d, d.get("ERROR"))
    check("port is 0", d.get("PORT") == "0", d)
    check("exporter not imported", d.get("EXPORTER") == "False", d)
    check("prometheus_client not imported", d.get("PROMETHEUS") == "False", d)
    print("2. With a port")
    on = run(ON, {**base, "WORKER_PROMETHEUS_PORT": str(PORT)})
    check("exporter runs", "ERROR" not in on, on.get("ERROR"))
    check("port read from WORKER_PROMETHEUS_PORT", on.get("PORT") == str(PORT), on)
    for key in ("PROCESSES", "TARGET", "CRASHES", "BUILD"):
        check(f"{key.lower()} exported", on.get(key) == "True", on)
    print(f"\n{sum(results)}/{len(results)} checks passed")
    return 0 if all(results) else 1


if __name__ == "__main__":
    sys.exit(main())
