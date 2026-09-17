# AirForge Execution

The warm worker pool that runs AirForge pipelines. Separate from the backend on
purpose: it gets its own resources, and a heavy pipeline can never starve the
API.

## How it works

```
┌────────────┐   register / heartbeat    ┌──────────────────┐
│ supervisor │──spawns N procs──┐        │ AirForge backend │
└────────────┘                  ▼        │  (Postgres queue)│
                          ┌─────────┐    │                  │
                          │ worker  │───▶│ POST /worker/claim  (long-poll,
                          │ (proc)  │    │   FOR UPDATE SKIP LOCKED)
                          └─────────┘    │                  │
                               │ run code in subprocess     │
                               │ + temp dir (R2 bundle)     │
                               ├──────logs (batches)───────▶│
                               └──────complete─────────────▶│
```

- **Queue semantics.** The queue is the backend's `pipeline_runs` table. A
  worker claims the oldest `queued` run atomically (`FOR UPDATE SKIP LOCKED`),
  so any number of workers run in parallel with exactly-once delivery. The
  claim endpoint long-polls (~20s), so dispatch latency is sub-second while
  idle workers cost one request per poll window.
- **Code delivery.** The claim response carries the pipeline's entire folder
  (from Cloudflare R2, via the backend) inlined base64 — workers never hold
  storage credentials. Files are materialised into a fresh temp directory, so
  multi-file pipelines import each other naturally.
- **Execution.** Manual/scheduled runs: `python -u <entrypoint>`. API
  invocations: a bootstrap imports the entrypoint and calls
  `handler(payload)`, and its return value goes back as the invocation result.
- **Sandboxing.** Because playground pipelines are untrusted public code, each
  run executes inside a **gVisor** container with a hard RAM/CPU/PID budget and
  full internet egress; a `requirements.txt` in the bundle is installed once
  into a cached, read-only dependency layer. The worker orchestrates — it never
  runs user code in-process. See [`deploy/DEPLOY.md`](deploy/DEPLOY.md).
  (`WORKER_SANDBOX=false` runs code directly, for local dev without a container
  engine.)
- **Observability.** stdout/stderr stream back in batches (the response also
  carries the cancel flag); heartbeats keep the worker visible on the
  dashboard. If a worker dies mid-run, the backend reaps the run as
  "worker lost". Sandboxed runs are also **measured**: the worker samples the
  container's cgroup (memory, CPU, block I/O) on every tick of its log loop (0.7 s), ships the
  timeline with the log batches and a summary with the completion, and ends
  the run log with one `[resources]` line — see [Resource metrics](#resource-metrics).
- **Lifecycle.** SIGTERM/Ctrl-C is graceful: workers finish their current run,
  deregister, and exit. A second signal forces it.

## Running

```bash
python -m venv .venv && .venv/bin/pip install -r requirements.txt
cp .env.example .env    # point BACKEND_URL + WORKER_API_TOKEN at your backend

WORKER_SANDBOX=false \
.venv/bin/python main.py                # fixed pool (WORKER_POOL_SIZE), unsandboxed
.venv/bin/python main.py --workers 4    # explicit fixed pool
.venv/bin/python main.py --autoscale    # scale with queue depth
```

> Local dev sets `WORKER_SANDBOX=false` (no container engine needed). Production
> is sandboxed and runs on a DigitalOcean Droplet — see
> [`deploy/DEPLOY.md`](deploy/DEPLOY.md) for the one-command install.

## Configuration (.env)

| Variable | Default | Meaning |
| --- | --- | --- |
| `BACKEND_URL` | `http://127.0.0.1:8001` | The AirForge backend. |
| `WORKER_API_TOKEN` | dev token | Shared secret; must match the backend. |
| `WORKER_POOL_SIZE` | `2` | Fixed pool size (autoscale floor start). |
| `WORKER_AUTOSCALE` | `false` | Scale with queue depth instead. |
| `WORKER_MIN_WORKERS` / `WORKER_MAX_WORKERS` | `1` / `8` | Autoscale bounds. |
| `WORKER_SCALE_DOWN_IDLE_SECONDS` | `90` | Quiet time before retiring one worker. |
| `WORKER_JOB_PYTHON` | this interpreter | Interpreter for pipeline code (unsandboxed mode only). |
| `WORKER_SANDBOX` | `true` | Run each job in a gVisor container. Fail-closed. |
| `WORKER_CONTAINER_RUNTIME` | `runsc` | Container runtime; gVisor in prod, `""` for the engine default. |
| `WORKER_JOB_IMAGE` | `airforge/job-base:latest` | Image runs execute in (see `deploy/`). |
| `WORKER_JOB_MEMORY` / `WORKER_JOB_CPUS` / `WORKER_JOB_PIDS_LIMIT` | `1g` / `1` / `256` | Per-run resource caps. |
| `WORKER_CONTAINER_CGROUP_PARENT` | — | Slice bounding *all* runs' aggregate RAM/CPU. |
| `WORKER_METRICS` | `true` | Sample each run's cgroup and report its resource usage. |
| `WORKER_METRICS_SAMPLE_INTERVAL_SECONDS` | `0.5` | Minimum gap between samples; the run loop ticks every 0.7 s, so that is the effective cadence (stretches on long runs). |
| `WORKER_METRICS_MAX_SAMPLES` | `3600` | Soft cap on samples per run. |
| `WORKER_DEPS_CACHE_DIR` | `/var/lib/airforge/deps` | Cached `requirements.txt` layers, one per hash. |

The full sandbox/deploy knobs live in [`deploy/worker.env.example`](deploy/worker.env.example).

## Resource metrics

Every sandboxed run is one container in its own cgroup, so the kernel already
counts its memory, CPU time and block I/O; the worker just reads those counters
(`memory.current`, `memory.peak`, `cpu.stat`, `memory.events`, `io.stat`) while
the run executes. gVisor keeps its own runtime inside the container's cgroup,
so the memory figure is the whole sandbox — the same number the `--memory` cap
is enforced against. That makes "peak against limit" an honest OOM diagnosis:
a SIGKILL with the kernel's `oom_kill` counter set, or a peak near the cap, is
reported as such in the run's error instead of guessed from the exit code.

What the backend receives (a backend that predates these keys ignores them):

- `POST /worker/runs/{id}/logs` carries an optional `samples` list next to
  `entries`: `{"t": seconds since the container started, "mem": bytes,
  "cpu": cumulative CPU µs, "rd": bytes read, "wr": bytes written}`. Samples
  ride with whatever batch goes out, so a silent run still reports once per
  control-poll interval.
- `POST /worker/runs/{id}/complete` carries an optional `metrics` summary:
  `source` (`cgroup`; `unavailable` when the cgroup was never found; `none`
  when the run ended before the first sample), `sample_count`, `wall_seconds`,
  `memory_peak_bytes`, `memory_limit_bytes`, `cpu_seconds`, `cpu_limit_cores`,
  `oom_kills`, `io_read_bytes`, `io_write_bytes`, `workdir_bytes`.

The run log also ends with one `[resources]` line, so the numbers are visible
before the backend and UI store and chart them. If that line says
*unavailable*, the cgroup was not where the worker looked
(`/sys/fs/cgroup/<WORKER_CONTAINER_CGROUP_PARENT>/docker-<id>.scope` on the
systemd cgroup driver, then the usual defaults, then a shallow search) — the
worker journal names the container id and parent it tried. The container id
comes from a `--cidfile` next to the run directory, so no `docker inspect` is
needed. Past `WORKER_METRICS_MAX_SAMPLES` the interval stretches so a long run
yields a bounded number of samples. Unsandboxed runs (`WORKER_SANDBOX=false`)
have no cgroup and report nothing.

Autoscaling grows the pool immediately when queued work outnumbers hands
(`running + queued`, clamped to the bounds) and shrinks it one worker at a
time after sustained quiet — retiring is graceful, never mid-run.
