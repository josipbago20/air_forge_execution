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
  "worker lost".
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
| `WORKER_DEPS_CACHE_DIR` | `/var/lib/airforge/deps` | Cached `requirements.txt` layers, one per hash. |

The full sandbox/deploy knobs live in [`deploy/worker.env.example`](deploy/worker.env.example).

Autoscaling grows the pool immediately when queued work outnumbers hands
(`running + queued`, clamped to the bounds) and shrinks it one worker at a
time after sustained quiet — retiring is graceful, never mid-run.
