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

.venv/bin/python main.py                # fixed pool (WORKER_POOL_SIZE)
.venv/bin/python main.py --workers 4    # explicit fixed pool
.venv/bin/python main.py --autoscale    # scale with queue depth
```

## Configuration (.env)

| Variable | Default | Meaning |
| --- | --- | --- |
| `BACKEND_URL` | `http://127.0.0.1:8001` | The AirForge backend. |
| `WORKER_API_TOKEN` | dev token | Shared secret; must match the backend. |
| `WORKER_POOL_SIZE` | `2` | Fixed pool size (autoscale floor start). |
| `WORKER_AUTOSCALE` | `false` | Scale with queue depth instead. |
| `WORKER_MIN_WORKERS` / `WORKER_MAX_WORKERS` | `1` / `8` | Autoscale bounds. |
| `WORKER_SCALE_DOWN_IDLE_SECONDS` | `90` | Quiet time before retiring one worker. |
| `WORKER_JOB_PYTHON` | this interpreter | Interpreter used for pipeline code. |

Autoscaling grows the pool immediately when queued work outnumbers hands
(`running + queued`, clamped to the bounds) and shrinks it one worker at a
time after sustained quiet — retiring is graceful, never mid-run.
