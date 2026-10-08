# Deploying the AirForge execution worker pool

This service is the **playground execution surface**: it runs arbitrary Python
submitted by public users. Two consequences shape the whole deployment:

1. **Untrusted code** → every run executes inside a **gVisor** sandbox with a
   hard RAM/CPU/PID budget. The worker never runs user code in-process.
2. **A long-running pull worker** (it long-polls the backend; nothing connects
   *to* it) that needs kernel/cgroup control → it runs on a **DigitalOcean
   Droplet**, not App Platform.

```
DigitalOcean Droplet (same region/VPC as the backend)
 ├─ systemd: airforge-worker.service ──> supervisor ──> N warm workers
 │     each worker:  claim (long-poll) → run → complete   (register/heartbeat)
 └─ per run:  docker run --runtime=runsc  (gVisor sandbox)
                 ├─ cgroup caps: --memory / --cpus / --pids-limit
                 ├─ airforge-jobs.slice: aggregate ceiling for all runs
                 ├─ /work  = the pipeline bundle (rw)
                 └─ /deps  = cached dependency layer (ro, built once per reqs)
```

## Why a Droplet (and not App Platform)

| Requirement | Droplet | App Platform |
| --- | --- | --- |
| gVisor / custom container runtime for untrusted code | ✅ full control | ❌ not possible |
| Per-run cgroup RAM/CPU caps | ✅ | ❌ no cgroup knobs |
| Persistent dependency-layer cache across restarts | ✅ disk | ❌ ephemeral FS |
| Long graceful drain (finish in-flight run on redeploy) | ✅ `TimeoutStopSec` | ❌ short, fixed |
| No inbound port (pure long-poll worker) | ✅ | ⚠️ "Worker" type only |

At larger scale, DigitalOcean Kubernetes (DOKS) with a gVisor `RuntimeClass` is
the same model, orchestrated. One Droplet is the right starting point.

---

## 1. Create the Droplet

- **Region:** the **same** region as the backend — each claim ships the
  pipeline's whole folder base64-inlined, plus constant log traffic.
- **Size:** the current config targets a **2 GB / 1 vCPU** droplet running
  **3 workers** (512 MB per run) — enough for the pre-baked pandas/sklearn
  stack on modest data. See the sizing table for the knobs when scaling up.
- **Image:** Ubuntu 22.04 or 24.04 LTS.
- **Networking:** outbound internet must be open (playground code calls public
  APIs; workers reach PyPI). Nothing connects inbound to the workers.

### Sizing

**The rule:** `max_workers × WORKER_JOB_MEMORY ≤ airforge-jobs.slice MemoryMax
≤ droplet RAM − ~700 MB host headroom` (OS + Docker + the worker processes,
each ~40–50 MB warm).

| Droplet | Slice (`MemoryMax` / `CPUQuota`) | Pool | Fits |
| --- | --- | --- | --- |
| 2 GB / 1 vCPU | `1600M` / `90%` | **3 × 512m** (current) | Modest pandas/sklearn runs, three at a time. |
| 2 GB / 1 vCPU | `1600M` / `90%` | 10 × 160m | Many small API-glue scripts — but **not** pandas/sklearn (`import sklearn` alone is ~150–200 MB and OOMs). |
| 8 GB / 4 vCPU | `6G` / `350%` | 6 × 1g | Comfortable scientific-stack runs. |

At full concurrency on 1 vCPU, three runs share ~0.9 core (~0.3 each) — size
pipeline `timeout_seconds` generously. Changing tier = edit `WORKER_JOB_MEMORY`
/ `WORKER_MAX_WORKERS` in `worker.env` **and** the slice's `MemoryMax`, then
`systemctl daemon-reload && systemctl restart airforge-jobs.slice airforge-worker`.

## 2. Provision it

**Option A — automatically, at droplet creation (recommended).** DigitalOcean's
*"Add initialization scripts (free)"* box takes a cloud-init user-data script.
Copy the example to a **gitignored** working copy, fill in the repo URL,
`BACKEND_URL`, and worker token there, and paste *that* into the form:

```bash
cp deploy/user-data.example.sh deploy/user-data.sh   # gitignored — safe for the token
```

Never put the real token in the tracked example file: the repo is public, and
a pushed token lives in git history forever (deleting the file later does not
un-leak it — rotation is the only fix). On first boot the script adds host
swap, clones the repo, runs the installer, writes `/etc/airforge/worker.env`,
and starts the service — no SSH needed. Watch it with
`tail -f /var/log/cloud-init-output.log`. (No interactive commands like `nano`
can run there; the script writes the env file itself.)

*Zero-secrets-in-user-data alternative:* delete the `WORKER_API_TOKEN` line and
the `sed` override from your copy, let the droplet come up with the service
stopped-or-failing to register, then SSH in once and put the real token in
`/etc/airforge/worker.env` by hand. The token then exists only on the droplet.

**Option B — by hand.** SSH in as root:

```bash
git clone <your-remote>/air_forge_execution.git
cd air_forge_execution
sudo bash deploy/install.sh
```

`install.sh` is idempotent and does all of:

- installs Docker + **gVisor** (`runsc`, checksum-verified) and registers the
  runtime with Docker;
- sets Docker's **systemd cgroup driver** (required for the aggregate slice);
- **firewalls the cloud metadata service** (`169.254.169.254`) off from
  containers, so untrusted code can't read the droplet's user-data/secrets;
- creates the `airforge` service user (in the `docker` group);
- builds the worker venv and the **base job image** (`airforge/job-base`);
- installs the `airforge-jobs.slice` and `airforge-worker.service` units and a
  `/etc/airforge/worker.env` from the example.

## 3. Configure

Edit `/etc/airforge/worker.env` (see [`worker.env.example`](worker.env.example)
for every knob; the user-data script fills these two in for you):

```bash
BACKEND_URL=https://api.airforge.net
WORKER_API_TOKEN=<must match the backend's WORKER_API_TOKEN>
```

> **BACKEND_URL must be routable from inside a job container** — a public
> domain is fine; `127.0.0.1` is not (that's the container itself). User code
> reads it as `AIRFORGE_API_URL` to call the backend's `/runtime` API with its
> run-scoped token.
>
> **CORS does not apply here.** CORS is a browser-enforced mechanism; the
> worker's HTTP client and pipeline code send no `Origin` header and are never
> subject to it. The backend's CORS settings need no change for workers. The
> worker protocol is authenticated by `X-Worker-Token` alone — over a public
> domain, HTTPS is what protects it in transit.

## 4. Start it

```bash
sudo systemctl enable --now airforge-worker
journalctl -u airforge-worker -f
```

You should see the sandbox preflight pass, then workers register and go "Warm
and waiting for work." They now appear on the backend's Pipelines dashboard.

---

## How dependencies work (no per-run `pip install`)

A pipeline that needs packages ships a `requirements.txt` in its folder — it
rides along in the claim bundle automatically (no backend change).

- The worker hashes the file's contents and builds a **dependency layer** once,
  at `/var/lib/airforge/deps/<hash>/`, inside a sandboxed, networked build
  container. A cross-process lock means two workers never build the same layer.
- Every later run with the same requirements just **mounts that layer read-only**
  at `/deps` — no install.
- **uv's shared wheel cache** (`/var/lib/airforge/uv-cache`) makes even a new
  hash cheap when it overlaps existing layers.
- The base image **pre-bakes** a modest common stack (`requests`, `httpx`,
  `numpy`, `pandas`, `python-dateutil`, `scikit-learn`, plus the
  `clickhouse-connect` and `google-cloud-bigquery`/`db-dtypes` clients), so
  many pipelines install nothing at all. A pipeline's own layer takes precedence over the
  baked packages (it's ahead on `PYTHONPATH`).
- The cache is pruned (LRU) back under `WORKER_DEPS_CACHE_MAX_GB` after builds.

To widen the pre-baked set, edit [`job-base.Dockerfile`](job-base.Dockerfile),
rebuild (`docker build -t airforge/job-base:latest -f deploy/job-base.Dockerfile
deploy`), and restart the service.

## Resource limits & isolation

- **Per run:** `--memory` (hard cap, OOM-killed inside its own cgroup),
  `--cpus` (fraction/multiple of a core), `--pids-limit` (fork-bomb guard).
  Tune via `WORKER_JOB_*` in `worker.env`.
- **Aggregate:** all job + build containers run under `airforge-jobs.slice`,
  whose `MemoryMax`/`CPUQuota` cap the pool as a whole. Edit the slice unit to
  match your Droplet, then `systemctl daemon-reload && systemctl restart
  airforge-jobs.slice`.
- **Isolation:** gVisor (`runsc`) intercepts syscalls in userspace — a real
  boundary for untrusted code. On top: `--cap-drop ALL`,
  `--security-opt no-new-privileges`, read-only rootfs (only `/work` + a small
  `/tmp` tmpfs are writable), and an explicit env so the worker's token/host
  environment never enters the container.

## Operations

**Deploy an update** (drains in-flight runs, up to `TimeoutStopSec`):
```bash
cd /opt/airforge/air_forge_execution && sudo git pull
sudo bash deploy/install.sh        # rebuilds venv + image, reinstalls units
sudo systemctl restart airforge-worker
```

**Scale:** raise `WORKER_MAX_WORKERS` (respecting the sizing rule) and restart,
or resize the Droplet, or add another Droplet pointed at the same backend — the
Postgres queue makes workers horizontally safe (`FOR UPDATE SKIP LOCKED`).

**Verify gVisor is actually in use:**
```bash
docker run --rm --runtime=runsc airforge/job-base:latest dmesg | head   # says "gVisor"
```

**Logs:** `journalctl -u airforge-worker -f`. Per-run stdout/stderr stream to
the backend and the dashboard, not here.

## Security notes

- gVisor + caps + no-new-privileges + read-only rootfs contain a *single* run.
  They do **not** stop a run from abusing the **open network** (you chose full
  internet so pipelines can call public APIs) — add egress rate-limiting /
  abuse monitoring at the network layer if that becomes a problem.
- Dependency installs execute arbitrary package build code; that's why the build
  step is sandboxed too, not just the run.
- Keep `WORKER_API_TOKEN` in `/etc/airforge/worker.env` at mode `600`; it never
  enters a job container.
- The droplet's **metadata service** (`169.254.169.254`) exposes user-data —
  including any provisioning script and the token inside it. The installer adds
  a persistent `DOCKER-USER` iptables rule dropping container traffic to it.
  Verify after deploy:
  `docker run --rm --runtime=runsc airforge/job-base:latest python -c "import urllib.request;urllib.request.urlopen('http://169.254.169.254/metadata/v1/user-data', timeout=5)"`
  — this must fail.

## Troubleshooting

| Symptom | Likely cause |
| --- | --- |
| Service exits at start: "Sandbox preflight failed" | Docker or `runsc` missing/misconfigured; run the probe: `docker run --rm --runtime=runsc airforge/job-base:latest true` |
| Runs fail: "Dependency install failed" | Bad `requirements.txt`, no PyPI egress, or the build hit `WORKER_DEP_INSTALL_TIMEOUT_SECONDS` |
| User code can't reach the backend `/runtime` API | `BACKEND_URL` is `127.0.0.1` or otherwise not routable from inside the container |
| `--cgroup-parent` errors | Docker not on the systemd cgroup driver (re-run the installer) or the slice isn't started |
| Runs OOM immediately | `WORKER_JOB_MEMORY` too low for the workload, or the aggregate slice cap is saturated |
