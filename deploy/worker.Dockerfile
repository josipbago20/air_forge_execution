# syntax=docker/dockerfile:1
#
# The AirForge worker as a container, for on-prem installs (built from the repo
# root):
#
#   docker build -f deploy/worker.Dockerfile -t airforge/worker:dev .
#
# It runs the supervisor and launches each pipeline run as a sibling container
# through the host engine's socket (mounted at /var/run/docker.sock), exactly as
# on a droplet. Droplets keep using install.sh + systemd; nothing here changes
# them. Settings worth knowing in a container (see airforge_worker/config.py):
#   WORKER_RUN_API_URL   the API address runs use on their own network
#   WORKER_INSTANCE_ID   label runs and scope orphan cleanup to this install
#   TMPDIR / WORKER_DEPS_CACHE_DIR  host paths mounted at the *same* path, since
#                        the engine resolves bind mounts on the host
#   WORKER_CONTAINER_RUNTIME=""  standard isolation where gVisor is not installed

FROM docker:27-cli AS dockercli

FROM python:3.11-slim
ENV PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
COPY --from=dockercli /usr/local/bin/docker /usr/local/bin/docker
WORKDIR /app
COPY requirements.txt deploy/requirements-onprem.txt ./
RUN pip install -r requirements.txt -r requirements-onprem.txt \
    && groupadd --system --gid 10001 airforge \
    && useradd --system --uid 10001 --gid airforge --home-dir /app --shell /usr/sbin/nologin airforge
COPY main.py ./
COPY airforge_worker ./airforge_worker
ARG AIRFORGE_VERSION=dev
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    AIRFORGE_VERSION=${AIRFORGE_VERSION} \
    WORKER_JOB_IMAGE=airforge/job-base:${AIRFORGE_VERSION}
USER airforge
# The supervisor drains the current run on the first SIGTERM; give it time
# (the Compose file sets stop_grace_period to the longest pipeline timeout).
STOPSIGNAL SIGTERM
CMD ["python", "main.py"]
