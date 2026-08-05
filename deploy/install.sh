#!/usr/bin/env bash
# Provision a DigitalOcean Droplet (Ubuntu 22.04 / 24.04) to run the AirForge
# execution worker pool with gVisor-sandboxed jobs.
#
# Run as root, from the cloned repo:
#     sudo bash deploy/install.sh
#
# Idempotent: safe to re-run after pulling new code (it rebuilds the venv +
# image and reinstalls the units, but never overwrites /etc/airforge/worker.env).
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
APP_USER=airforge
APP_HOME=/opt/airforge
APP_DIR="$APP_HOME/air_forge_execution"
IMAGE=airforge/job-base:latest

log() { echo -e "\n\033[1;36m==> $*\033[0m"; }

if [[ $EUID -ne 0 ]]; then echo "Run as root (sudo bash deploy/install.sh)."; exit 1; fi

log "Installing base packages (Docker, Python, git)…"
apt-get update -y
apt-get install -y --no-install-recommends \
    ca-certificates curl gnupg git python3 python3-venv python3-pip

if ! command -v docker >/dev/null 2>&1; then
  log "Installing Docker…"
  curl -fsSL https://get.docker.com | sh
fi

if ! command -v runsc >/dev/null 2>&1; then
  log "Installing gVisor (runsc), with checksum verification…"
  ARCH=$(uname -m)
  URL="https://storage.googleapis.com/gvisor/releases/release/latest/${ARCH}"
  tmp=$(mktemp -d)
  for bin in runsc containerd-shim-runsc-v1; do
    curl -fsSL "${URL}/${bin}"        -o "${tmp}/${bin}"
    curl -fsSL "${URL}/${bin}.sha512" -o "${tmp}/${bin}.sha512"
    ( cd "$tmp" && sha512sum -c "${bin}.sha512" )
    install -m 0755 "${tmp}/${bin}" "/usr/local/bin/${bin}"
  done
  rm -rf "$tmp"
fi

log "Registering runsc with Docker and setting the systemd cgroup driver…"
runsc install   # adds the "runsc" runtime to /etc/docker/daemon.json
# --cgroup-parent slices require Docker's systemd cgroup driver.
python3 - <<'PY'
import json, pathlib
p = pathlib.Path("/etc/docker/daemon.json")
cfg = json.loads(p.read_text()) if p.exists() and p.read_text().strip() else {}
opts = [o for o in cfg.get("exec-opts", []) if not o.startswith("native.cgroupdriver=")]
opts.append("native.cgroupdriver=systemd")
cfg["exec-opts"] = opts
p.write_text(json.dumps(cfg, indent=2))
print(p.read_text())
PY
systemctl restart docker

log "Blocking the cloud metadata service from job containers…"
# The droplet's metadata endpoint (169.254.169.254) exposes user-data — which
# may contain provisioning secrets. Untrusted pipeline code must never read it.
# DOCKER-USER is consulted for all container bridge traffic.
apt-get install -y --no-install-recommends iptables-persistent
iptables -C DOCKER-USER -d 169.254.169.254 -j DROP 2>/dev/null || \
  iptables -I DOCKER-USER -d 169.254.169.254 -j DROP
netfilter-persistent save

log "Creating service user '$APP_USER'…"
id -u "$APP_USER" >/dev/null 2>&1 || \
  useradd --system --home "$APP_HOME" --shell /usr/sbin/nologin "$APP_USER"
usermod -aG docker "$APP_USER"

log "Placing code at $APP_DIR…"
mkdir -p "$APP_DIR"
if [[ "$REPO_DIR" != "$APP_DIR" ]]; then
  cp -a "$REPO_DIR/." "$APP_DIR/"
fi
chown -R "$APP_USER:$APP_USER" "$APP_HOME"

log "Building the worker venv…"
sudo -u "$APP_USER" python3 -m venv "$APP_DIR/.venv"
sudo -u "$APP_USER" "$APP_DIR/.venv/bin/pip" install --quiet --upgrade pip
sudo -u "$APP_USER" "$APP_DIR/.venv/bin/pip" install --quiet -r "$APP_DIR/requirements.txt"

log "Building the base job image ($IMAGE)…"
docker build -t "$IMAGE" -f "$APP_DIR/deploy/job-base.Dockerfile" "$APP_DIR/deploy"

log "Installing systemd units…"
install -m 0644 "$APP_DIR/deploy/airforge-jobs.slice"     /etc/systemd/system/airforge-jobs.slice
install -m 0644 "$APP_DIR/deploy/airforge-worker.service" /etc/systemd/system/airforge-worker.service

mkdir -p /etc/airforge
if [[ ! -f /etc/airforge/worker.env ]]; then
  install -m 0600 "$APP_DIR/deploy/worker.env.example" /etc/airforge/worker.env
  NEEDS_EDIT=1
fi

systemctl daemon-reload
systemctl start airforge-jobs.slice

log "Provisioning complete."
if [[ "${NEEDS_EDIT:-0}" == "1" ]]; then
  echo "  1. Edit /etc/airforge/worker.env  →  set BACKEND_URL + WORKER_API_TOKEN and sizing"
fi
echo "  2. systemctl enable --now airforge-worker"
echo "  3. journalctl -u airforge-worker -f     # watch it register + claim"
