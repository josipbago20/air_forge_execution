#!/usr/bin/env bash
# DigitalOcean user-data ("Add initialization scripts" on the create-droplet
# form). Runs ONCE, as root, on first boot.
#
# ⚠️  DO NOT edit this tracked file with real values — the repo is public and a
# committed token lives in git history forever. Instead copy it to
# deploy/user-data.sh (gitignored), fill in the values THERE, and paste that
# into the form:
#
#     cp deploy/user-data.example.sh deploy/user-data.sh
#
# Watch progress after boot:   tail -f /var/log/cloud-init-output.log
#
# CAVEATS
#   • The token below becomes part of the droplet's user-data, readable on the
#     droplet via the metadata service (169.254.169.254). install.sh firewalls
#     that address off from job containers, so untrusted pipeline code cannot
#     read it — but treat user-data as semi-sensitive and rotate the token if
#     in doubt.
#   • If the repo is private, the clone URL needs a read-only deploy token
#     (https://<token>@github.com/you/air_forge_execution.git).
#   • No interactive commands here (no nano/editors): first boot has no TTY.
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive

REPO_URL="https://github.com/you/air_forge_execution.git"   # ← edit
BACKEND_URL="https://api.airforge.net"                      # ← edit if needed
WORKER_API_TOKEN="change-me"                                # ← edit

# On a 2 GB droplet the host needs a swap shock-absorber: job cgroups have swap
# disabled (deterministic per-run OOM), but the OS + Docker should page rather
# than OOM when the pool is saturated.
if ! swapon --show | grep -q .; then
  fallocate -l 2G /swapfile
  chmod 600 /swapfile
  mkswap /swapfile
  swapon /swapfile
  echo '/swapfile none swap sw 0 0' >> /etc/fstab
fi

apt-get update -y
apt-get install -y git

git clone "$REPO_URL" /root/air_forge_execution
bash /root/air_forge_execution/deploy/install.sh

# Non-interactive stand-in for "edit /etc/airforge/worker.env": keep the tuned
# sizing from worker.env.example, override only the deployment-specific values.
sed -i \
  -e "s|^BACKEND_URL=.*|BACKEND_URL=${BACKEND_URL}|" \
  -e "s|^WORKER_API_TOKEN=.*|WORKER_API_TOKEN=${WORKER_API_TOKEN}|" \
  /etc/airforge/worker.env
chmod 600 /etc/airforge/worker.env

systemctl enable --now airforge-worker
