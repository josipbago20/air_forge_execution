# The image every playground run executes inside (and that dependency layers are
# built with). It MUST ship the same Python the layers are installed for — uv
# installs a pipeline's requirements into /out for *this* interpreter, and the
# run then imports them from /deps. If you change the Python minor version here,
# clear the layer cache (/var/lib/airforge/deps) so layers rebuild against it.
FROM python:3.11-slim

# uv: fast installer with a shared wheel cache. Used to prebake the stack below
# and, at runtime, to build each pipeline's dependency layer.
RUN pip install --no-cache-dir uv

# ── Prebaked common stack ─────────────────────────────────────────────────────
# A modest, widely-used set so the majority of playground pipelines import what
# they need with zero per-run install. Anything else a pipeline lists in its
# requirements.txt is layered on top at runtime and takes precedence (the run
# mounts it at /deps, which is ahead of these on PYTHONPATH).
#
# All of these ship manylinux wheels, so no compiler is needed. If you later
# accept pipelines that build from source, add build-essential here.
RUN uv pip install --system --no-cache \
        requests \
        httpx \
        numpy \
        pandas \
        python-dateutil \
        scikit-learn

# The worker supplies the exact command and --user per run; no entrypoint here.
CMD ["python3"]
