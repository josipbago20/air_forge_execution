"""AirForge execution workers: a warm pool that runs pipeline code.

Each worker is its own OS process. It registers with the backend, heartbeats,
long-polls the claim endpoint for queued runs, executes each run's code in a
fresh subprocess and temp directory, streams the output back in batches, and
reports the outcome. The supervisor keeps N workers alive (fixed pool size or
autoscaled from queue depth).
"""

__version__ = "0.1.0"
