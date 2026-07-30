"""AirForge execution service — the warm worker pool.

Run it::

    python main.py                 # pool size from .env / WORKER_POOL_SIZE
    python main.py --workers 4     # explicit fixed pool
    python main.py --autoscale     # scale with queue depth (min/max from env)

Every worker registers itself with the backend, so the pool is visible live on
the dashboard's Pipelines page.
"""

from __future__ import annotations

import argparse
import logging
import os


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="AirForge pipeline workers")
    parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help="Fixed pool size (overrides WORKER_POOL_SIZE)",
    )
    parser.add_argument(
        "--autoscale",
        action="store_true",
        help="Scale the pool with queue depth (overrides WORKER_AUTOSCALE)",
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s [supervisor] %(name)s: %(message)s",
    )

    args = parse_args()
    # CLI beats env; config reads env, so translate before loading it.
    if args.workers is not None:
        os.environ["WORKER_POOL_SIZE"] = str(args.workers)
    if args.autoscale:
        os.environ["WORKER_AUTOSCALE"] = "true"

    from airforge_worker.config import load_config
    from airforge_worker.supervisor import Supervisor

    Supervisor(load_config()).run()


if __name__ == "__main__":
    main()
