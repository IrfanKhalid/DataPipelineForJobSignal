"""CLI to run a single pipeline immediately (no scheduler).

Usage::

    python -m src.run_pipeline job_processing
    python -m src.run_pipeline job_processing --batch-size 10
"""

from __future__ import annotations

import argparse
import sys

from src.config.loader import load_config
from src.core.registry import get_pipeline_class
from src.db.connection import create_session_factory
from src.log_setup import setup_logging
from src.pipelines import discover_pipelines


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a pipeline immediately.")
    parser.add_argument("pipeline", help="Pipeline name as defined in pipelines.yaml")
    parser.add_argument(
        "--batch-size",
        type=int,
        default=None,
        help="Override the batch_size param for this run",
    )
    args = parser.parse_args()

    config = load_config()
    setup_logging(config.logging)
    discover_pipelines()

    # Find the pipeline config by name
    pipeline_config = None
    for pc in config.pipelines:
        if pc.name == args.pipeline:
            pipeline_config = pc
            break

    if pipeline_config is None:
        registered = [pc.name for pc in config.pipelines]
        print(f"Pipeline '{args.pipeline}' not found in pipelines.yaml.")
        print(f"Available: {registered}")
        sys.exit(1)

    if args.batch_size is not None:
        pipeline_config.params["batch_size"] = args.batch_size

    session_factory = create_session_factory(config.database)
    cls = get_pipeline_class(pipeline_config.pipeline_class)
    pipeline = cls(config=pipeline_config, session_factory=session_factory)
    pipeline.run()


if __name__ == "__main__":
    main()
