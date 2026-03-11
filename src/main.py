"""Application entry point.

Wires together configuration, logging, database, scheduler, and health check,
then keeps the main thread alive until a shutdown signal is received.
"""

from __future__ import annotations

import signal
import sys
import time

import structlog

from src.config.loader import load_config
from src.core.health import HealthCheckServer
from src.core.scheduler import PipelineScheduler
from src.db.connection import create_session_factory
from src.log_setup import setup_logging
from src.pipelines import discover_pipelines

logger = structlog.get_logger()


def main() -> None:
    # 1. Load configuration
    config = load_config()

    # 2. Setup structured logging
    setup_logging(config.logging)

    # 3. Auto-discover pipeline classes (@register_pipeline decorators)
    discover_pipelines()

    # 4. Database session factory
    session_factory = create_session_factory(config.database)

    # 5. Start scheduler
    scheduler = PipelineScheduler(config, session_factory)
    scheduler.start()

    # 6. Start health-check HTTP server
    def build_status() -> dict:
        return {
            "healthy": scheduler.running,
            "scheduler_running": scheduler.running,
            "jobs": scheduler.jobs,
        }

    health = HealthCheckServer(config.health_check.port, build_status)
    health.start()

    # 7. Handle graceful shutdown
    def shutdown(signum: int | None, _frame: object) -> None:
        logger.info("shutdown_signal_received", signal=signum)
        scheduler.shutdown()
        health.stop()
        sys.exit(0)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    # 8. Keep main thread alive
    logger.info("application_started")
    try:
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        shutdown(None, None)


if __name__ == "__main__":
    main()
