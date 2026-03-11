from __future__ import annotations

import structlog
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy.orm import Session, sessionmaker

from src.config.models import AppConfig, PipelineConfig
from src.core.registry import get_pipeline_class

logger = structlog.get_logger()


class PipelineScheduler:
    """Wraps APScheduler to schedule pipeline jobs from configuration."""

    def __init__(
        self,
        config: AppConfig,
        session_factory: sessionmaker[Session],
    ) -> None:
        self.config = config
        self.session_factory = session_factory
        self._scheduler = BackgroundScheduler(
            timezone=config.scheduler.timezone,
            job_defaults={
                "coalesce": config.scheduler.coalesce,
                "max_instances": config.scheduler.max_instances,
            },
        )

    def _run_pipeline_job(self, pipeline_config: PipelineConfig) -> None:
        """Callback invoked by APScheduler on each trigger."""
        cls = get_pipeline_class(pipeline_config.pipeline_class)
        instance = cls(config=pipeline_config, session_factory=self.session_factory)
        instance.run()

    def register_all(self) -> None:
        """Register every enabled pipeline from configuration."""
        for pc in self.config.pipelines:
            if not pc.enabled:
                logger.info("pipeline_skipped_disabled", pipeline=pc.name)
                continue

            parts = pc.schedule.split()
            if len(parts) != 5:
                logger.error(
                    "invalid_cron_expression",
                    pipeline=pc.name,
                    schedule=pc.schedule,
                )
                continue

            trigger = CronTrigger(
                minute=parts[0],
                hour=parts[1],
                day=parts[2],
                month=parts[3],
                day_of_week=parts[4],
                timezone=self.config.scheduler.timezone,
            )

            self._scheduler.add_job(
                func=self._run_pipeline_job,
                trigger=trigger,
                args=[pc],
                id=pc.name,
                name=pc.name,
                replace_existing=True,
            )
            logger.info("pipeline_scheduled", pipeline=pc.name, schedule=pc.schedule)

    def start(self) -> None:
        """Start the scheduler (non-blocking — runs in a background thread)."""
        self.register_all()
        self._scheduler.start()
        logger.info(
            "scheduler_started",
            pipeline_count=len(self._scheduler.get_jobs()),
        )

    def shutdown(self) -> None:
        """Gracefully shut down, waiting for running jobs to complete."""
        self._scheduler.shutdown(wait=True)

    @property
    def running(self) -> bool:
        return self._scheduler.running

    @property
    def jobs(self) -> list[dict]:
        return [
            {"id": j.id, "name": j.name, "next_run": str(j.next_run_time)}
            for j in self._scheduler.get_jobs()
        ]
