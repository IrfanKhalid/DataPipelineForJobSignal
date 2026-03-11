from __future__ import annotations

from typing import Any

from pydantic import BaseModel
from pydantic_settings import BaseSettings


class DatabaseConfig(BaseSettings):
    """Database configuration. Reads from environment variables with DB_ prefix."""

    model_config = {"env_prefix": "DB_"}

    host: str = "localhost"
    port: int = 5432
    name: str = "jobsignal"
    user: str = "postgres"
    password: str = "postgres"

    @property
    def url(self) -> str:
        return (
            f"postgresql://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.name}"
        )


class RetryConfig(BaseModel):
    """Retry behaviour for a pipeline."""

    max_attempts: int = 3
    backoff_base: float = 2.0
    backoff_max: float = 60.0


class PipelineConfig(BaseModel):
    """Definition of a single pipeline from pipelines.yaml."""

    name: str
    pipeline_class: str  # registry key, e.g. "example_scraper"
    schedule: str  # 5-field cron expression, e.g. "*/30 * * * *"
    enabled: bool = True
    retry: RetryConfig = RetryConfig()
    params: dict[str, Any] = {}


class SchedulerConfig(BaseModel):
    """APScheduler tuning knobs."""

    timezone: str = "UTC"
    coalesce: bool = True
    max_instances: int = 1


class LoggingConfig(BaseModel):
    """Logging configuration."""

    level: str = "INFO"
    format: str = "json"  # "json" for production, "console" for dev


class HealthCheckConfig(BaseModel):
    """Health-check HTTP server settings."""

    port: int = 8080
    path: str = "/health"


class AppConfig(BaseModel):
    """Top-level application configuration assembled by the loader."""

    database: DatabaseConfig = DatabaseConfig()
    scheduler: SchedulerConfig = SchedulerConfig()
    logging: LoggingConfig = LoggingConfig()
    health_check: HealthCheckConfig = HealthCheckConfig()
    pipelines: list[PipelineConfig] = []
