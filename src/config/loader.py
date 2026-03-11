from __future__ import annotations

from pathlib import Path

import yaml

from src.config.models import AppConfig, DatabaseConfig, PipelineConfig


def load_config(
    app_path: Path = Path("config/app.yaml"),
    pipelines_path: Path = Path("config/pipelines.yaml"),
) -> AppConfig:
    """Load application configuration from YAML files and environment variables.

    Structural settings come from YAML; database secrets come from env vars
    via pydantic-settings (DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD).
    """
    with open(app_path) as f:
        app_raw: dict = yaml.safe_load(f) or {}

    with open(pipelines_path) as f:
        pipelines_raw: dict = yaml.safe_load(f) or {}

    # DatabaseConfig reads DB_* env vars automatically
    app_raw["database"] = DatabaseConfig()

    app_raw["pipelines"] = [
        PipelineConfig(**p) for p in pipelines_raw.get("pipelines", [])
    ]

    return AppConfig(**app_raw)
