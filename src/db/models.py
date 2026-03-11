from __future__ import annotations

import enum
from datetime import datetime, timezone

from sqlalchemy import Column, DateTime, Enum, Integer, String, Text
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class RunStatus(str, enum.Enum):
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"


class PipelineRun(Base):  # type: ignore[misc]
    """Tracks individual pipeline execution runs."""

    __tablename__ = "pipeline_runs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    pipeline_name = Column(String(255), nullable=False, index=True)
    status = Column(Enum(RunStatus), nullable=False)
    started_at = Column(
        DateTime, default=lambda: datetime.now(timezone.utc), nullable=False
    )
    finished_at = Column(DateTime, nullable=True)
    error_message = Column(Text, nullable=True)
    records_processed = Column(Integer, default=0)
