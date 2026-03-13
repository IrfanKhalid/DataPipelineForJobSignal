from __future__ import annotations

import enum
from datetime import datetime, timezone

from sqlalchemy import Column, DateTime, Enum, Integer, String, Text, UniqueConstraint, text
from sqlalchemy.dialects.postgresql import UUID
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


class ProcessingJob(Base):  # type: ignore[misc]
    """Unique jobs extracted from JobDetails for downstream processing."""

    __tablename__ = "processing_jobs"
    __table_args__ = (
        UniqueConstraint("jobs_ids", "apply_url", name="uq_jobs_ids_apply_url"),
    )

    id = Column(UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()"))
    jobs_ids = Column(String(255), nullable=False, index=True)
    title = Column(String(500), nullable=True)
    location = Column(String(500), nullable=True)
    description = Column(Text, nullable=True)
    apply_url = Column(String(2048), nullable=False)
    created_at = Column(
        DateTime, default=lambda: datetime.now(timezone.utc), nullable=False
    )
