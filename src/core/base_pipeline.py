from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Generic, TypeVar

import structlog
from sqlalchemy.orm import sessionmaker, Session

from src.config.models import PipelineConfig
from src.core.retry import with_retry

T_Raw = TypeVar("T_Raw")
T_Clean = TypeVar("T_Clean")

logger = structlog.get_logger()


class BasePipeline(ABC, Generic[T_Raw, T_Clean]):
    """Abstract base for all pipelines.

    Subclasses implement :meth:`extract`, :meth:`transform`, and :meth:`load`.
    The :meth:`run` method orchestrates them with logging, retry, and error
    handling.
    """

    def __init__(
        self,
        config: PipelineConfig,
        session_factory: sessionmaker[Session],
    ) -> None:
        self.config = config
        self.params: dict[str, Any] = config.params
        self.session_factory = session_factory
        self.log = logger.bind(pipeline=config.name)

    # ------------------------------------------------------------------
    # Hooks — implement in subclasses
    # ------------------------------------------------------------------

    @abstractmethod
    def extract(self) -> T_Raw:
        """Pull raw data from the source."""
        ...

    @abstractmethod
    def transform(self, raw_data: T_Raw) -> T_Clean:
        """Clean / reshape the raw data."""
        ...

    @abstractmethod
    def load(self, clean_data: T_Clean) -> None:
        """Write processed data to the destination."""
        ...

    # ------------------------------------------------------------------
    # Orchestration
    # ------------------------------------------------------------------

    def run(self) -> None:
        """Template method.  Orchestrates extract -> transform -> load
        with retry wrapping on the full cycle."""
        self.log.info("pipeline_started")
        try:
            retried = with_retry(self.config.retry)(self._execute)
            retried()
            self.log.info("pipeline_completed")
        except Exception as exc:
            self.log.error("pipeline_failed", error=str(exc), exc_info=True)
            raise

    def _execute(self) -> None:
        raw = self.extract()
        clean = self.transform(raw)
        self.load(clean)
