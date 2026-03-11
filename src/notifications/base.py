"""Notification abstractions.

Currently only :class:`LogNotifier` is implemented (logs events via structlog).
To add Slack, email, or other channels:

1. Subclass :class:`BaseNotifier`
2. Add it to a :class:`CompositeNotifier` in ``main.py``

No pipeline code changes required.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

import structlog


class EventType(str, Enum):
    PIPELINE_STARTED = "pipeline_started"
    PIPELINE_COMPLETED = "pipeline_completed"
    PIPELINE_FAILED = "pipeline_failed"


@dataclass
class PipelineEvent:
    event_type: EventType
    pipeline_name: str
    details: dict[str, Any] = field(default_factory=dict)


class BaseNotifier(ABC):
    """Interface for notification channels."""

    @abstractmethod
    def notify(self, event: PipelineEvent) -> None: ...


class LogNotifier(BaseNotifier):
    """Default notifier — logs events via structlog."""

    def __init__(self) -> None:
        self.log = structlog.get_logger()

    def notify(self, event: PipelineEvent) -> None:
        self.log.info(
            "notification",
            event_type=event.event_type.value,
            pipeline=event.pipeline_name,
            details=event.details,
        )


class CompositeNotifier(BaseNotifier):
    """Fans out to multiple notifiers."""

    def __init__(self, notifiers: list[BaseNotifier]) -> None:
        self._notifiers = notifiers

    def notify(self, event: PipelineEvent) -> None:
        for notifier in self._notifiers:
            notifier.notify(event)
