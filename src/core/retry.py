from __future__ import annotations

from typing import Callable, TypeVar

import structlog
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from src.config.models import RetryConfig

logger = structlog.get_logger()
F = TypeVar("F", bound=Callable)


def with_retry(config: RetryConfig) -> Callable[[F], F]:
    """Return a tenacity retry decorator configured from *config*.

    Usage::

        retried_fn = with_retry(pipeline_config.retry)(some_callable)
        retried_fn()
    """
    return retry(  # type: ignore[return-value]
        stop=stop_after_attempt(config.max_attempts),
        wait=wait_exponential(
            multiplier=config.backoff_base,
            max=config.backoff_max,
        ),
        before_sleep=before_sleep_log(logger, structlog.stdlib.INFO),
        retry=retry_if_exception_type(Exception),
        reraise=True,
    )
