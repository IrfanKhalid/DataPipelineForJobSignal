from __future__ import annotations

from typing import Type

from src.core.base_pipeline import BasePipeline

_REGISTRY: dict[str, Type[BasePipeline]] = {}


def register_pipeline(name: str):
    """Class decorator that registers a pipeline under *name*.

    Usage::

        @register_pipeline("my_scraper")
        class MyScraper(BasePipeline[...]):
            ...
    """

    def decorator(cls: Type[BasePipeline]) -> Type[BasePipeline]:
        if name in _REGISTRY:
            raise ValueError(
                f"Pipeline '{name}' already registered by {_REGISTRY[name].__name__}"
            )
        _REGISTRY[name] = cls
        return cls

    return decorator


def get_pipeline_class(name: str) -> Type[BasePipeline]:
    """Look up a registered pipeline class by *name*."""
    if name not in _REGISTRY:
        available = list(_REGISTRY.keys())
        raise KeyError(f"Pipeline '{name}' not found. Registered: {available}")
    return _REGISTRY[name]


def all_pipelines() -> dict[str, Type[BasePipeline]]:
    """Return a copy of the full registry."""
    return dict(_REGISTRY)
