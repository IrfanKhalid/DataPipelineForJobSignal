"""Pipeline auto-discovery.

Importing this module and calling :func:`discover_pipelines` walks every
submodule under ``src.pipelines`` so that ``@register_pipeline`` decorators
execute and make their classes available to the scheduler.
"""

from __future__ import annotations

import importlib
import pkgutil
from pathlib import Path


def discover_pipelines() -> None:
    """Import all submodules so @register_pipeline decorators execute."""
    package_dir = Path(__file__).parent
    for _finder, module_name, _is_pkg in pkgutil.walk_packages(
        [str(package_dir)], prefix=__name__ + "."
    ):
        importlib.import_module(module_name)
