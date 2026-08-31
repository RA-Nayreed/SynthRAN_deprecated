"""SynthRAN experiment orchestration package."""

from __future__ import annotations

from importlib import import_module
import sys

__version__ = "0.0.1"


def _module_alias(name: str, target: str) -> None:
    """Keep the last test-only import contracts while migration completes."""

    sys.modules[f"{__name__}.{name}"] = import_module(target)


# Final compatibility debt: older network tests still patch these synthetic
# module paths. Production code uses the canonical synthran.network package.
_module_alias("network_runtime", "synthran.network.runtime")
_module_alias("rfsim_runtime", "synthran.network.rfsim")
_module_alias("resource_runtime", "synthran.network.resources")
