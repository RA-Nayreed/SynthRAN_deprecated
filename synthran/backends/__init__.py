"""Backend registry for the single SynthRAN command surface."""

from __future__ import annotations

import argparse
from collections.abc import Sequence

from synthran.backends.base import (
    Backend,
    BackendContract,
    BackendError,
    BackendName,
    LIFECYCLE_STAGES,
    LifecycleStage,
    RadioMode,
)
from synthran.backends.r2lab import R2LabBackend
from synthran.backends.rfsim import RfsimBackend


_BACKENDS: dict[BackendName, Backend] = {
    "rfsim": RfsimBackend(),
    "r2lab": R2LabBackend(),
}
_CLI_BACKENDS: dict[str, BackendName] = {
    "doctor": "rfsim",
    "network": "rfsim",
    "r2lab": "r2lab",
}


def get_backend(name: BackendName) -> Backend:
    return _BACKENDS[name]


def backend_for_argv(argv: Sequence[str]) -> Backend | None:
    if not argv:
        return None
    name = _CLI_BACKENDS.get(argv[0])
    return None if name is None else get_backend(name)


def configure_backend_parser(parser: argparse.ArgumentParser) -> None:
    """Let each backend extend the one operator parser in place."""

    for backend in _BACKENDS.values():
        backend.configure_parser(parser)


__all__ = [
    "Backend",
    "BackendContract",
    "BackendError",
    "BackendName",
    "LIFECYCLE_STAGES",
    "LifecycleStage",
    "R2LabBackend",
    "RadioMode",
    "RfsimBackend",
    "backend_for_argv",
    "configure_backend_parser",
    "get_backend",
]
