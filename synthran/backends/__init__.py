"""Backend-neutral run exports."""

from synthran.backends.base import BackendError
from synthran.backends.unified_run import RunCommandAdapter


__all__ = ["BackendError", "RunCommandAdapter"]
