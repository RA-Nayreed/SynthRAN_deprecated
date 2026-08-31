"""Shared public error boundary for SynthRAN orchestration."""

from __future__ import annotations


class SynthRANError(RuntimeError):
    """A SynthRAN operator command could not be completed safely."""
