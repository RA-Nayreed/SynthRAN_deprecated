"""Shared backend error boundary."""

from __future__ import annotations


class BackendError(RuntimeError):
    """A backend command could not be completed through its integration boundary."""
