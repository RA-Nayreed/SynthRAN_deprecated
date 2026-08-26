"""Small process-environment helpers with exact restoration semantics."""

from __future__ import annotations

from contextlib import contextmanager
import os
from typing import Iterator, Mapping


@contextmanager
def scoped_environment(updates: Mapping[str, str | None]) -> Iterator[None]:
    """Apply environment changes for one call boundary and restore them exactly."""

    previous = {name: os.environ.get(name) for name in updates}
    present = {name: name in os.environ for name in updates}
    try:
        for name, value in updates.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value
        yield
    finally:
        for name in updates:
            if present[name]:
                value = previous[name]
                assert value is not None
                os.environ[name] = value
            else:
                os.environ.pop(name, None)
