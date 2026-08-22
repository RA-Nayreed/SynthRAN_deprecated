"""Compatibility import for the cohesive :mod:`synthran.r2lab` backend.

New implementation code lives in ``synthran.r2lab``. This module remains only
because the CLI and existing callers use ``synthran.network.r2lab`` as the
stable public surface.
"""

from synthran.r2lab.controller import *  # noqa: F403
from synthran.r2lab.runtime import *  # noqa: F403
