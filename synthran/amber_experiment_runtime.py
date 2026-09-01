"""Compatibility import surface for the current RFSIM Amber experiment.

The implementation lives under :mod:`synthran.experiment.rfsim`; this module
keeps the public imports used by the research layer without retaining a second
runtime implementation.
"""

from synthran.experiment.rfsim import (
    AmberMeasurementLifecycle,
    AmberRuntimeContext,
    execute_rfsim_amber_experiment,
)


execute_amber_experiment = execute_rfsim_amber_experiment

__all__ = (
    "AmberMeasurementLifecycle",
    "AmberRuntimeContext",
    "execute_amber_experiment",
)
