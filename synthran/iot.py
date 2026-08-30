"""Compatibility boundary for retired executable sensor rendering."""

from __future__ import annotations

from pathlib import Path

from synthran.experiment import ExperimentError, ExperimentScenario


def write_run_inputs(
    scenario: ExperimentScenario,
    *,
    run_directory: Path,
    contiki_directory: Path,
) -> tuple[Path, Path, Path]:
    """Reject attempts to invoke the removed executable source renderer."""

    del scenario, run_directory, contiki_directory
    raise ExperimentError(
        "the previous executable sensor renderer has been removed; use the portable IoT source"
    )
