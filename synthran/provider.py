"""Shared SLICES provider selection for every SynthRAN radio backend."""

from __future__ import annotations

import argparse
import re
from pathlib import Path

from synthran.backends.base import BackendError
from synthran.dependencies import load_lock
from synthran.slices_controller import (
    SlicesControllerError,
    subprocess_runner as slices_runner,
    verify_slices_controller,
)


_SLICES_DURATION_RE = re.compile(r"^[1-9][0-9]*(?:m|h)$")


def ensure_slices_provider_context(args: argparse.Namespace) -> tuple[str, str, bool, object]:
    """Select or create the provider experiment used by one unified run."""

    project = getattr(args, "slices_project", None)
    if not project:
        raise BackendError(
            "run requires --slices-project or SYNTHRAN_SLICES_PROJECT"
        )

    experiment = getattr(args, "slices_experiment", None) or str(args.run_id)
    duration = str(getattr(args, "slices_duration", "4h"))
    if _SLICES_DURATION_RE.fullmatch(duration) is None:
        raise BackendError("SLICES experiment duration must look like 30m or 4h")

    timeout = min(max(int(args.timeout), 60), 300)
    selected = slices_runner(("slices", "project", "use", str(project)), timeout)
    if selected.returncode != 0:
        raise SlicesControllerError("SLICES project selection failed")

    shown = slices_runner(("slices", "experiment", "show", experiment), timeout)
    created = False
    if shown.returncode != 0:
        created_result = slices_runner(
            ("slices", "experiment", "create", experiment, "--duration", duration),
            timeout,
        )
        if created_result.returncode != 0:
            raise SlicesControllerError("SLICES experiment creation failed")
        created = True

    prefix = slices_runner(("post5g", "experiment", "prefix", experiment), timeout)
    if prefix.returncode != 0:
        raise SlicesControllerError("Post5G prefix acquisition failed")

    report = verify_slices_controller(
        lock=load_lock(Path(args.lock)),
        project=str(project),
        experiment=experiment,
        timeout_seconds=timeout,
    )
    if report.post5g_network is None:
        raise SlicesControllerError("Post5G provider network was not verified")
    return str(project), experiment, created, report
