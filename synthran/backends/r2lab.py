"""Physical R2Lab backend adapter."""

from __future__ import annotations

import argparse

from synthran import command_runtime
from synthran.backends.base import (
    BackendContract,
    BackendError,
    LIFECYCLE_STAGES,
    LifecycleStage,
)
from synthran.dependencies import DependencyError
from synthran.fiveg_ansible import FiveGAnsibleError
from synthran.privacy import PrivacyError
from synthran.r2lab.controller import R2LabResourceError
from synthran.r2lab.foundation import R2LabPhysicalFoundationError
from synthran.r2lab.gnb import R2LabPhysicalGnbError
from synthran.slices_controller import SlicesControllerError


_IMPLEMENTED_STAGES = LIFECYCLE_STAGES[
    : LIFECYCLE_STAGES.index(LifecycleStage.N2) + 1
]


class R2LabBackend:
    contract = BackendContract(
        name="r2lab",
        radio_mode="physical",
        implemented_stages=_IMPLEMENTED_STAGES,
    )

    def dispatch(self, args: argparse.Namespace) -> int:
        if args.command != "r2lab":
            raise BackendError("unsupported R2Lab command")
        try:
            return command_runtime._dispatch_r2lab(args)
        except (
            DependencyError,
            FiveGAnsibleError,
            PrivacyError,
            R2LabPhysicalFoundationError,
            R2LabPhysicalGnbError,
            R2LabResourceError,
            SlicesControllerError,
            OSError,
        ) as exc:
            raise BackendError(str(exc)) from exc
