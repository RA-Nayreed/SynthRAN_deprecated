"""Virtual RFSIM backend adapter."""

from __future__ import annotations

import argparse

from synthran import command_runtime
from synthran.backends.base import (
    BackendContract,
    BackendError,
    LIFECYCLE_STAGES,
)
from synthran.dependencies import DependencyError
from synthran.fiveg_ansible import FiveGAnsibleError
from synthran.live_preflight import LivePreflightError
from synthran.network.resources import ResourcePreparationError
from synthran.network.runtime import NetworkRuntimeError
from synthran.slices_controller import SlicesControllerError


class RfsimBackend:
    contract = BackendContract(
        name="rfsim",
        radio_mode="virtual",
        implemented_stages=LIFECYCLE_STAGES,
    )

    def dispatch(self, args: argparse.Namespace) -> int:
        try:
            if args.command == "doctor":
                return command_runtime._doctor(args)
            if args.command == "network" and args.network_command == "prepare":
                return command_runtime._network_prepare(args)
            if args.command == "network" and args.network_command == "deploy":
                return command_runtime._network_deploy(args)
            if args.command == "network" and args.network_command == "verify":
                return command_runtime._network_verify(args)
        except (
            DependencyError,
            FiveGAnsibleError,
            LivePreflightError,
            NetworkRuntimeError,
            ResourcePreparationError,
            SlicesControllerError,
            OSError,
        ) as exc:
            raise BackendError(str(exc)) from exc
        raise BackendError("unsupported RFSIM command")
