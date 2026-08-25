"""Physical R2Lab backend adapter."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from synthran import command_runtime
from synthran.backends.base import BackendContract, BackendError, LIFECYCLE_STAGES
from synthran.dependencies import DependencyError
from synthran.experiment import ExperimentError
from synthran.experiment.runtime import (
    DEFAULT_COLLECTION_SECONDS,
    DEFAULT_MINIMUM_PER_SENSOR,
)
from synthran.fiveg_ansible import FiveGAnsibleError
from synthran.privacy import PrivacyError
from synthran.r2lab.controller import R2LabResourceError
from synthran.r2lab.foundation import R2LabPhysicalFoundationError
from synthran.r2lab.gnb import R2LabPhysicalGnbError
from synthran.r2lab.lifecycle import (
    R2LabPhysicalLifecycleError,
    continue_physical_path,
    run_physical_workload,
)
from synthran.slices_controller import SlicesControllerError


def _subparsers(parser: argparse.ArgumentParser) -> argparse._SubParsersAction:
    for action in parser._actions:
        if isinstance(action, argparse._SubParsersAction):
            return action
    raise BackendError("SynthRAN parser does not expose backend subcommands")


def _require_path_context(args: argparse.Namespace) -> tuple[str, Path]:
    slice_name = getattr(args, "r2lab_slice", None)
    known_hosts = getattr(args, "known_hosts", None)
    missing = []
    if not slice_name:
        missing.append("--slice or SYNTHRAN_R2LAB_SLICE")
    if known_hosts is None:
        missing.append("--known-hosts or SYNTHRAN_SLICES_KNOWN_HOSTS")
    if missing:
        raise BackendError("physical path requires " + ", ".join(missing))
    return str(slice_name), Path(known_hosts)


class R2LabBackend:
    contract = BackendContract(
        name="r2lab",
        radio_mode="physical",
        implemented_stages=LIFECYCLE_STAGES,
    )

    def configure_parser(self, parser: argparse.ArgumentParser) -> None:
        root = _subparsers(parser)
        r2lab = root.choices.get("r2lab")
        if r2lab is None:
            raise BackendError("SynthRAN parser is missing the R2Lab command group")
        commands = _subparsers(r2lab)
        if "path-up" not in commands.choices:
            path_up = commands.add_parser(
                "path-up",
                help="advance a proven physical gNB through UE, PDU, and user-plane proof",
            )
            path_up.add_argument(
                "--slice",
                dest="r2lab_slice",
                default=os.environ.get("SYNTHRAN_R2LAB_SLICE"),
            )
            path_up.add_argument(
                "--known-hosts",
                type=Path,
                default=os.environ.get("SYNTHRAN_SLICES_KNOWN_HOSTS"),
            )
            path_up.add_argument("--run-id", required=True)
            path_up.add_argument("--peer", required=True)
            path_up.add_argument("--timeout", type=int, default=30)
            path_up.add_argument("--json", action="store_true")
            path_up.add_argument(
                "--run-root",
                type=Path,
                default=Path(".synthran/r2lab"),
                help=argparse.SUPPRESS,
            )

        if "workload-run" not in commands.choices:
            workload = commands.add_parser(
                "workload-run",
                help="run the canonical ten-sensor workload through an accepted physical path",
            )
            workload.add_argument(
                "--slice",
                dest="r2lab_slice",
                default=os.environ.get("SYNTHRAN_R2LAB_SLICE"),
            )
            workload.add_argument(
                "--known-hosts",
                type=Path,
                default=os.environ.get("SYNTHRAN_SLICES_KNOWN_HOSTS"),
            )
            workload.add_argument("--run-id", required=True)
            workload.add_argument("--workload-id", required=True)
            workload.add_argument("--inventory", type=Path, required=True)
            workload.add_argument(
                "--lock", type=Path, default=Path("dependencies.lock.yml")
            )
            workload.add_argument("--deps-root", type=Path, default=Path(".deps"))
            workload.add_argument(
                "--collection-seconds",
                type=int,
                default=DEFAULT_COLLECTION_SECONDS,
            )
            workload.add_argument(
                "--minimum-per-sensor",
                type=int,
                default=DEFAULT_MINIMUM_PER_SENSOR,
            )
            workload.add_argument("--timeout", type=int, default=30)
            workload.add_argument("--json", action="store_true")
            workload.add_argument(
                "--run-root",
                type=Path,
                default=Path(".synthran/r2lab"),
                help=argparse.SUPPRESS,
            )
            workload.add_argument(
                "--experiment-root",
                type=Path,
                default=Path(".synthran/experiments-r2lab"),
                help=argparse.SUPPRESS,
            )

    def dispatch(self, args: argparse.Namespace) -> int:
        if args.command != "r2lab":
            raise BackendError("unsupported R2Lab command")
        try:
            if args.r2lab_command == "path-up":
                slice_name, known_hosts = _require_path_context(args)
                result = continue_physical_path(
                    run_id=args.run_id,
                    slice_name=slice_name,
                    known_hosts=known_hosts,
                    peer=args.peer,
                    run_root=args.run_root,
                    timeout_seconds=args.timeout,
                )
                if args.json:
                    print(json.dumps(result.to_dict(), indent=2, sort_keys=True))
                else:
                    print(
                        "Physical UE/PDU/user plane ready for workload."
                        if result.ready_for_workload
                        else "Physical UE/PDU/user plane was not proven."
                    )
                    print(f"Sanitized evidence: {result.evidence_path}")
                return 0 if result.ready_for_workload else 2

            if args.r2lab_command == "workload-run":
                slice_name, known_hosts = _require_path_context(args)
                result = run_physical_workload(
                    run_id=args.run_id,
                    workload_id=args.workload_id,
                    slice_name=slice_name,
                    known_hosts=known_hosts,
                    inventory_path=args.inventory,
                    lock_path=args.lock,
                    deps_root=args.deps_root,
                    run_root=args.run_root,
                    experiment_root=args.experiment_root,
                    collection_seconds=args.collection_seconds,
                    minimum_per_sensor=args.minimum_per_sensor,
                    timeout_seconds=args.timeout,
                )
                if args.json:
                    print(json.dumps(result.to_dict(), indent=2, sort_keys=True))
                else:
                    print(
                        "Physical deterministic workload accepted."
                        if result.accepted
                        else "Physical deterministic workload was not accepted."
                    )
                    print(f"Sanitized evidence: {result.workload_result_path}")
                return 0 if result.accepted else 2

            return command_runtime._dispatch_r2lab(args)
        except (
            DependencyError,
            ExperimentError,
            FiveGAnsibleError,
            PrivacyError,
            R2LabPhysicalFoundationError,
            R2LabPhysicalGnbError,
            R2LabPhysicalLifecycleError,
            R2LabResourceError,
            SlicesControllerError,
            OSError,
        ) as exc:
            raise BackendError(str(exc)) from exc
