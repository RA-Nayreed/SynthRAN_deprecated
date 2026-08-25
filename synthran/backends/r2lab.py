"""Physical R2Lab backend adapter."""

from __future__ import annotations

import argparse
import json
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
from synthran.r2lab.acceptance import PhysicalRunEvidence, R2LabAcceptanceError
from synthran.r2lab.controller import gateway_command, subprocess_runner as r2lab_runner
from synthran.r2lab.foundation_topology import (
    R2LabTopologyFoundationError,
    accept_topology_foundation,
)
from synthran.r2lab.hardware import (
    RADIOS,
    UES,
    PhysicalTopology,
    R2LabHardwareError,
    capabilities,
)
from synthran.r2lab.lifecycle import (
    R2LabPhysicalLifecycleError,
    continue_physical_path,
    run_physical_workload,
)
from synthran.r2lab.n3xx import (
    R2LabN3xxError,
    stage_n3xx_gnb,
    start_n3xx_gnb,
    stop_n3xx_gnb,
)
from synthran.r2lab.resources import (
    R2LabTopologyResourceError,
    prepare_physical_resources,
    release_physical_resources,
)
from synthran.r2lab.ue import R2LabPhysicalUeError
from synthran.slices_controller import SlicesControllerError


_EXECUTABLE_RADIOS = tuple(
    sorted(name for name, profile in RADIOS.items() if profile.executable)
)
_EXECUTABLE_UES = tuple(
    sorted(name for name, profile in UES.items() if profile.executable)
)


def _subparsers(parser: argparse.ArgumentParser) -> argparse._SubParsersAction:
    for action in parser._actions:
        if isinstance(action, argparse._SubParsersAction):
            return action
    raise BackendError("SynthRAN parser does not expose backend subcommands")


def _action(parser: argparse.ArgumentParser, dest: str):
    return next((item for item in parser._actions if item.dest == dest), None)


def _set_choices(parser: argparse.ArgumentParser, dest: str, choices: tuple[str, ...]) -> None:
    action = _action(parser, dest)
    if action is None:
        raise BackendError(f"R2Lab parser is missing {dest}")
    action.choices = choices


def _add_if_missing(parser: argparse.ArgumentParser, *flags: str, **kwargs) -> None:
    dest = kwargs.get("dest")
    if dest is None:
        dest = flags[-1].lstrip("-").replace("-", "_")
    if _action(parser, str(dest)) is None:
        parser.add_argument(*flags, **kwargs)


def _require_slice(args: argparse.Namespace) -> str:
    slice_name = getattr(args, "r2lab_slice", None)
    if not slice_name:
        raise BackendError("R2Lab command requires --slice or SYNTHRAN_R2LAB_SLICE")
    return str(slice_name)


def _require_mutation_context(
    args: argparse.Namespace,
) -> tuple[str, str, str | None, Path]:
    slice_name = _require_slice(args)
    owner = getattr(args, "owner", None)
    known_hosts = getattr(args, "known_hosts", None)
    missing: list[str] = []
    if not owner:
        missing.append("--owner or SYNTHRAN_OWNER")
    if known_hosts is None:
        missing.append("--known-hosts or SYNTHRAN_SLICES_KNOWN_HOSTS")
    if missing:
        raise BackendError("physical mutation requires " + ", ".join(missing))
    return (
        slice_name,
        str(owner),
        getattr(args, "allocation_id", None),
        Path(known_hosts),
    )


def _topology_from_args(args: argparse.Namespace) -> PhysicalTopology:
    missing = [
        flag
        for flag, value in (
            ("--core-node", getattr(args, "core_node", None)),
            ("--ran-node", getattr(args, "ran_node", None)),
            ("--radio", getattr(args, "radio", None)),
            ("--ue", getattr(args, "ue", None)),
        )
        if not value
    ]
    if missing:
        raise BackendError("physical topology requires " + ", ".join(missing))
    return PhysicalTopology(
        core_node=str(args.core_node),
        ran_node=str(args.ran_node),
        radio=str(args.radio),
        ue=str(args.ue),
    ).validate()


def _doctor(args: argparse.Namespace) -> int:
    slice_name = _require_slice(args)
    radio = RADIOS.get(str(args.radio))
    ue = UES.get(str(args.ue))
    selection_ok = (
        radio is not None
        and radio.executable
        and ue is not None
        and ue.executable
        and ue.is_fr1_quectel
    )
    checks: list[dict[str, object]] = [
        {
            "name": "selection",
            "passed": selection_ok,
            "detail": "executable FR1 physical selection"
            if selection_ok
            else "selection is outside the executable N3xx/FR1 backend",
        }
    ]
    if selection_ok:
        gateway = r2lab_runner(gateway_command(slice_name, "true"), args.timeout)
        gateway_ok = gateway.returncode == 0
        checks.append(
            {
                "name": "gateway",
                "passed": gateway_ok,
                "detail": "strict public-key SSH to Faraday"
                if gateway_ok
                else "strict public-key SSH to Faraday failed",
            }
        )
        if gateway_ok:
            lease = r2lab_runner(
                gateway_command(slice_name, "rhubarbe", "leases", "--check"),
                args.timeout,
            )
            checks.append(
                {
                    "name": "lease",
                    "passed": lease.returncode == 0,
                    "detail": "active R2Lab lease verified"
                    if lease.returncode == 0
                    else "active R2Lab lease not verified",
                }
            )
    ready = bool(checks) and all(item["passed"] is True for item in checks)
    payload: dict[str, object] = {
        "schema": "synthran/r2lab-doctor/v1alpha2",
        "ready": ready,
        "radio": args.radio,
        "ue": args.ue,
        "checks": checks,
    }
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print("SynthRAN R2Lab doctor (read-only)")
        for check in checks:
            print(
                f"[{'PASS' if check['passed'] else 'FAIL'}] "
                f"{check['name']}: {check['detail']}"
            )
        print(f"Result: {'READY' if ready else 'NOT READY'}")
    return 0 if ready else 2


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

        for name in ("doctor", "plan", "prepare"):
            existing = commands.choices.get(name)
            if existing is None:
                raise BackendError(f"SynthRAN parser is missing r2lab {name}")
            _set_choices(existing, "radio", _EXECUTABLE_RADIOS)
            _set_choices(existing, "ue", _EXECUTABLE_UES)

        for name in ("plan", "prepare"):
            existing = commands.choices[name]
            _add_if_missing(existing, "--core-node", required=True)
            _add_if_missing(existing, "--ran-node", required=True)

        foundation = commands.choices.get("foundation")
        if foundation is None:
            raise BackendError("SynthRAN parser is missing r2lab foundation")
        previous = _action(foundation, "previous_run_id")
        if previous is not None:
            previous.required = False

        stage = commands.choices.get("gnb-stage")
        if stage is None:
            raise BackendError("SynthRAN parser is missing r2lab gnb-stage")
        for dest in (
            "amf_n2_address",
            "gnb_n2_address",
            "n300_address",
            "ru_pod_address",
            "ru_subnet",
        ):
            action = _action(stage, dest)
            if action is not None:
                action.help = argparse.SUPPRESS

        if "capabilities" not in commands.choices:
            capabilities_parser = commands.add_parser(
                "capabilities",
                help="show R2Lab hardware inventory and executable backend capabilities",
            )
            capabilities_parser.add_argument("--json", action="store_true")

        if "path-up" not in commands.choices:
            path_up = commands.add_parser(
                "path-up",
                help="advance the selected physical UE through PDU and user-plane proof",
            )
            command_runtime._add_physical_authority_arguments(path_up)
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
                help="run the canonical ten-sensor workload through the selected physical path",
            )
            command_runtime._add_physical_authority_arguments(workload)
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

        release = commands.choices.get("release")
        if release is not None:
            _add_if_missing(release, "--json", action="store_true")

    def dispatch(self, args: argparse.Namespace) -> int:
        if args.command != "r2lab":
            raise BackendError("unsupported R2Lab command")
        try:
            if args.r2lab_command == "capabilities":
                print(json.dumps(capabilities(), indent=2, sort_keys=True))
                return 0

            if args.r2lab_command == "doctor":
                return _doctor(args)

            if args.r2lab_command == "plan":
                topology = _topology_from_args(args)
                payload = {
                    "schema": "synthran/r2lab-plan/v1alpha2",
                    "execution_enabled": False,
                    "run_id": args.run_id,
                    "backend": "r2lab",
                    "topology": topology.to_dict(),
                    "lease_action": "reuse-active",
                    "cleanup": "exact selected radio/UE and run-owned gNB only",
                    "global_power_off": False,
                }
                if args.json:
                    print(json.dumps(payload, indent=2, sort_keys=True))
                else:
                    print("SynthRAN R2Lab topology plan (NON-EXECUTING)")
                    print(
                        f"Compute: {topology.core_node} (core) + {topology.ran_node} (RAN)"
                    )
                    print(f"Radio: {topology.radio}")
                    print(
                        f"UE: {topology.ue} ({topology.ue_profile.kind}, {topology.ue_profile.mode})"
                    )
                    print("Lease: require and reuse the active R2Lab lease")
                    print("Cleanup: exact selected resources only")
                return 0

            if args.r2lab_command == "prepare":
                topology = _topology_from_args(args)
                result = prepare_physical_resources(
                    run_id=args.run_id,
                    slice_name=_require_slice(args),
                    topology=topology,
                    run_root=args.run_root,
                    timeout_seconds=args.timeout,
                )
                print(json.dumps(result.to_dict(), indent=2, sort_keys=True))
                return 0

            if args.r2lab_command == "foundation":
                slice_name, owner, allocation_id, known_hosts = _require_mutation_context(args)
                result = accept_topology_foundation(
                    run_id=args.run_id,
                    previous_run_id=getattr(args, "previous_run_id", None),
                    slice_name=slice_name,
                    owner=owner,
                    allocation_id=allocation_id,
                    known_hosts=known_hosts,
                    run_root=args.run_root,
                    lock_path=args.lock,
                    dependency_root=args.deps_root,
                    timeout_seconds=args.timeout,
                )
                payload = result.to_dict()
                if args.json:
                    print(json.dumps(payload, indent=2, sort_keys=True))
                else:
                    print("Selected physical foundation ready.")
                    print(f"Evidence: {result.evidence_path}")
                return 0

            if args.r2lab_command == "gnb-stage":
                legacy_overrides = {
                    dest: getattr(args, dest, None)
                    for dest in (
                        "amf_n2_address",
                        "gnb_n2_address",
                        "n300_address",
                        "ru_pod_address",
                        "ru_subnet",
                    )
                }
                if any(value is not None for value in legacy_overrides.values()):
                    raise BackendError(
                        "physical gNB bindings are derived from the selected pinned radio profile; manual legacy overrides are not supported"
                    )
                slice_name, owner, allocation_id, known_hosts = _require_mutation_context(args)
                artifact = stage_n3xx_gnb(
                    run_id=args.run_id,
                    slice_name=slice_name,
                    owner=owner,
                    allocation_id=allocation_id,
                    known_hosts=known_hosts,
                    lock_path=args.lock,
                    deps_root=args.deps_root,
                    run_root=args.run_root,
                    timeout_seconds=args.timeout,
                )
                payload = artifact.to_dict()
                if args.json:
                    print(json.dumps(payload, indent=2, sort_keys=True))
                else:
                    print(f"{artifact.radio} gNB staged at zero pods.")
                return 0

            if args.r2lab_command == "gnb-start":
                slice_name, owner, allocation_id, known_hosts = _require_mutation_context(args)
                result = start_n3xx_gnb(
                    run_id=args.run_id,
                    slice_name=slice_name,
                    owner=owner,
                    allocation_id=allocation_id,
                    known_hosts=known_hosts,
                    run_root=args.run_root,
                    timeout_seconds=args.timeout,
                    required_consecutive_proofs=args.n2_attempts,
                    convergence_attempts=args.n2_convergence_attempts,
                    poll_interval_seconds=args.n2_interval,
                )
                payload = result.to_dict()
                if args.json:
                    print(json.dumps(payload, indent=2, sort_keys=True))
                else:
                    print(f"{result.radio} singleton gNB/N2 ready.")
                    print(f"Evidence: {result.evidence_path}")
                return 0

            if args.r2lab_command == "path-up":
                slice_name, owner, allocation_id, known_hosts = _require_mutation_context(args)
                result = continue_physical_path(
                    run_id=args.run_id,
                    slice_name=slice_name,
                    owner=owner,
                    allocation_id=allocation_id,
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
                slice_name, owner, allocation_id, known_hosts = _require_mutation_context(args)
                result = run_physical_workload(
                    run_id=args.run_id,
                    workload_id=args.workload_id,
                    slice_name=slice_name,
                    owner=owner,
                    allocation_id=allocation_id,
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

            if args.r2lab_command == "release":
                slice_name, owner, allocation_id, known_hosts = _require_mutation_context(args)
                evidence_path = args.run_root.expanduser().resolve() / args.run_id / "physical-run.json"
                stop = None
                if evidence_path.is_file():
                    evidence = PhysicalRunEvidence.read_json(evidence_path)
                    if evidence.gnb_start is not None:
                        stop = lambda: stop_n3xx_gnb(
                            run_id=args.run_id,
                            slice_name=slice_name,
                            owner=owner,
                            allocation_id=allocation_id,
                            known_hosts=known_hosts,
                            run_root=args.run_root,
                            timeout_seconds=max(args.timeout, 30),
                        )
                payload = release_physical_resources(
                    run_id=args.run_id,
                    slice_name=slice_name,
                    run_root=args.run_root,
                    timeout_seconds=args.timeout,
                    stop_gnb=stop,
                )
                if getattr(args, "json", False):
                    print(json.dumps(payload, indent=2, sort_keys=True))
                else:
                    print("Selected physical resources released with exact off-state proof.")
                return 0

            return command_runtime._dispatch_r2lab(args)
        except (
            DependencyError,
            ExperimentError,
            FiveGAnsibleError,
            PrivacyError,
            R2LabAcceptanceError,
            R2LabHardwareError,
            R2LabN3xxError,
            R2LabPhysicalLifecycleError,
            R2LabPhysicalUeError,
            R2LabTopologyFoundationError,
            R2LabTopologyResourceError,
            SlicesControllerError,
            OSError,
            ValueError,
        ) as exc:
            raise BackendError(str(exc)) from exc
