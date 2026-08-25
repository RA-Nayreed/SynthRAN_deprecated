"""Backend-neutral operator commands for SynthRAN."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys
import tempfile
import time
from typing import Iterable

from synthran import command_runtime
from synthran.backends.base import BackendError
from synthran.backends.run import RunCommandAdapter, configure_run_parser
from synthran.dependencies import DependencyError, load_lock
from synthran.fiveg_ansible import FiveGAnsibleError, run_offline_doctor
from synthran.network.resources import SUPPORTED_NODES, build_preparation_inventory
from synthran.privacy import PrivacyError, redact_file
from synthran.r2lab.acceptance import PhysicalRunEvidence
from synthran.r2lab.controller import gateway_command, subprocess_runner as r2lab_runner
from synthran.r2lab.hardware import RADIOS, UES, PhysicalTopology, capabilities
from synthran.r2lab.n3xx import stop_n3xx_gnb
from synthran.r2lab.resources import load_topology, release_physical_resources
from synthran.slices_controller import SlicesControllerError, verify_slices_controller


PUBLIC_COMMANDS = (
    "run",
    "doctor",
    "inspect",
    "logs",
    "stop",
    "research",
    "deps",
    "dev",
)

_EXECUTABLE_DEVICES = tuple(sorted(name for name, profile in RADIOS.items() if profile.executable))
_EXECUTABLE_UES = tuple(sorted(name for name, profile in UES.items() if profile.executable))


def _subparsers(parser: argparse.ArgumentParser) -> argparse._SubParsersAction:
    for action in parser._actions:
        if isinstance(action, argparse._SubParsersAction):
            return action
    raise BackendError("SynthRAN parser does not expose top-level commands")


def _add_provider_context(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--slices-project",
        default=os.environ.get("SYNTHRAN_SLICES_PROJECT"),
        help="SLICES project (or SYNTHRAN_SLICES_PROJECT)",
    )
    parser.add_argument(
        "--slices-experiment",
        default=os.environ.get("SYNTHRAN_SLICES_EXPERIMENT"),
        help="existing provider experiment to verify",
    )


def configure_operator_parser(parser: argparse.ArgumentParser) -> None:
    commands = _subparsers(parser)
    configure_run_parser(parser)

    doctor = commands.add_parser("doctor", help="run read-only readiness checks")
    doctor.add_argument("--radio", required=True, choices=("rfsim", "r2lab"))
    doctor.add_argument("--core-node", default="sopnode-f2", choices=tuple(sorted(SUPPORTED_NODES)))
    doctor.add_argument("--ran-node", default="sopnode-f3", choices=tuple(sorted(SUPPORTED_NODES)))
    doctor.add_argument("--device", choices=_EXECUTABLE_DEVICES)
    doctor.add_argument("--ue", choices=_EXECUTABLE_UES)
    doctor.add_argument(
        "--slice",
        dest="r2lab_slice",
        default=os.environ.get("SYNTHRAN_R2LAB_SLICE"),
    )
    _add_provider_context(doctor)
    doctor.add_argument("--lock", type=Path, default=Path("dependencies.lock.yml"))
    doctor.add_argument("--deps-root", type=Path, default=Path(".deps"))
    doctor.add_argument("--timeout", type=int, default=60)
    doctor.add_argument("--json", action="store_true")

    inspect = commands.add_parser("inspect", help="show capabilities or persisted run evidence")
    inspect.add_argument("--radio", choices=("rfsim", "r2lab"))
    inspect.add_argument("--run-id")
    inspect.add_argument("--json", action="store_true")

    logs = commands.add_parser("logs", help="show the unified run event stream")
    logs.add_argument("--run-id", required=True)
    logs.add_argument("--follow", action="store_true")
    logs.add_argument("--tail", type=int, default=200)

    stop = commands.add_parser("stop", help="stop or release resources owned by one run")
    stop.add_argument("--run-id", required=True)
    stop.add_argument(
        "--slice",
        dest="r2lab_slice",
        default=os.environ.get("SYNTHRAN_R2LAB_SLICE"),
    )
    stop.add_argument("--owner", default=os.environ.get("SYNTHRAN_OWNER"))
    stop.add_argument("--allocation-id", default=os.environ.get("SYNTHRAN_ALLOCATION_ID"))
    stop.add_argument(
        "--known-hosts",
        type=Path,
        default=os.environ.get("SYNTHRAN_SLICES_KNOWN_HOSTS"),
    )
    stop.add_argument("--timeout", type=int, default=300)
    stop.add_argument("--json", action="store_true")

    command_runtime._add_research_parser(commands)

    deps = commands.add_parser("deps", help="manage immutable external dependencies")
    deps_commands = deps.add_subparsers(dest="deps_command", required=True)
    sync = deps_commands.add_parser("sync", help="synchronize pinned checkouts")
    sync.add_argument("--lock", type=Path, default=Path("dependencies.lock.yml"))
    sync.add_argument("--root", type=Path, default=Path(".deps"))
    selection = sync.add_mutually_exclusive_group()
    selection.add_argument("--all", action="store_true")
    selection.add_argument("--name", action="append", dest="dependency_names")
    sync.add_argument("--dry-run", action="store_true")

    dev = commands.add_parser("dev", help="repository maintenance commands")
    dev_commands = dev.add_subparsers(dest="dev_command", required=True)
    privacy = dev_commands.add_parser("privacy", help="scan or redact repository text")
    privacy_commands = privacy.add_subparsers(dest="privacy_command", required=True)
    scan = privacy_commands.add_parser("scan", help="scan for sensitive context")
    scan_mode = scan.add_mutually_exclusive_group()
    scan_mode.add_argument("--worktree", action="store_true")
    scan_mode.add_argument("--history", action="store_true")
    scan_mode.add_argument("--outgoing", action="store_true")
    scan.add_argument("--remote", default="origin")
    redact = privacy_commands.add_parser("redact", help="write a sanitized derivative")
    redact.add_argument("source", type=Path)
    redact.add_argument("destination", type=Path)
    redact.add_argument("--dry-run", action="store_true")
    hooks = dev_commands.add_parser("hooks", help="configure repository hooks")
    hooks_commands = hooks.add_subparsers(dest="hooks_command", required=True)
    install = hooks_commands.add_parser("install", help="activate tracked hooks")
    install.add_argument("--dry-run", action="store_true")


def _doctor_r2lab(args: argparse.Namespace) -> int:
    if args.device is None or args.ue is None:
        raise BackendError("doctor --radio r2lab requires --device and --ue")
    if not args.r2lab_slice:
        raise BackendError("doctor --radio r2lab requires --slice or SYNTHRAN_R2LAB_SLICE")
    topology = PhysicalTopology(
        core_node=args.core_node,
        ran_node=args.ran_node,
        radio=args.device,
        ue=args.ue,
    ).validate()
    gateway = r2lab_runner(gateway_command(args.r2lab_slice, "true"), args.timeout)
    lease = None
    if gateway.returncode == 0:
        lease = r2lab_runner(
            gateway_command(args.r2lab_slice, "rhubarbe", "leases", "--check"),
            args.timeout,
        )
    checks = [
        {"name": "selection", "passed": True, "detail": topology.to_dict()},
        {
            "name": "gateway",
            "passed": gateway.returncode == 0,
            "detail": "strict public-key SSH to Faraday",
        },
        {
            "name": "lease",
            "passed": lease is not None and lease.returncode == 0,
            "detail": "active R2Lab lease",
        },
    ]
    ready = all(check["passed"] is True for check in checks)
    payload = {"schema": "synthran/doctor/v1", "radio": "r2lab", "ready": ready, "checks": checks}
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print("SynthRAN doctor (r2lab)")
        for check in checks:
            print(f"[{'PASS' if check['passed'] else 'FAIL'}] {check['name']}: {check['detail']}")
        print(f"Result: {'READY' if ready else 'NOT READY'}")
    return 0 if ready else 2


def _doctor_rfsim(args: argparse.Namespace) -> int:
    if args.core_node == args.ran_node:
        raise BackendError("core and RAN nodes must differ")
    with tempfile.TemporaryDirectory(prefix="synthran-doctor-") as directory:
        inventory = Path(directory) / "hosts.ini"
        text, _ = build_preparation_inventory(
            core_node=args.core_node,
            ran_node=args.ran_node,
            source=inventory,
        )
        inventory.write_text(text, encoding="utf-8", newline="\n")
        report = run_offline_doctor(
            inventory_path=inventory,
            lock_path=args.lock,
            dependency_root=args.deps_root,
        )
    checks = [
        {"name": check.name, "passed": check.passed, "detail": check.detail}
        for check in report.checks
    ]
    if args.slices_experiment:
        if not args.slices_project:
            raise BackendError("provider verification requires --slices-project or SYNTHRAN_SLICES_PROJECT")
        provider = verify_slices_controller(
            lock=load_lock(args.lock),
            project=args.slices_project,
            experiment=args.slices_experiment,
            timeout_seconds=args.timeout,
        )
        checks.append(
            {
                "name": "provider",
                "passed": provider.ready,
                "detail": f"{args.slices_project}/{args.slices_experiment}",
            }
        )
    ready = bool(checks) and all(check["passed"] is True for check in checks)
    payload = {"schema": "synthran/doctor/v1", "radio": "rfsim", "ready": ready, "checks": checks}
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print("SynthRAN doctor (rfsim)")
        for check in checks:
            print(f"[{'PASS' if check['passed'] else 'FAIL'}] {check['name']}: {check['detail']}")
        print(f"Result: {'READY' if ready else 'NOT READY'}")
    return 0 if ready else 2


def doctor(args: argparse.Namespace) -> int:
    return _doctor_r2lab(args) if args.radio == "r2lab" else _doctor_rfsim(args)


def _candidate_evidence(run_id: str) -> tuple[Path, ...]:
    return (
        Path(".synthran/r2lab") / run_id / "physical-run.json",
        Path(".synthran/experiments-r2lab") / run_id / "experiment-evidence.json",
        Path(".synthran/experiments") / run_id / "experiment-evidence.json",
        Path(".synthran/runs") / run_id / "network-evidence.json",
        Path(".synthran/preparations") / run_id / "manifest.json",
    )


def inspect_command(args: argparse.Namespace) -> int:
    if args.run_id is None:
        if args.radio != "r2lab":
            raise BackendError("inspect without --run-id currently supports --radio r2lab capabilities")
        payload = capabilities()
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 0

    found: list[dict[str, object]] = []
    for path in _candidate_evidence(args.run_id):
        if not path.is_file():
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise BackendError(f"persisted run evidence is unreadable: {path}") from exc
        found.append({"path": str(path), "payload": payload})
    if not found:
        raise BackendError(f"no persisted evidence found for run {args.run_id}")
    if args.json:
        print(json.dumps({"run_id": args.run_id, "evidence": found}, indent=2, sort_keys=True))
    else:
        print(f"SynthRAN run {args.run_id}")
        for item in found:
            payload = item["payload"]
            schema = payload.get("schema") if isinstance(payload, dict) else None
            state = None
            if isinstance(payload, dict):
                state = payload.get("status", payload.get("ready", payload.get("accepted")))
            print(f"- {item['path']} :: {schema or 'unknown'} :: {state}")
    return 0


def _event_path(run_id: str) -> Path:
    return Path(".synthran/events") / f"{run_id}.jsonl"


def _render_events(lines: Iterable[str]) -> None:
    for line in lines:
        line = line.rstrip("\n")
        if not line:
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            print(line)
            continue
        if isinstance(payload, dict) and isinstance(payload.get("message"), str):
            stamp = payload.get("time", "")
            print(f"{stamp}  {payload['message']}".rstrip())
        else:
            print(line)


def logs_command(args: argparse.Namespace) -> int:
    path = _event_path(args.run_id)
    if not path.is_file():
        raise BackendError(f"no unified event stream found for run {args.run_id}")
    tail = max(1, args.tail)
    lines = path.read_text(encoding="utf-8").splitlines()
    _render_events(lines[-tail:])
    if not args.follow:
        return 0
    with path.open("r", encoding="utf-8") as stream:
        stream.seek(0, 2)
        try:
            while True:
                line = stream.readline()
                if line:
                    _render_events((line,))
                else:
                    time.sleep(0.5)
        except KeyboardInterrupt:
            return 0


def stop_command(args: argparse.Namespace) -> int:
    run_root = Path(".synthran/r2lab")
    run_directory = run_root / args.run_id
    if not run_directory.exists():
        payload = {
            "schema": "synthran/stop/v1",
            "run_id": args.run_id,
            "released": False,
            "detail": "no active physical claim found; virtual workloads clean up within their run",
        }
        print(json.dumps(payload, indent=2, sort_keys=True) if args.json else payload["detail"])
        return 0
    if not args.r2lab_slice or not args.owner or args.known_hosts is None:
        raise BackendError(
            "physical stop requires --slice/SYNTHRAN_R2LAB_SLICE, --owner/SYNTHRAN_OWNER, and --known-hosts/SYNTHRAN_SLICES_KNOWN_HOSTS"
        )
    known_hosts = Path(args.known_hosts).expanduser().resolve()
    if not known_hosts.is_file():
        raise BackendError("strict SLICES known-hosts file is missing")
    topology = load_topology(run_root=run_root, run_id=args.run_id).validate()
    evidence_path = run_directory / "physical-run.json"
    stop = None
    if evidence_path.is_file():
        evidence = PhysicalRunEvidence.read_json(evidence_path)
        if evidence.gnb_start is not None:
            stop = lambda: stop_n3xx_gnb(
                run_id=args.run_id,
                slice_name=args.r2lab_slice,
                owner=args.owner,
                allocation_id=args.allocation_id,
                known_hosts=known_hosts,
                run_root=run_root,
                timeout_seconds=max(args.timeout, 30),
            )
    payload = release_physical_resources(
        run_id=args.run_id,
        slice_name=args.r2lab_slice,
        run_root=run_root,
        timeout_seconds=min(args.timeout, 300),
        stop_gnb=stop,
    )
    result = {
        "schema": "synthran/stop/v1",
        "run_id": args.run_id,
        "radio": topology.radio,
        "ue": topology.ue,
        "released": True,
        "release": payload,
    }
    print(json.dumps(result, indent=2, sort_keys=True) if args.json else "Selected run resources released.")
    return 0


def dispatch(args: argparse.Namespace) -> int:
    try:
        if args.command == "run":
            return RunCommandAdapter().dispatch(args)
        if args.command == "doctor":
            return doctor(args)
        if args.command == "inspect":
            return inspect_command(args)
        if args.command == "logs":
            return logs_command(args)
        if args.command == "stop":
            return stop_command(args)
        if args.command == "research":
            return command_runtime._dispatch_research(args)
        if args.command == "deps":
            return command_runtime._deps_sync(args)
        if args.command == "dev" and args.dev_command == "privacy":
            if args.privacy_command == "scan":
                return command_runtime._privacy_scan(args)
            if args.privacy_command == "redact":
                redact_file(
                    args.source,
                    args.destination,
                    dry_run=args.dry_run,
                    output=sys.stdout,
                )
                return 0
        if args.command == "dev" and args.dev_command == "hooks" and args.hooks_command == "install":
            return command_runtime._hooks_install(args)
        raise BackendError("unsupported SynthRAN command")
    except (
        BackendError,
        DependencyError,
        FiveGAnsibleError,
        PrivacyError,
        SlicesControllerError,
        OSError,
        ValueError,
    ) as exc:
        raise BackendError(str(exc)) from exc
