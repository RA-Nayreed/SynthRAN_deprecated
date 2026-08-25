"""One sequential operator command across SynthRAN radio backends.

The public ``synthran run`` command owns orchestration. Existing step commands
remain recovery/debug primitives; this adapter composes them without duplicating
their implementation contracts.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import shlex
import sys
from typing import Mapping, TextIO

from synthran import command_runtime
from synthran.backends.base import BackendError
from synthran.backends.r2lab import _ensure_slices_provider_context
from synthran.dependencies import DependencyError
from synthran.experiment import ExperimentError
from synthran.experiment.runtime import DEFAULT_COLLECTION_SECONDS, DEFAULT_MINIMUM_PER_SENSOR
from synthran.fiveg_ansible import FiveGAnsibleError
from synthran.live_preflight import LivePreflightError, subprocess_runner as cluster_runner
from synthran.network.resources import SUPPORTED_NODES, ResourcePreparationError, build_preparation_inventory
from synthran.network.runtime import NetworkRuntimeError, validate_run_id
from synthran.privacy import PrivacyError
from synthran.r2lab.acceptance import PhysicalAcceptanceStage, PhysicalRunEvidence, R2LabAcceptanceError
from synthran.r2lab.foundation_topology import R2LabTopologyFoundationError, accept_topology_foundation
from synthran.r2lab.hardware import RADIOS, UES, PhysicalTopology, R2LabHardwareError
from synthran.r2lab.lifecycle import R2LabPhysicalLifecycleError, continue_physical_path, run_physical_workload
from synthran.r2lab.n3xx import R2LabN3xxError, stage_n3xx_gnb, start_n3xx_gnb, stop_n3xx_gnb
from synthran.r2lab.resources import (
    R2LabTopologyResourceError,
    load_topology,
    prepare_physical_resources,
    release_physical_resources,
    verify_physical_authority,
)
from synthran.r2lab.ue import R2LabPhysicalUeError
from synthran.slices_controller import SlicesControllerError
from synthran.workspace.model import WorkspaceError


_EXECUTABLE_DEVICES = tuple(sorted(name for name, profile in RADIOS.items() if profile.executable))
_EXECUTABLE_UES = tuple(sorted(name for name, profile in UES.items() if profile.executable))


class _RunProgress:
    """Terminal progress for long unified runs, isolated from result stdout."""

    def __init__(self, *, enabled: bool = True, stream: TextIO | None = None) -> None:
        self.enabled = enabled
        self.stream = stream if stream is not None else sys.stderr
        self.current_stage: str | None = None

    def _emit(self, marker: str, stage: str, detail: str | None = None) -> None:
        if not self.enabled:
            return
        suffix = f": {detail}" if detail else ""
        print(f"{marker} {stage}{suffix}", file=self.stream, flush=True)

    def start(self, stage: str, detail: str | None = None) -> None:
        self.current_stage = stage
        self._emit("→", stage, detail)

    def done(self, stage: str, detail: str | None = None) -> None:
        self._emit("✓", stage, detail)
        if self.current_stage == stage:
            self.current_stage = None

    def resumed(self, stage: str, detail: str | None = None) -> None:
        self._emit("↻", stage, detail)
        if self.current_stage == stage:
            self.current_stage = None

    def skipped(self, stage: str, detail: str | None = None) -> None:
        self._emit("–", stage, detail)
        if self.current_stage == stage:
            self.current_stage = None

    def fail(self, detail: str) -> None:
        stage = self.current_stage or "run"
        self._emit("✗", stage, detail)
        self.current_stage = None


def _subparsers(parser: argparse.ArgumentParser) -> argparse._SubParsersAction:
    for action in parser._actions:
        if isinstance(action, argparse._SubParsersAction):
            return action
    raise BackendError("SynthRAN parser does not expose top-level commands")


def _add_common(run: argparse.ArgumentParser) -> None:
    run.add_argument("--radio", required=True, choices=("rfsim", "r2lab"), help="radio backend")
    run.add_argument("--run-id", required=True)
    run.add_argument("--core-node", required=True, choices=tuple(sorted(SUPPORTED_NODES)))
    run.add_argument("--ran-node", required=True, choices=tuple(sorted(SUPPORTED_NODES)))
    run.add_argument("--owner", default=os.environ.get("SYNTHRAN_OWNER"))
    command_runtime._add_slices_context(run)
    run.add_argument("--slices-duration", default="4h")
    run.add_argument("--lock", type=Path, default=Path("dependencies.lock.yml"))
    run.add_argument("--deps-root", type=Path, default=Path(".deps"))
    run.add_argument("--collection-seconds", type=int, default=DEFAULT_COLLECTION_SECONDS)
    run.add_argument("--minimum-per-sensor", type=int, default=DEFAULT_MINIMUM_PER_SENSOR)
    run.add_argument("--timeout", type=int, default=1800)
    run.add_argument("--json", action="store_true")
    run.add_argument(
        "--quiet",
        action="store_true",
        help="suppress live progress messages; final output is unchanged",
    )


def _add_r2lab(run: argparse.ArgumentParser) -> None:
    run.add_argument("--device", choices=_EXECUTABLE_DEVICES)
    run.add_argument("--ue", choices=_EXECUTABLE_UES)
    run.add_argument(
        "--slice",
        dest="r2lab_slice",
        default=os.environ.get("SYNTHRAN_R2LAB_SLICE"),
        help="R2Lab slice name (or SYNTHRAN_R2LAB_SLICE)",
    )
    run.add_argument(
        "--allocation-id",
        default=os.environ.get("SYNTHRAN_ALLOCATION_ID"),
        help="expected current SLICES allocation identifier when one is pinned",
    )
    run.add_argument(
        "--known-hosts",
        type=Path,
        default=os.environ.get("SYNTHRAN_SLICES_KNOWN_HOSTS"),
        help="strict SLICES known-hosts path",
    )
    run.add_argument("--previous-run-id")
    run.add_argument("--n2-attempts", type=int, default=12)
    run.add_argument("--n2-convergence-attempts", type=int, default=12)
    run.add_argument("--n2-interval", type=float, default=5.0)
    run.add_argument(
        "--keep-resources",
        action="store_true",
        help="leave the exact run-owned gNB/radio/UE up after accepted workload",
    )
    run.add_argument(
        "--r2lab-run-root",
        type=Path,
        default=Path(".synthran/r2lab"),
        help=argparse.SUPPRESS,
    )
    run.add_argument(
        "--r2lab-experiment-root",
        type=Path,
        default=Path(".synthran/experiments-r2lab"),
        help=argparse.SUPPRESS,
    )


def _add_rfsim(run: argparse.ArgumentParser) -> None:
    run.add_argument("--duration-minutes", type=int, default=120)
    run.add_argument(
        "--preparation-root",
        type=Path,
        default=Path(".synthran/preparations"),
        help=argparse.SUPPRESS,
    )
    run.add_argument(
        "--network-run-root",
        type=Path,
        default=Path(".synthran/runs"),
        help=argparse.SUPPRESS,
    )
    run.add_argument(
        "--experiment-root",
        type=Path,
        default=Path(".synthran/experiments"),
        help=argparse.SUPPRESS,
    )


def configure_run_parser(parser: argparse.ArgumentParser) -> None:
    root = _subparsers(parser)
    if "run" in root.choices:
        return
    run = root.add_parser(
        "run",
        help="execute one complete SynthRAN lifecycle sequentially",
        description=(
            "Create/reuse provider context, prepare resources, prove the network path, "
            "run the deterministic workload, and persist acceptance evidence."
        ),
    )
    _add_common(run)
    _add_r2lab(run)
    _add_rfsim(run)


def _require_owner(args: argparse.Namespace) -> str:
    if not args.owner:
        raise BackendError("run requires --owner or SYNTHRAN_OWNER")
    return str(args.owner)


def _provider(args: argparse.Namespace) -> tuple[str, str, bool, object]:
    return _ensure_slices_provider_context(args)


def _namespace_owner(
    *, topology: PhysicalTopology, known_hosts: Path, timeout_seconds: int
) -> str | None:
    command = (
        "ssh",
        "-o",
        "BatchMode=yes",
        "-o",
        "ConnectTimeout=10",
        "-o",
        "StrictHostKeyChecking=yes",
        "-o",
        f"UserKnownHostsFile={known_hosts}",
        "-o",
        "GlobalKnownHostsFile=/dev/null",
        f"root@{topology.core_node}",
        shlex.join(
            (
                "kubectl",
                "get",
                "namespace",
                "open5gs",
                "--ignore-not-found",
                "-o",
                "jsonpath={.metadata.labels.synthran\\.run/id}",
            )
        ),
    )
    result = cluster_runner(command, min(timeout_seconds, 60))
    if result.returncode != 0:
        raise BackendError("current Open5GS namespace owner could not be observed")
    owner = result.stdout.strip()
    if not owner:
        return None
    validate_run_id(owner)
    return owner


def _physical_inventory(run_root: Path, run_id: str, topology: PhysicalTopology) -> Path:
    path = run_root.expanduser().resolve() / run_id / "physical-workload-hosts.ini"
    text, _ = build_preparation_inventory(
        core_node=topology.core_node,
        ran_node=topology.ran_node,
        source=path,
    )
    text = text.replace('rru="rfsim"', f'rru="{topology.radio}"', 1)
    if path.exists():
        if path.read_text(encoding="utf-8") != text:
            raise BackendError("persisted physical workload inventory does not match selected topology")
        return path
    path.write_text(text, encoding="utf-8", newline="\n")
    return path


def _physical_topology(args: argparse.Namespace) -> PhysicalTopology:
    if args.device is None:
        raise BackendError("--radio r2lab requires --device n300 or --device n320")
    if args.ue is None:
        raise BackendError("--radio r2lab requires --ue")
    return PhysicalTopology(
        core_node=args.core_node,
        ran_node=args.ran_node,
        radio=args.device,
        ue=args.ue,
    ).validate()


def _physical_context(args: argparse.Namespace) -> tuple[str, str, str | None, Path]:
    if not args.r2lab_slice:
        raise BackendError("--radio r2lab requires --slice or SYNTHRAN_R2LAB_SLICE")
    owner = _require_owner(args)
    if args.known_hosts is None:
        raise BackendError("--radio r2lab requires --known-hosts or SYNTHRAN_SLICES_KNOWN_HOSTS")
    known_hosts = Path(args.known_hosts).expanduser().resolve()
    if not known_hosts.is_file():
        raise BackendError("strict SLICES known-hosts file is missing")
    return str(args.r2lab_slice), owner, args.allocation_id, known_hosts


def _run_r2lab(
    args: argparse.Namespace, progress: _RunProgress | None = None
) -> dict[str, object]:
    progress = progress or _RunProgress(enabled=False)
    topology = _physical_topology(args)
    slice_name, owner, allocation_id, known_hosts = _physical_context(args)

    progress.start("provider", "select/create SLICES experiment and Post5G prefix")
    project, experiment, experiment_created, controller = _provider(args)
    progress.done(
        "provider",
        f"{project}/{experiment} ({'created' if experiment_created else 'reused'})",
    )

    run_root = args.r2lab_run_root.expanduser().resolve()
    run_directory = run_root / args.run_id

    if run_directory.exists():
        progress.start("resources", f"verify existing {topology.radio} + {topology.ue} claim")
        stored = load_topology(run_root=run_root, run_id=args.run_id).validate()
        if stored != topology:
            raise BackendError("existing physical run topology does not match requested run")
        verify_physical_authority(
            run_id=args.run_id,
            slice_name=slice_name,
            run_root=run_root,
            timeout_seconds=min(args.timeout, 300),
        )
        resource_status = "resumed"
        progress.resumed("resources", "existing physical authority verified")
    else:
        progress.start("resources", f"prepare {topology.radio} + {topology.ue}")
        prepare_physical_resources(
            run_id=args.run_id,
            slice_name=slice_name,
            topology=topology,
            run_root=run_root,
            timeout_seconds=min(args.timeout, 300),
        )
        resource_status = "prepared"
        progress.done("resources", "physical claim held")

    evidence_path = run_directory / "physical-run.json"
    evidence = PhysicalRunEvidence.read_json(evidence_path) if evidence_path.is_file() else None
    if evidence is None or (
        evidence.acceptance.outcome_for(PhysicalAcceptanceStage.OPEN5GS).value != "passed"
    ):
        progress.start("foundation", "prove Kubernetes, Open5GS, n3network and ru-network")
        previous = args.previous_run_id
        if previous is None:
            current_owner = _namespace_owner(
                topology=topology,
                known_hosts=known_hosts,
                timeout_seconds=args.timeout,
            )
            previous = current_owner if current_owner not in {None, args.run_id} else None
        foundation = accept_topology_foundation(
            run_id=args.run_id,
            previous_run_id=previous,
            slice_name=slice_name,
            owner=owner,
            allocation_id=allocation_id,
            known_hosts=known_hosts,
            run_root=run_root,
            lock_path=args.lock,
            dependency_root=args.deps_root,
            timeout_seconds=args.timeout,
        )
        foundation_status: Mapping[str, object] = foundation.to_dict()
        progress.done("foundation", "physical foundation ready")
    else:
        foundation_status = {
            "status": "resumed",
            "next_stage": evidence.acceptance.next_stage.value if evidence.acceptance.next_stage else None,
        }
        progress.resumed("foundation", "accepted foundation evidence present")

    evidence = PhysicalRunEvidence.read_json(evidence_path)
    if evidence.staged is None:
        progress.start("gNB staging", f"render and bind {topology.radio} at zero replicas")
        artifact = stage_n3xx_gnb(
            run_id=args.run_id,
            slice_name=slice_name,
            owner=owner,
            allocation_id=allocation_id,
            known_hosts=known_hosts,
            lock_path=args.lock,
            deps_root=args.deps_root,
            run_root=run_root,
            timeout_seconds=args.timeout,
        )
        staging_status: Mapping[str, object] = artifact.to_dict()
        progress.done("gNB staging", "artifact rendered and network attachments validated")
    else:
        staging_status = {"status": "resumed-staged"}
        progress.resumed("gNB staging", "staged artifact already accepted")

    evidence = PhysicalRunEvidence.read_json(evidence_path)
    if evidence.gnb_start is None:
        progress.start("gNB/N2", "start singleton gNB and establish stable N2")
        started = start_n3xx_gnb(
            run_id=args.run_id,
            slice_name=slice_name,
            owner=owner,
            allocation_id=allocation_id,
            known_hosts=known_hosts,
            run_root=run_root,
            timeout_seconds=args.timeout,
            required_consecutive_proofs=args.n2_attempts,
            convergence_attempts=args.n2_convergence_attempts,
            poll_interval_seconds=args.n2_interval,
        )
        gnb_status: Mapping[str, object] = started.to_dict()
        progress.done("gNB/N2", "stable N2 established")
    else:
        gnb_status = {"status": "resumed-gnb-n2"}
        progress.resumed("gNB/N2", "accepted gNB/N2 evidence present")

    evidence = PhysicalRunEvidence.read_json(evidence_path)
    peer = SUPPORTED_NODES[topology.ran_node].ip
    retryable_ue_failure = evidence.acceptance.failed_stage in {
        PhysicalAcceptanceStage.CELL_ACQUISITION,
        PhysicalAcceptanceStage.REGISTRATION,
        PhysicalAcceptanceStage.PDU_SESSION,
    }
    if (
        evidence.acceptance.next_stage not in {PhysicalAcceptanceStage.WORKLOAD, None}
        or retryable_ue_failure
    ):
        detail = f"cell → registration → PDU → user plane via {peer}"
        if retryable_ue_failure:
            detail = f"retry transport-derived UE proof; {detail}"
        progress.start("UE path", detail)
        path = continue_physical_path(
            run_id=args.run_id,
            slice_name=slice_name,
            owner=owner,
            allocation_id=allocation_id,
            known_hosts=known_hosts,
            peer=peer,
            run_root=run_root,
            timeout_seconds=min(args.timeout, 300),
        )
        if not path.ready_for_workload:
            raise BackendError(f"physical path stopped at {path.failed_stage or path.next_stage}")
        path_status: Mapping[str, object] = path.to_dict()
        progress.done("UE path", "registration, PDU and user-plane proof accepted")
    else:
        path_status = {"status": "resumed", "measurement_peer": peer}
        progress.resumed("UE path", "accepted path evidence present")

    evidence = PhysicalRunEvidence.read_json(evidence_path)
    if evidence.acceptance.next_stage is PhysicalAcceptanceStage.WORKLOAD:
        progress.start("workload", "run deterministic ten-sensor experiment and collect data")
        inventory = _physical_inventory(run_root, args.run_id, topology)
        workload = run_physical_workload(
            run_id=args.run_id,
            workload_id=args.run_id,
            slice_name=slice_name,
            owner=owner,
            allocation_id=allocation_id,
            known_hosts=known_hosts,
            inventory_path=inventory,
            lock_path=args.lock,
            deps_root=args.deps_root,
            run_root=run_root,
            experiment_root=args.r2lab_experiment_root,
            collection_seconds=args.collection_seconds,
            minimum_per_sensor=args.minimum_per_sensor,
            timeout_seconds=min(args.timeout, 300),
        )
        if not workload.accepted:
            raise BackendError("physical deterministic workload was not accepted")
        workload_status: Mapping[str, object] = workload.to_dict()
        progress.done("workload", "deterministic workload accepted")
    else:
        workload_status = {"status": "resumed-accepted"}
        progress.resumed("workload", "accepted workload evidence present")

    progress.start("acceptance", "verify complete physical lifecycle evidence")
    evidence = PhysicalRunEvidence.read_json(evidence_path)
    if not evidence.acceptance.accepted:
        raise BackendError("physical lifecycle did not reach acceptance")
    progress.done("acceptance", str(evidence_path))

    released = False
    release_status: Mapping[str, object] | None = None
    if not args.keep_resources:
        progress.start("cleanup", "stop run-owned gNB and release exact radio/UE resources")
        stop = lambda: stop_n3xx_gnb(
            run_id=args.run_id,
            slice_name=slice_name,
            owner=owner,
            allocation_id=allocation_id,
            known_hosts=known_hosts,
            run_root=run_root,
            timeout_seconds=max(min(args.timeout, 300), 30),
        )
        release_status = release_physical_resources(
            run_id=args.run_id,
            slice_name=slice_name,
            run_root=run_root,
            timeout_seconds=min(args.timeout, 300),
            stop_gnb=stop,
        )
        released = True
        progress.done("cleanup", "exact physical off-state proven")
    else:
        progress.skipped("cleanup", "resources retained by --keep-resources")

    assert controller.post5g_network is not None
    return {
        "schema": "synthran/run/v1alpha1",
        "run_id": args.run_id,
        "radio": "r2lab",
        "device": topology.radio,
        "ue": topology.ue,
        "topology": topology.to_dict(),
        "provider": {
            "project": project,
            "experiment": experiment,
            "experiment_created": experiment_created,
            "network": controller.post5g_network.to_dict(),
        },
        "measurement_peer": peer,
        "stages": {
            "resources": resource_status,
            "foundation": dict(foundation_status),
            "gnb_staging": dict(staging_status),
            "gnb_n2": dict(gnb_status),
            "path": dict(path_status),
            "workload": dict(workload_status),
        },
        "accepted": True,
        "released": released,
        "release": dict(release_status) if release_status is not None else None,
        "evidence_path": str(evidence_path),
    }


def _authority(path: Path) -> dict[str, str]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise BackendError("prepared authority file is unavailable") from exc
    result: dict[str, str] = {}
    for line in lines:
        if not line.startswith("export ") or "=" not in line:
            continue
        key, raw = line[len("export "):].split("=", 1)
        values = shlex.split(raw)
        result[key] = values[0] if values else ""
    return result


def _run_rfsim(
    args: argparse.Namespace, progress: _RunProgress | None = None
) -> dict[str, object]:
    progress = progress or _RunProgress(enabled=False)
    if args.device is not None or args.ue is not None:
        raise BackendError("--device/--ue are only valid with --radio r2lab")
    owner = _require_owner(args)

    progress.start("provider", "select/create SLICES experiment and Post5G prefix")
    project, experiment, experiment_created, controller = _provider(args)
    progress.done(
        "provider",
        f"{project}/{experiment} ({'created' if experiment_created else 'reused'})",
    )

    preparation_root = args.preparation_root.expanduser().resolve()
    prep_dir = preparation_root / args.run_id
    if not prep_dir.exists():
        progress.start("resources", "prepare RFSIM compute reservation and allocation")
        rc = command_runtime._network_prepare(
            argparse.Namespace(
                lock=args.lock,
                core_node=args.core_node,
                ran_node=args.ran_node,
                duration_minutes=args.duration_minutes,
                run_id=args.run_id,
                reservation_id=None,
                dry_run=False,
                json=False,
                slices_project=project,
                slices_experiment=experiment,
                owner=owner,
                deps_root=args.deps_root,
                run_root=preparation_root,
                timeout=args.timeout,
            )
        )
        if rc != 0:
            raise BackendError("RFSIM resource preparation failed")
        progress.done("resources", "reservation/allocation prepared")
    else:
        progress.resumed("resources", "existing preparation present")
    inventory = prep_dir / "hosts.ini"
    authority = _authority(prep_dir / "authority.env")
    reservation_id = authority.get("SYNTHRAN_RESERVATION_ID")
    allocation_id = authority.get("SYNTHRAN_ALLOCATION_ID")
    if not reservation_id or not allocation_id:
        raise BackendError("prepared RFSIM authority is missing reservation/allocation IDs")

    preflight = prep_dir / "live-preflight.json"
    if not preflight.is_file():
        progress.start("preflight", "verify live provider and compute authority")
        rc = command_runtime._doctor(
            argparse.Namespace(
                inventory=inventory,
                lock=args.lock,
                deps_root=args.deps_root,
                offline=False,
                slices_project=project,
                slices_experiment=experiment,
                owner=owner,
                reservation_id=reservation_id,
                allocation_id=allocation_id,
                evidence_out=preflight,
                timeout=min(args.timeout, 300),
            )
        )
        if rc != 0:
            raise BackendError("RFSIM live preflight failed")
        progress.done("preflight", "live authority verified")
    else:
        progress.resumed("preflight", "existing preflight evidence present")

    network_dir = args.network_run_root.expanduser().resolve() / args.run_id
    if not (network_dir / "manifest.json").is_file():
        progress.start("network", "deploy RFSIM 5G network")
        rc = command_runtime._network_deploy(
            argparse.Namespace(
                inventory=inventory,
                lock=args.lock,
                deps_root=args.deps_root,
                profile="default",
                dry_run=False,
                json=False,
                slices_project=project,
                slices_experiment=experiment,
                owner=owner,
                reservation_id=reservation_id,
                allocation_id=allocation_id,
                preflight_evidence=preflight,
                run_id=args.run_id,
                run_root=args.network_run_root,
                timeout=args.timeout,
            )
        )
        if rc != 0:
            raise BackendError("RFSIM network deployment failed")
        progress.done("network", "RFSIM network deployed")
    else:
        progress.resumed("network", "existing network manifest present")

    network_evidence = network_dir / "network-evidence.json"
    if not network_evidence.is_file():
        progress.start("path", "prove RFSIM network path")
        rc = command_runtime._network_verify(
            argparse.Namespace(
                inventory=inventory,
                lock=args.lock,
                deps_root=args.deps_root,
                slices_project=project,
                slices_experiment=experiment,
                run_id=args.run_id,
                run_root=args.network_run_root,
                timeout=min(args.timeout, 300),
            )
        )
        if rc != 0:
            raise BackendError("RFSIM network path was not proven")
        progress.done("path", "network path accepted")
    else:
        progress.resumed("path", "existing network evidence present")

    experiment_dir = args.experiment_root.expanduser().resolve() / args.run_id
    experiment_evidence = experiment_dir / "experiment-evidence.json"
    if not experiment_evidence.is_file():
        progress.start("workload", "run deterministic experiment and collect data")
        rc = command_runtime._experiment_run(
            argparse.Namespace(
                inventory=inventory,
                lock=args.lock,
                deps_root=args.deps_root,
                network_run_id=args.run_id,
                run_id=args.run_id,
                network_run_root=args.network_run_root,
                run_root=args.experiment_root,
                collection_seconds=args.collection_seconds,
                minimum_per_sensor=args.minimum_per_sensor,
            )
        )
        if rc != 0:
            raise BackendError("RFSIM deterministic workload was not accepted")
        progress.done("workload", "deterministic workload completed")
    else:
        progress.resumed("workload", "existing experiment evidence present")

    progress.start("acceptance", "verify experiment evidence")
    try:
        accepted_payload = json.loads(experiment_evidence.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise BackendError("RFSIM experiment evidence is unreadable") from exc
    accepted = accepted_payload.get("ready") is True
    if not accepted:
        raise BackendError("RFSIM experiment evidence is not accepted")
    progress.done("acceptance", str(experiment_evidence))

    assert controller.post5g_network is not None
    return {
        "schema": "synthran/run/v1alpha1",
        "run_id": args.run_id,
        "radio": "rfsim",
        "topology": {"core_node": args.core_node, "ran_node": args.ran_node},
        "provider": {
            "project": project,
            "experiment": experiment,
            "experiment_created": experiment_created,
            "network": controller.post5g_network.to_dict(),
        },
        "accepted": True,
        "evidence_path": str(experiment_evidence),
    }


class RunCommandAdapter:
    """Backend-selecting adapter for the single sequential run command."""

    def configure_parser(self, parser: argparse.ArgumentParser) -> None:
        configure_run_parser(parser)

    def dispatch(self, args: argparse.Namespace) -> int:
        if args.command != "run":
            raise BackendError("unsupported unified run command")
        progress = _RunProgress(enabled=not args.quiet)
        try:
            validate_run_id(args.run_id)
            if args.core_node == args.ran_node:
                raise BackendError("core and RAN nodes must differ")
            payload = (
                _run_r2lab(args, progress)
                if args.radio == "r2lab"
                else _run_rfsim(args, progress)
            )
            if args.json:
                print(json.dumps(payload, indent=2, sort_keys=True))
            else:
                print(f"SynthRAN run accepted: {payload['run_id']} ({payload['radio']})")
                print(f"Evidence: {payload['evidence_path']}")
                if payload.get("released") is True:
                    print("Physical resources: released")
            return 0
        except BackendError as exc:
            progress.fail(str(exc))
            raise
        except (
            DependencyError,
            ExperimentError,
            FiveGAnsibleError,
            LivePreflightError,
            NetworkRuntimeError,
            PrivacyError,
            ResourcePreparationError,
            R2LabAcceptanceError,
            R2LabHardwareError,
            R2LabN3xxError,
            R2LabPhysicalLifecycleError,
            R2LabPhysicalUeError,
            R2LabTopologyFoundationError,
            R2LabTopologyResourceError,
            SlicesControllerError,
            WorkspaceError,
            OSError,
            ValueError,
        ) as exc:
            progress.fail(str(exc))
            raise BackendError(str(exc)) from exc
