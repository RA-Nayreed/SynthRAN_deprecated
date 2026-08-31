"""Canonical full-run lifecycle orchestration for SynthRAN.

This module owns the backend-neutral lifecycle boundary. Backend-specific work
remains in the network and R2Lab domains; this layer decides the semantic
provider → infrastructure → network → workload → acceptance → cleanup flow and
emits those semantics directly to the canonical RunEvent stream.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import shlex
from typing import Mapping

from synthran.amber_experiment_runtime import execute_amber_experiment
from synthran.dependencies import load_lock
from synthran.errors import SynthRANError
from synthran.experiment.live import DEFAULT_COLLECTION_SECONDS, DEFAULT_MINIMUM_PER_SENSOR
from synthran.fiveg_ansible import build_network_plan, load_inventory, parse_inventory, run_offline_doctor
from synthran.live_preflight import (
    LivePreflightError,
    load_fresh_live_evidence,
    run_live_preflight,
    save_live_evidence,
    subprocess_runner as cluster_runner,
)
from synthran.network.resources import (
    SUPPORTED_NODES,
    build_preparation_inventory,
    build_resource_preparation_plan,
    execute_resource_preparation,
)
from synthran.network.runtime import (
    execute_network_deployment,
    load_deployment_manifest,
    save_network_evidence,
    validate_run_id,
    verify_network_path,
)
from synthran.privacy import repository_root
from synthran.provider import ensure_slices_provider_context
from synthran.r2lab.acceptance import PhysicalAcceptanceStage, PhysicalRunEvidence
from synthran.r2lab.foundation_topology import accept_topology_foundation
from synthran.r2lab.hardware import RADIOS, UES, PhysicalTopology
from synthran.r2lab.iot_lifecycle import run_physical_iot_workload
from synthran.r2lab.lifecycle import continue_physical_path
from synthran.r2lab.n3xx import stage_n3xx_gnb, start_n3xx_gnb, stop_n3xx_gnb
from synthran.r2lab.reconciliation import reconcile_live_resume
from synthran.r2lab.resources import (
    load_topology,
    prepare_physical_resources,
    reconcile_physical_resources,
    release_physical_resources,
)
from synthran.r2lab.upstream_roles import stop_role_managed_gnb
from synthran.run_events import RunProgress
from synthran.slices_controller import verify_slices_controller
from synthran.utils.environment import scoped_environment
from synthran.utils.ssh import strict_ssh_command


_EXECUTABLE_DEVICES = tuple(sorted(name for name, profile in RADIOS.items() if profile.executable))
_EXECUTABLE_UES = tuple(sorted(name for name, profile in UES.items() if profile.executable))


def _subparsers(parser: argparse.ArgumentParser) -> argparse._SubParsersAction:
    for action in parser._actions:
        if isinstance(action, argparse._SubParsersAction):
            return action
    raise SynthRANError("SynthRAN parser does not expose top-level commands")


def _add_slices_context(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--slices-project",
        default=os.environ.get("SYNTHRAN_SLICES_PROJECT"),
        help="SLICES project (or SYNTHRAN_SLICES_PROJECT)",
    )
    parser.add_argument(
        "--slices-experiment",
        default=os.environ.get("SYNTHRAN_SLICES_EXPERIMENT"),
        help="provider experiment override; defaults to the run ID",
    )


def _add_common(run: argparse.ArgumentParser) -> None:
    run.add_argument("--radio", required=True, choices=("rfsim", "r2lab"), help="radio backend")
    run.add_argument("--run-id", required=True)
    run.add_argument("--core-node", required=True, choices=tuple(sorted(SUPPORTED_NODES)))
    run.add_argument("--ran-node", required=True, choices=tuple(sorted(SUPPORTED_NODES)))
    run.add_argument("--owner", default=os.environ.get("SYNTHRAN_OWNER"))
    _add_slices_context(run)
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
        help="suppress terminal progress while still persisting the event stream",
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
        help="leave the exact run-owned physical resources active after acceptance",
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
    """Install the single full-lifecycle run parser."""

    root = _subparsers(parser)
    if "run" in root.choices:
        return
    run = root.add_parser(
        "run",
        help="execute one complete SynthRAN run",
        description=(
            "Create or reuse provider context, prepare resources, verify network readiness, "
            "run the deterministic AMBER workload, persist evidence, and clean up exact owned resources."
        ),
    )
    _add_common(run)
    _add_r2lab(run)
    _add_rfsim(run)


def _component(
    progress: RunProgress,
    stage: str,
    name: str,
    detail: str | None = None,
    *,
    event: str = "detail",
) -> None:
    marker = {
        "started": "→",
        "completed": "✓",
        "resumed": "↻",
        "skipped": "–",
        "failed": "✗",
        "heartbeat": "…",
    }.get(event, "·")
    suffix = f": {detail}" if detail else ""
    progress.stream.emit(
        f"  {marker} {name}{suffix}",
        stage=stage,
        component=name,
        event=event,
    )


def _require_owner(args: argparse.Namespace) -> str:
    if not args.owner:
        raise SynthRANError("run requires --owner or SYNTHRAN_OWNER")
    return str(args.owner)


def _provider(args: argparse.Namespace) -> tuple[str, str, bool, object]:
    return ensure_slices_provider_context(args)


def _namespace_owner(
    *, topology: PhysicalTopology, known_hosts: Path, timeout_seconds: int
) -> str | None:
    try:
        command = strict_ssh_command(
            f"root@{topology.core_node}",
            "kubectl",
            "get",
            "namespace",
            "open5gs",
            "--ignore-not-found",
            "-o",
            "jsonpath={.metadata.labels.synthran\\.run/id}",
            known_hosts=known_hosts,
            isolated_config=True,
            quote_remote=True,
        )
    except ValueError as exc:
        raise SynthRANError(str(exc)) from exc
    result = cluster_runner(command, min(timeout_seconds, 60))
    if result.returncode != 0:
        raise SynthRANError("current Open5GS namespace owner could not be observed")
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
            raise SynthRANError("persisted physical workload inventory does not match selected topology")
        return path
    path.write_text(text, encoding="utf-8", newline="\n")
    return path


def _physical_topology(args: argparse.Namespace) -> PhysicalTopology:
    if args.device is None:
        raise SynthRANError("--radio r2lab requires --device n300 or --device n320")
    if args.ue is None:
        raise SynthRANError("--radio r2lab requires --ue")
    return PhysicalTopology(
        core_node=args.core_node,
        ran_node=args.ran_node,
        radio=args.device,
        ue=args.ue,
    ).validate()


def _physical_context(args: argparse.Namespace) -> tuple[str, str, str | None, Path]:
    if not args.r2lab_slice:
        raise SynthRANError("--radio r2lab requires --slice or SYNTHRAN_R2LAB_SLICE")
    owner = _require_owner(args)
    if args.known_hosts is None:
        raise SynthRANError("--radio r2lab requires --known-hosts or SYNTHRAN_SLICES_KNOWN_HOSTS")
    known_hosts = Path(args.known_hosts).expanduser().resolve()
    if not known_hosts.is_file():
        raise SynthRANError("strict SLICES known-hosts file is missing")
    return str(args.r2lab_slice), owner, args.allocation_id, known_hosts


def _authority(path: Path) -> dict[str, str]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise SynthRANError("prepared authority file is unavailable") from exc
    result: dict[str, str] = {}
    for line in lines:
        if not line.startswith("export ") or "=" not in line:
            continue
        key, raw = line[len("export "):].split("=", 1)
        values = shlex.split(raw)
        result[key] = values[0] if values else ""
    return result


def _read_json(path: Path) -> Mapping[str, object] | None:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None


def _network_details(evidence_path: Path) -> tuple[str, ...]:
    evidence = _read_json(evidence_path)
    if evidence is None:
        return ("5G session readiness verified",)
    path = evidence.get("path")
    pdu = path.get("pdu_address") if isinstance(path, dict) else None
    lines = ["gNB cell active"]
    if isinstance(pdu, str) and pdu:
        lines.append(f"PDU session · {pdu}")
    lines.append("srsUE session and UPF route ready")
    return tuple(lines)


def _amber_details(experiment_directory: Path) -> tuple[str, ...]:
    wrapper = _read_json(experiment_directory / "experiment-evidence.json")
    if wrapper is None:
        return ()
    iot_name = wrapper.get("iot_evidence")
    if not isinstance(iot_name, str) or not iot_name:
        return ()
    iot = _read_json(experiment_directory / iot_name)
    if iot is None:
        return ()
    live = iot.get("live_transport")
    reconciliation = live.get("reconciliation") if isinstance(live, dict) else None
    if not isinstance(reconciliation, dict):
        return ()

    def count(name: str) -> int | None:
        value = reconciliation.get(name)
        return value if isinstance(value, int) and not isinstance(value, bool) else None

    planned = count("planned_count")
    decoded = count("decoded_count")
    source_loss = count("source_loss_count")
    published = count("published_count")
    received = count("central_received_count")
    transport_loss = count("transport_loss_count")
    duplicates = count("duplicate_count")

    lines: list[str] = ["PDU-bound TCP transport gate passed"]
    if None not in (planned, decoded, source_loss):
        lines.append(
            f"source · planned={planned} · decoded={decoded} · source-loss={source_loss}"
        )
    if None not in (published, received, transport_loss, duplicates):
        lines.append(
            "transport · "
            f"published={published} · received={received} · "
            f"loss={transport_loss} · duplicates={duplicates}"
        )
    return tuple(lines)


def _run_r2lab(args: argparse.Namespace, progress: RunProgress) -> dict[str, object]:
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
    resumed_physical_run = run_directory.exists()

    progress.start("infrastructure", f"prepare {topology.radio} + {topology.ue}")
    if resumed_physical_run:
        stored = load_topology(run_root=run_root, run_id=args.run_id).validate()
        if stored != topology:
            raise SynthRANError("existing physical run topology does not match requested run")
        reconcile_physical_resources(
            run_id=args.run_id,
            slice_name=slice_name,
            lock_path=args.lock,
            deps_root=args.deps_root,
            run_root=run_root,
            timeout_seconds=min(args.timeout, 300),
            progress=progress.child_stream,
        )
        resource_status = "reconciled"
        _component(progress, "infrastructure", "physical authority", "existing claim reconciled", event="resumed")
    else:
        prepare_physical_resources(
            run_id=args.run_id,
            slice_name=slice_name,
            topology=topology,
            lock_path=args.lock,
            deps_root=args.deps_root,
            run_root=run_root,
            timeout_seconds=min(args.timeout, 300),
            progress=progress.child_stream,
        )
        resource_status = "prepared"
        _component(progress, "infrastructure", "physical authority", "exact radio/UE claim held", event="completed")
    progress.done("infrastructure", "physical resources ready")

    evidence_path = run_directory / "physical-run.json"
    evidence = PhysicalRunEvidence.read_json(evidence_path) if evidence_path.is_file() else None
    live_resume_status: Mapping[str, object] | None = None

    progress.start("network", "prove current physical 5G session readiness")
    if (
        resumed_physical_run
        and evidence is not None
        and not evidence.acceptance.accepted
        and evidence.acceptance.outcome_for(PhysicalAcceptanceStage.OPEN5GS).value == "passed"
    ):
        _component(progress, "network", "live resume", "re-prove current foundation, gNB/N2 and UE state", event="started")
        live_resume = reconcile_live_resume(
            run_id=args.run_id,
            slice_name=slice_name,
            owner=owner,
            allocation_id=allocation_id,
            known_hosts=known_hosts,
            lock_path=args.lock,
            deps_root=args.deps_root,
            run_root=run_root,
            timeout_seconds=args.timeout,
            n2_attempts=args.n2_attempts,
            n2_convergence_attempts=args.n2_convergence_attempts,
            n2_interval=args.n2_interval,
            progress=progress.child_stream,
        )
        allocation_id = live_resume.allocation_id
        live_resume_status = live_resume.to_dict()
        _component(progress, "network", "live resume", "current prerequisites re-proven", event="completed")

    if evidence is None or (
        evidence.acceptance.outcome_for(PhysicalAcceptanceStage.OPEN5GS).value != "passed"
    ):
        _component(progress, "network", "foundation", "Kubernetes, Open5GS and physical networks", event="started")
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
            progress=progress.child_stream,
        )
        foundation_status: Mapping[str, object] = foundation.to_dict()
        _component(progress, "network", "foundation", "physical foundation ready", event="completed")
    else:
        foundation_status = {
            "status": "resumed",
            "next_stage": evidence.acceptance.next_stage.value if evidence.acceptance.next_stage else None,
        }
        _component(progress, "network", "foundation", "accepted evidence retained; current state re-proven", event="resumed")

    evidence = PhysicalRunEvidence.read_json(evidence_path)
    if evidence.staged is None:
        _component(progress, "network", "gNB staging", f"bind {topology.radio} at zero replicas", event="started")
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
        _component(progress, "network", "gNB staging", "artifact and attachments validated", event="completed")
    else:
        staging_status = {"status": "resumed-staged"}
        _component(progress, "network", "gNB staging", "immutable staged artifact retained", event="resumed")

    evidence = PhysicalRunEvidence.read_json(evidence_path)
    if evidence.gnb_start is None:
        _component(progress, "network", "gNB/N2", "start singleton gNB and establish stable N2", event="started")
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
        _component(progress, "network", "gNB/N2", "stable N2 established", event="completed")
    else:
        gnb_status = {"status": "resumed-gnb-n2"}
        _component(progress, "network", "gNB/N2", "historical evidence retained; current N2 re-proven", event="resumed")

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
        _component(progress, "network", "UE session", detail, event="started")
        path = continue_physical_path(
            run_id=args.run_id,
            slice_name=slice_name,
            owner=owner,
            allocation_id=allocation_id,
            known_hosts=known_hosts,
            peer=peer,
            lock_path=args.lock,
            deps_root=args.deps_root,
            run_root=run_root,
            timeout_seconds=min(args.timeout, 300),
            progress=progress.child_stream,
        )
        if not path.ready_for_workload:
            raise SynthRANError(f"physical session stopped at {path.failed_stage or path.next_stage}")
        path_status: Mapping[str, object] = path.to_dict()
        _component(progress, "network", "UE session", "registration, PDU and user-plane readiness accepted", event="completed")
    else:
        path_status = {"status": "resumed", "measurement_peer": peer}
        _component(progress, "network", "UE session", "historical evidence retained; current state re-proven", event="resumed")
    progress.done("network", "READY")

    evidence = PhysicalRunEvidence.read_json(evidence_path)
    progress.start("workload", "deterministic AMBER source and PDU-bound transport")
    if evidence.acceptance.next_stage is PhysicalAcceptanceStage.WORKLOAD:
        inventory = _physical_inventory(run_root, args.run_id, topology)
        workload = run_physical_iot_workload(
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
            iot_profile=args.iot_profile,
            iot_seed=args.iot_seed,
            sensor_period_seconds=args.sensor_period,
            progress=progress.child_stream,
        )
        if not workload.accepted:
            raise SynthRANError("physical deterministic workload was not accepted")
        workload_status: Mapping[str, object] = workload.to_dict()
        progress.done("workload", "accepted")
    else:
        workload_status = {"status": "resumed-accepted"}
        progress.resumed("workload", "accepted evidence retained")

    progress.start("acceptance", "verify complete physical run evidence")
    evidence = PhysicalRunEvidence.read_json(evidence_path)
    if not evidence.acceptance.accepted:
        raise SynthRANError("physical run did not reach acceptance")
    progress.done("acceptance", str(evidence_path))

    released = False
    release_status: Mapping[str, object] | None = None
    if not args.keep_resources:
        progress.start("cleanup", "stop run-owned gNB and release exact radio/UE resources")
        role_managed_gnb = bool(
            live_resume_status is not None
            and live_resume_status.get("gnb_restarted") is True
        )
        if role_managed_gnb:
            stop = lambda: stop_role_managed_gnb(
                run_id=args.run_id,
                slice_name=slice_name,
                owner=owner,
                allocation_id=allocation_id,
                known_hosts=known_hosts,
                lock_path=args.lock,
                run_root=run_root,
                timeout_seconds=max(min(args.timeout, 300), 30),
            )
        else:
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
        "schema": "synthran/run/v1alpha2",
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
            "live_resume": dict(live_resume_status) if live_resume_status is not None else None,
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
        "event_path": str(progress.event_path),
    }


def _run_rfsim(args: argparse.Namespace, progress: RunProgress) -> dict[str, object]:
    if args.device is not None or args.ue is not None:
        raise SynthRANError("--device/--ue are only valid with --radio r2lab")
    owner = _require_owner(args)

    progress.start("provider", "select/create SLICES experiment and Post5G prefix")
    project, experiment, experiment_created, controller = _provider(args)
    progress.done(
        "provider",
        f"{project}/{experiment} ({'created' if experiment_created else 'reused'})",
    )

    preparation_root = args.preparation_root.expanduser().resolve()
    prep_dir = preparation_root / args.run_id
    inventory = prep_dir / "hosts.ini"

    progress.start("infrastructure", "allocate, bootstrap and prepare selected nodes")
    if not prep_dir.exists():
        lock = load_lock(args.lock)
        plan = build_resource_preparation_plan(
            lock=lock,
            core_node=args.core_node,
            ran_node=args.ran_node,
            duration_minutes=args.duration_minutes,
            run_id=args.run_id,
            reservation_id=None,
        )
        result = execute_resource_preparation(
            plan=plan,
            lock=lock,
            dependency_root=args.deps_root,
            owner=owner,
            slices_project=project,
            slices_experiment=experiment,
            reservation_id=None,
            run_root=preparation_root,
            repository_root=repository_root(),
            timeout_seconds=args.timeout,
            progress=progress.child_stream,
        )
        inventory = result.inventory_path
        _component(progress, "infrastructure", "resource preparation", "reservation/allocation prepared", event="completed")
    else:
        _component(progress, "infrastructure", "resource preparation", "existing preparation retained", event="resumed")

    authority = _authority(prep_dir / "authority.env")
    reservation_id = authority.get("SYNTHRAN_RESERVATION_ID")
    allocation_id = authority.get("SYNTHRAN_ALLOCATION_ID")
    known_hosts_value = authority.get("SYNTHRAN_KNOWN_HOSTS")
    if not reservation_id or not allocation_id:
        raise SynthRANError("prepared RFSIM authority is missing reservation/allocation IDs")
    if not known_hosts_value:
        raise SynthRANError("prepared RFSIM authority is missing strict SSH trust")
    known_hosts = Path(known_hosts_value).expanduser().resolve()
    if not known_hosts.is_file():
        raise SynthRANError("prepared RFSIM strict known-hosts file is missing")

    try:
        inventory_model = parse_inventory(
            inventory.read_text(encoding="utf-8"),
            source=inventory,
        )
    except OSError as exc:
        raise SynthRANError("prepared RFSIM inventory is unavailable") from exc
    lock = load_lock(args.lock)
    authority_environment = {"SYNTHRAN_KNOWN_HOSTS": str(known_hosts)}

    network_dir = args.network_run_root.expanduser().resolve() / args.run_id
    network_manifest = network_dir / "manifest.json"
    network_deployed = network_manifest.is_file()
    preflight = prep_dir / "live-preflight.json"

    if network_deployed:
        _component(progress, "infrastructure", "authority verification", "deployment already consumed accepted preflight", event="resumed")
    else:
        preflight_ready = False
        if preflight.is_file():
            try:
                load_fresh_live_evidence(
                    path=preflight,
                    inventory=inventory_model,
                    owner=owner,
                    reservation_id=reservation_id,
                    allocation_id=allocation_id,
                    lock=lock,
                    slices_project=project,
                    slices_experiment=experiment,
                )
            except LivePreflightError:
                preflight_ready = False
            else:
                preflight_ready = True

        if not preflight_ready:
            _component(progress, "infrastructure", "authority verification", "live provider and compute authority", event="started")
            offline = run_offline_doctor(
                inventory_path=inventory,
                lock_path=args.lock,
                dependency_root=args.deps_root,
            )
            if not offline.ready:
                raise SynthRANError("RFSIM offline readiness failed")
            live_report = run_live_preflight(
                inventory=inventory_model,
                lock=lock,
                owner=owner,
                reservation_id=reservation_id,
                allocation_id=allocation_id,
                slices_project=project,
                slices_experiment=experiment,
                timeout_seconds=min(args.timeout, 300),
            )
            save_live_evidence(live_report, preflight)
            if not live_report.ready:
                raise SynthRANError("RFSIM live preflight failed")
            _component(progress, "infrastructure", "authority verification", "live authority verified", event="completed")
        else:
            _component(progress, "infrastructure", "authority verification", "fresh READY preflight retained", event="resumed")
    progress.done("infrastructure", "READY")

    progress.start("network", "deploy and verify current RFSIM 5G session")
    if not network_deployed:
        _component(progress, "network", "5G deployment", "Open5GS + srsRAN RFSIM", event="started")
        report = run_offline_doctor(
            inventory_path=inventory,
            lock_path=args.lock,
            dependency_root=args.deps_root,
        )
        if not report.ready:
            raise SynthRANError("RFSIM offline readiness failed before deployment")
        plan = build_network_plan(lock=lock, inventory=inventory_model, profile="default")
        with scoped_environment(authority_environment):
            execute_network_deployment(
                plan=plan,
                lock=lock,
                dependency_root=args.deps_root,
                live_evidence_path=preflight,
                owner=owner,
                reservation_id=reservation_id,
                allocation_id=allocation_id,
                slices_project=project,
                slices_experiment=experiment,
                run_id=args.run_id,
                run_root=args.network_run_root,
                repository_root=repository_root(),
                timeout_seconds=args.timeout,
                progress=progress.child_stream,
            )
        _component(progress, "network", "5G deployment", "Open5GS + srsRAN deployed", event="completed")
    else:
        _component(progress, "network", "5G deployment", "existing deployment retained", event="resumed")

    network_evidence = network_dir / "network-evidence.json"
    if not network_evidence.is_file():
        _component(progress, "network", "session readiness", "live gNB, UE/PDU and UPF route", event="started")
        report = run_offline_doctor(
            inventory_path=inventory,
            lock_path=args.lock,
            dependency_root=args.deps_root,
        )
        if not report.ready:
            raise SynthRANError("RFSIM offline readiness failed before session verification")
        active_controller = verify_slices_controller(
            lock=lock,
            project=project,
            experiment=experiment,
            timeout_seconds=min(args.timeout, 300),
        )
        manifest = load_deployment_manifest(
            path=network_manifest,
            run_id=args.run_id,
            inventory=inventory_model,
            lock=lock,
            slices_project=project,
            slices_experiment=experiment,
        )
        if manifest.get("slices_controller") != active_controller.to_dict():
            raise SynthRANError("deployment manifest controller versions do not match the active shell")
        with scoped_environment(authority_environment):
            verification = verify_network_path(
                inventory=inventory_model,
                lock=lock,
                run_id=args.run_id,
                timeout_seconds=min(args.timeout, 300),
            )
        save_network_evidence(verification, network_evidence, network_manifest)
        if not verification.ready:
            raise SynthRANError("RFSIM 5G session readiness was not verified")
        _component(progress, "network", "session readiness", "accepted", event="completed")
    else:
        _component(progress, "network", "session readiness", "existing readiness evidence retained", event="resumed")
    for detail in _network_details(network_evidence):
        progress.stream.emit(f"  ✓ {detail}", stage="network", event="detail")
    progress.done("network", "READY")

    experiment_dir = args.experiment_root.expanduser().resolve() / args.run_id
    experiment_evidence = experiment_dir / "experiment-evidence.json"
    progress.start("workload", "deterministic AMBER source and PDU-bound transport")
    if not experiment_evidence.is_file():
        with scoped_environment(authority_environment):
            result = execute_amber_experiment(
                inventory=load_inventory(inventory),
                lock=lock,
                dependency_root=args.deps_root,
                network_manifest=network_manifest,
                network_evidence=network_evidence,
                run_id=args.run_id,
                repository_root=repository_root(),
                run_root=args.experiment_root,
                collection_seconds=args.collection_seconds,
                minimum_per_sensor=args.minimum_per_sensor,
                iot_profile=args.iot_profile,
                iot_seed=args.iot_seed,
                sensor_period_seconds=args.sensor_period,
                energy_power_scale=args.energy_power_scale,
                energy_node_variation=args.energy_node_variation,
                progress=progress.child_stream,
            )
        if not result.ready:
            raise SynthRANError("RFSIM AMBER workload was not accepted")
        for detail in _amber_details(experiment_dir):
            progress.stream.emit(f"  ✓ {detail}", stage="workload", event="detail")
        progress.done("workload", "accepted")
    else:
        for detail in _amber_details(experiment_dir):
            progress.stream.emit(f"  ✓ {detail}", stage="workload", event="detail")
        progress.resumed("workload", "accepted experiment evidence retained")

    progress.start("acceptance", "verify experiment evidence")
    accepted_payload = _read_json(experiment_evidence)
    if accepted_payload is None:
        raise SynthRANError("RFSIM experiment evidence is unreadable")
    if accepted_payload.get("ready") is not True:
        raise SynthRANError("RFSIM experiment evidence is not accepted")
    progress.done("acceptance", str(experiment_evidence))
    progress.skipped("cleanup", "virtual workload cleanup is run-scoped")

    assert controller.post5g_network is not None
    return {
        "schema": "synthran/run/v1alpha2",
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
        "event_path": str(progress.event_path),
    }


def execute_run(args: argparse.Namespace) -> dict[str, object]:
    """Execute one full lifecycle run through the canonical event stream."""

    if args.command != "run":
        raise SynthRANError("unsupported lifecycle command")
    progress = RunProgress(
        enabled=not args.quiet,
        run_id=args.run_id,
        radio=args.radio,
    )
    try:
        validate_run_id(args.run_id)
        if args.core_node == args.ran_node:
            raise SynthRANError("core and RAN nodes must differ")
        return _run_r2lab(args, progress) if args.radio == "r2lab" else _run_rfsim(args, progress)
    except SynthRANError as exc:
        stage = progress.current_stage or "run"
        terminal_enabled = progress.stream.terminal_enabled
        progress.stream.terminal_enabled = False
        try:
            progress.fail(str(exc))
        finally:
            progress.stream.terminal_enabled = terminal_enabled
        raise SynthRANError(str(exc) if stage == "run" else f"{stage}: {exc}") from exc
    except Exception as exc:
        stage = progress.current_stage or "run"
        terminal_enabled = progress.stream.terminal_enabled
        progress.stream.terminal_enabled = False
        try:
            progress.fail(str(exc))
        finally:
            progress.stream.terminal_enabled = terminal_enabled
        raise SynthRANError(str(exc) if stage == "run" else f"{stage}: {exc}") from exc
    finally:
        progress.close()
