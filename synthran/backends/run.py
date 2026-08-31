"""One sequential run implementation across SynthRAN radio backends."""

from __future__ import annotations

import argparse
from contextlib import redirect_stdout
import json
import os
from pathlib import Path
import shlex
from typing import Mapping

from synthran import command_runtime
from synthran.backends.base import BackendError
from synthran.dependencies import load_lock
from synthran.experiment.live import DEFAULT_COLLECTION_SECONDS, DEFAULT_MINIMUM_PER_SENSOR
from synthran.fiveg_ansible import parse_inventory
from synthran.live_preflight import (
    LivePreflightError,
    load_fresh_live_evidence,
    subprocess_runner as cluster_runner,
)
from synthran.network.resources import SUPPORTED_NODES, build_preparation_inventory
from synthran.network.runtime import validate_run_id
from synthran.provider import ensure_slices_provider_context
from synthran.r2lab.acceptance import PhysicalAcceptanceStage, PhysicalRunEvidence
from synthran.r2lab.foundation_topology import accept_topology_foundation
from synthran.r2lab.hardware import RADIOS, UES, PhysicalTopology
from synthran.r2lab.lifecycle import continue_physical_path, run_physical_workload
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
from synthran.utils.environment import scoped_environment
from synthran.utils.ssh import strict_ssh_command


_EXECUTABLE_DEVICES = tuple(sorted(name for name, profile in RADIOS.items() if profile.executable))
_EXECUTABLE_UES = tuple(sorted(name for name, profile in UES.items() if profile.executable))


def _subparsers(parser: argparse.ArgumentParser) -> argparse._SubParsersAction:
    for action in parser._actions:
        if isinstance(action, argparse._SubParsersAction):
            return action
    raise BackendError("SynthRAN parser does not expose top-level commands")


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


def _require_owner(args: argparse.Namespace) -> str:
    if not args.owner:
        raise BackendError("run requires --owner or SYNTHRAN_OWNER")
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
        raise BackendError(str(exc)) from exc
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


def _default_progress(args: argparse.Namespace) -> RunProgress:
    return RunProgress(
        enabled=False,
        run_id=args.run_id,
        radio=args.radio,
        network_root=(
            args.network_run_root if args.radio == "rfsim" else args.r2lab_run_root
        ),
        experiment_root=(
            args.experiment_root if args.radio == "rfsim" else args.r2lab_experiment_root
        ),
    )


def _run_r2lab(
    args: argparse.Namespace, progress: RunProgress | None = None
) -> dict[str, object]:
    progress = progress or _default_progress(args)
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

    if resumed_physical_run:
        progress.start("resources", f"reconcile existing {topology.radio} + {topology.ue} claim")
        stored = load_topology(run_root=run_root, run_id=args.run_id).validate()
        if stored != topology:
            raise BackendError("existing physical run topology does not match requested run")
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
        progress.resumed("resources", "existing physical authority reconciled")
    else:
        progress.start("resources", f"prepare {topology.radio} + {topology.ue}")
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
        progress.done("resources", "physical claim held")

    evidence_path = run_directory / "physical-run.json"
    evidence = PhysicalRunEvidence.read_json(evidence_path) if evidence_path.is_file() else None
    live_resume_status: Mapping[str, object] | None = None
    if (
        resumed_physical_run
        and evidence is not None
        and not evidence.acceptance.accepted
        and evidence.acceptance.outcome_for(PhysicalAcceptanceStage.OPEN5GS).value == "passed"
    ):
        progress.start("live resume", "re-prove current foundation, gNB/N2 and UE path")
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
        progress.resumed("live resume", "current physical prerequisites re-proven")

    if evidence is None or (
        evidence.acceptance.outcome_for(PhysicalAcceptanceStage.OPEN5GS).value != "passed"
    ):
        progress.start("foundation", "prove Kubernetes, Open5GS and physical networks")
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
        progress.done("foundation", "physical foundation ready")
    else:
        foundation_status = {
            "status": "resumed",
            "next_stage": evidence.acceptance.next_stage.value if evidence.acceptance.next_stage else None,
        }
        progress.resumed("foundation", "accepted foundation evidence present; current state re-proven")

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
        progress.resumed("gNB staging", "immutable staged artifact retained")

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
        progress.resumed("gNB/N2", "historical gNB/N2 evidence retained; current N2 re-proven")

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
            lock_path=args.lock,
            deps_root=args.deps_root,
            run_root=run_root,
            timeout_seconds=min(args.timeout, 300),
            progress=progress.child_stream,
        )
        if not path.ready_for_workload:
            raise BackendError(f"physical path stopped at {path.failed_stage or path.next_stage}")
        path_status: Mapping[str, object] = path.to_dict()
        progress.done("UE path", "registration, PDU and user-plane proof accepted")
    else:
        path_status = {"status": "resumed", "measurement_peer": peer}
        progress.resumed("UE path", "historical path evidence retained; current path re-proven")

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
            progress=progress.child_stream,
        )
        if not workload.accepted:
            raise BackendError("physical deterministic workload was not accepted")
        workload_status: Mapping[str, object] = workload.to_dict()
        progress.done("workload", "deterministic workload accepted")
    else:
        workload_status = {"status": "resumed-accepted"}
        progress.resumed("workload", "accepted workload evidence present")

    progress.start("acceptance", "verify complete physical run evidence")
    evidence = PhysicalRunEvidence.read_json(evidence_path)
    if not evidence.acceptance.accepted:
        raise BackendError("physical run did not reach acceptance")
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
        "event_path": str(progress.event_path) if progress.event_path is not None else None,
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
    args: argparse.Namespace, progress: RunProgress | None = None
) -> dict[str, object]:
    progress = progress or _default_progress(args)
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
        with redirect_stdout(progress.child_stream):
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
    known_hosts_value = authority.get("SYNTHRAN_KNOWN_HOSTS")
    if not reservation_id or not allocation_id:
        raise BackendError("prepared RFSIM authority is missing reservation/allocation IDs")
    if not known_hosts_value:
        raise BackendError("prepared RFSIM authority is missing strict SSH trust")
    known_hosts = Path(known_hosts_value).expanduser().resolve()
    if not known_hosts.is_file():
        raise BackendError("prepared RFSIM strict known-hosts file is missing")

    try:
        inventory_model = parse_inventory(
            inventory.read_text(encoding="utf-8"),
            source=inventory,
        )
    except OSError as exc:
        raise BackendError("prepared RFSIM inventory is unavailable") from exc
    lock = load_lock(args.lock)
    authority_environment = {"SYNTHRAN_KNOWN_HOSTS": str(known_hosts)}

    network_dir = args.network_run_root.expanduser().resolve() / args.run_id
    network_manifest = network_dir / "manifest.json"
    network_deployed = network_manifest.is_file()
    preflight = prep_dir / "live-preflight.json"

    if network_deployed:
        progress.resumed("preflight", "existing network deployment already consumed preflight authority")
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
            progress.start("preflight", "verify live provider and compute authority")
            with scoped_environment(authority_environment), redirect_stdout(progress.child_stream):
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
            progress.resumed("preflight", "fresh READY preflight evidence present")

    if not network_deployed:
        progress.start("network", "deploy RFSIM 5G network")
        with scoped_environment(authority_environment), redirect_stdout(progress.child_stream):
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
        progress.start("path", "verify RFSIM 5G session readiness")
        with scoped_environment(authority_environment), redirect_stdout(progress.child_stream):
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
            raise BackendError("RFSIM 5G session readiness was not verified")
        progress.done("path", "5G session readiness accepted")
    else:
        progress.resumed("path", "existing network readiness evidence present")

    experiment_dir = args.experiment_root.expanduser().resolve() / args.run_id
    experiment_evidence = experiment_dir / "experiment-evidence.json"
    if not experiment_evidence.is_file():
        progress.start("workload", "run deterministic AMBER experiment and collect data")
        with scoped_environment(authority_environment), redirect_stdout(progress.child_stream):
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
            raise BackendError("RFSIM AMBER workload was not accepted")
        progress.done("workload", "AMBER workload completed")
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
        "event_path": str(progress.event_path) if progress.event_path is not None else None,
    }
