"""Single command-line interface for SynthRAN."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
from typing import Mapping, Sequence

from synthran.dependencies import DependencyError, load_lock, sync_dependencies
from synthran.experiment import ExperimentError, build_scenario
from synthran.experiment.runtime import (
    DEFAULT_COLLECTION_SECONDS,
    DEFAULT_MINIMUM_PER_SENSOR,
    execute_experiment,
)
from synthran.fiveg_ansible import (
    FiveGAnsibleError,
    build_network_plan,
    load_inventory,
    run_offline_doctor,
)
from synthran.live_preflight import (
    LivePreflightError,
    run_live_preflight,
    save_live_evidence,
)
from synthran.r2lab.controller import (
    DEFAULT_TIMEOUT_SECONDS as DEFAULT_R2LAB_TIMEOUT_SECONDS,
    R2LabResourceError,
    R2LabSelection,
    SUPPORTED_QFITS,
    SUPPORTED_QHATS,
    SUPPORTED_RADIOS,
    build_plan as build_r2lab_plan,
    execute_prepare as execute_r2lab_prepare,
    execute_release as execute_r2lab_release,
    run_doctor as run_r2lab_doctor,
)
from synthran.r2lab.foundation import (
    R2LabPhysicalFoundationError,
    execute_physical_foundation_acceptance,
)
from synthran.r2lab.deployment import PhysicalChartBindings
from synthran.r2lab.gnb import (
    R2LabPhysicalGnbError,
    execute_physical_gnb_n2_acceptance,
    execute_physical_gnb_staging,
)
from synthran.network.resources import (
    DEFAULT_DURATION_MINUTES,
    ResourcePreparationError,
    build_resource_preparation_plan,
    execute_resource_preparation,
)
from synthran.network.runtime import (
    NetworkRuntimeError,
    execute_network_deployment,
    load_deployment_manifest,
    save_network_evidence,
    verify_network_path,
)
from synthran.privacy import (
    PrivacyError,
    outgoing_commits,
    redact_file,
    report_findings,
    repository_root,
    scan_commits,
    scan_history,
    scan_worktree,
)
from synthran.research import (
    CampaignCondition,
    LoadSpec,
    MeasurementSpec,
    ResearchCampaign,
    ResearchError,
    ResearchExperimentSpec,
    analyze_campaign,
    build_campaign,
    load_run_summary,
    save_campaign,
)
from synthran.research.runtime import calibrate_capacity, execute_research_experiment
from synthran.slices_controller import (
    DEFAULT_CONTROLLER_TIMEOUT_SECONDS,
    SlicesControllerError,
    verify_slices_controller,
)


def _network_paths(root: Path, run_id: str) -> tuple[Path, Path]:
    directory = root.resolve() / run_id
    return directory / "manifest.json", directory / "network-evidence.json"


def _add_research_spec_arguments(
    parser: argparse.ArgumentParser, *, require_target: bool
) -> None:
    parser.add_argument("--campaign-id", required=True)
    parser.add_argument("--network-run-id", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--condition", required=True)
    parser.add_argument("--seed", type=int, default=424242)
    parser.add_argument("--sensor-period", type=int, default=10)
    parser.add_argument("--warmup-seconds", type=int, default=30)
    parser.add_argument("--duration-seconds", type=int, default=180)
    parser.add_argument("--sample-interval", type=float, default=1.0)
    parser.add_argument("--probe-interval", type=float, default=1.0)
    parser.add_argument("--target-bps", type=int)
    parser.add_argument("--target-fraction", type=float)
    parser.add_argument("--reference-capacity-bps", type=int)
    parser.add_argument("--parallel-flows", type=int, default=1)
    parser.add_argument("--load-port", type=int, default=5201)
    parser.add_argument("--probe-target", required=require_target)


def _add_research_parser(
    commands: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    research = commands.add_parser(
        "research",
        help="plan, run, schedule, or analyze controlled measurement experiments",
    )
    sub = research.add_subparsers(dest="research_command", required=True)

    plan = sub.add_parser(
        "plan", help="render one immutable controlled-experiment specification"
    )
    _add_research_spec_arguments(plan, require_target=False)

    run = sub.add_parser(
        "run", help="execute one controlled fixed-window measurement"
    )
    _add_research_spec_arguments(run, require_target=True)
    run.add_argument("--inventory", type=Path, required=True)
    run.add_argument("--lock", type=Path, default=Path("dependencies.lock.yml"))
    run.add_argument("--deps-root", type=Path, default=Path(".deps"))
    run.add_argument(
        "--network-run-root",
        type=Path,
        default=Path(".synthran/runs"),
        help=argparse.SUPPRESS,
    )
    run.add_argument(
        "--run-root",
        type=Path,
        default=Path(".synthran/experiments"),
        help=argparse.SUPPRESS,
    )

    calibrate = sub.add_parser(
        "calibrate", help="measure reference UE-path capacity"
    )
    calibrate.add_argument("--inventory", type=Path, required=True)
    calibrate.add_argument("--network-run-id", required=True)
    calibrate.add_argument("--target", required=True)
    calibrate.add_argument("--duration-seconds", type=int, default=10)
    calibrate.add_argument("--server-port", type=int, default=5201)
    calibrate.add_argument("--lock", type=Path, default=Path("dependencies.lock.yml"))
    calibrate.add_argument("--out", type=Path, required=True)

    campaign_plan = sub.add_parser(
        "campaign-plan", help="create a deterministic blocked campaign schedule"
    )
    campaign_plan.add_argument("--campaign-id", required=True)
    campaign_plan.add_argument("--network-run-id", required=True)
    campaign_plan.add_argument("--seeds", required=True)
    campaign_plan.add_argument("--conditions", required=True)
    campaign_plan.add_argument("--campaign-seed", type=int, required=True)
    campaign_plan.add_argument("--out", type=Path, required=True)

    campaign_run = sub.add_parser(
        "campaign-run", help="execute a persisted campaign sequentially"
    )
    campaign_run.add_argument("--campaign", type=Path, required=True)
    campaign_run.add_argument("--inventory", type=Path, required=True)
    campaign_run.add_argument("--target", required=True)
    campaign_run.add_argument("--reference-capacity-bps", type=int)
    campaign_run.add_argument("--sensor-period", type=int, default=10)
    campaign_run.add_argument("--warmup-seconds", type=int, default=30)
    campaign_run.add_argument("--duration-seconds", type=int, default=180)
    campaign_run.add_argument("--sample-interval", type=float, default=1.0)
    campaign_run.add_argument("--probe-interval", type=float, default=1.0)
    campaign_run.add_argument("--parallel-flows", type=int, default=1)
    campaign_run.add_argument("--load-port", type=int, default=5201)
    campaign_run.add_argument(
        "--lock", type=Path, default=Path("dependencies.lock.yml")
    )
    campaign_run.add_argument("--deps-root", type=Path, default=Path(".deps"))
    campaign_run.add_argument(
        "--network-run-root",
        type=Path,
        default=Path(".synthran/runs"),
        help=argparse.SUPPRESS,
    )
    campaign_run.add_argument(
        "--run-root",
        type=Path,
        default=Path(".synthran/experiments"),
        help=argparse.SUPPRESS,
    )

    analyze = sub.add_parser(
        "analyze", help="analyze persisted run summaries without live access"
    )
    analyze.add_argument("--campaign", type=Path, required=True)
    analyze.add_argument(
        "--run-root",
        type=Path,
        default=Path(".synthran/experiments"),
        help=argparse.SUPPRESS,
    )
    analyze.add_argument("--out", type=Path, required=True)


def _add_experiment_parser(
    subcommands: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    experiment = subcommands.add_parser(
        "experiment",
        help="plan, run, verify, or measure deterministic IoT-to-5G experiments",
    )
    commands = experiment.add_subparsers(dest="experiment_command", required=True)

    plan = commands.add_parser(
        "plan",
        help="validate a path-proven network and print the experiment scenario",
    )
    plan.add_argument("--network-run-id", required=True)
    plan.add_argument("--run-id", required=True)
    plan.add_argument(
        "--network-run-root",
        type=Path,
        default=Path(".synthran/runs"),
        help=argparse.SUPPRESS,
    )

    run = commands.add_parser(
        "run",
        help="run the ten-sensor experiment against a path-proven network",
    )
    run.add_argument("--inventory", type=Path, required=True)
    run.add_argument("--network-run-id", required=True)
    run.add_argument("--run-id", required=True)
    run.add_argument("--lock", type=Path, default=Path("dependencies.lock.yml"))
    run.add_argument("--deps-root", type=Path, default=Path(".deps"))
    run.add_argument(
        "--network-run-root",
        type=Path,
        default=Path(".synthran/runs"),
        help=argparse.SUPPRESS,
    )
    run.add_argument(
        "--run-root",
        type=Path,
        default=Path(".synthran/experiments"),
        help=argparse.SUPPRESS,
    )
    run.add_argument(
        "--collection-seconds",
        type=int,
        default=DEFAULT_COLLECTION_SECONDS,
    )
    run.add_argument(
        "--minimum-per-sensor",
        type=int,
        default=DEFAULT_MINIMUM_PER_SENSOR,
    )

    verify = commands.add_parser(
        "verify",
        help="read persisted experiment acceptance evidence without changing live state",
    )
    verify.add_argument("--run-id", required=True)
    verify.add_argument(
        "--run-root",
        type=Path,
        default=Path(".synthran/experiments"),
        help=argparse.SUPPRESS,
    )

    _add_research_parser(commands)


def _add_slices_context(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--slices-project",
        default=os.environ.get("SYNTHRAN_SLICES_PROJECT"),
        help="selected SLICES project (or SYNTHRAN_SLICES_PROJECT)",
    )
    parser.add_argument(
        "--slices-experiment",
        default=os.environ.get("SYNTHRAN_SLICES_EXPERIMENT"),
        help="existing SLICES experiment (or SYNTHRAN_SLICES_EXPERIMENT)",
    )


def _add_r2lab_selection_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--slice",
        dest="r2lab_slice",
        default=os.environ.get("SYNTHRAN_R2LAB_SLICE"),
        help="R2Lab slice name (or SYNTHRAN_R2LAB_SLICE)",
    )
    parser.add_argument("--radio", required=True, choices=sorted(SUPPORTED_RADIOS))
    parser.add_argument(
        "--ue", required=True, choices=sorted(SUPPORTED_QHATS | SUPPORTED_QFITS)
    )
    parser.add_argument(
        "--timeout", type=int, default=DEFAULT_R2LAB_TIMEOUT_SECONDS
    )


def _add_physical_authority_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--slice",
        dest="r2lab_slice",
        default=os.environ.get("SYNTHRAN_R2LAB_SLICE"),
        help="R2Lab slice name (or SYNTHRAN_R2LAB_SLICE)",
    )
    parser.add_argument(
        "--owner",
        default=os.environ.get("SYNTHRAN_OWNER"),
        help="expected SLICES/POS owner (or SYNTHRAN_OWNER)",
    )
    parser.add_argument(
        "--reservation-id",
        default=os.environ.get("SYNTHRAN_RESERVATION_ID"),
        help="expected active reservation identifier",
    )
    parser.add_argument(
        "--allocation-id",
        default=os.environ.get("SYNTHRAN_ALLOCATION_ID"),
        help="expected current allocation identifier",
    )
    parser.add_argument(
        "--known-hosts",
        type=Path,
        default=os.environ.get("SYNTHRAN_SLICES_KNOWN_HOSTS"),
        help="strict SLICES known-hosts path",
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="synthran")
    subcommands = parser.add_subparsers(dest="command", required=True)
    _add_experiment_parser(subcommands)

    slices = subcommands.add_parser(
        "slices", help="verify the SLICES CLI controller context"
    )
    slices_commands = slices.add_subparsers(dest="slices_command", required=True)
    slices_doctor = slices_commands.add_parser(
        "doctor", help="read-only SLICES login, project, and experiment checks"
    )
    slices_doctor.add_argument("--lock", type=Path, default=Path("dependencies.lock.yml"))
    slices_doctor.add_argument(
        "--timeout", type=int, default=DEFAULT_CONTROLLER_TIMEOUT_SECONDS
    )
    _add_slices_context(slices_doctor)

    r2lab = subcommands.add_parser(
        "r2lab", help="verify and control exact R2Lab radio resources"
    )
    r2lab_commands = r2lab.add_subparsers(dest="r2lab_command", required=True)

    r2lab_doctor = r2lab_commands.add_parser(
        "doctor", help="verify Faraday access and an active R2Lab lease"
    )
    _add_r2lab_selection_arguments(r2lab_doctor)
    r2lab_doctor.add_argument("--json", action="store_true")

    r2lab_plan = r2lab_commands.add_parser(
        "plan", help="print exact R2Lab resource actions without executing them"
    )
    _add_r2lab_selection_arguments(r2lab_plan)
    r2lab_plan.add_argument("--run-id", required=True)
    r2lab_plan.add_argument("--json", action="store_true")

    r2lab_prepare = r2lab_commands.add_parser(
        "prepare", help="claim and power one R2Lab radio and UE under an active lease"
    )
    _add_r2lab_selection_arguments(r2lab_prepare)
    r2lab_prepare.add_argument("--run-id", required=True)
    r2lab_prepare.add_argument(
        "--run-root",
        type=Path,
        default=Path(".synthran/r2lab"),
        help=argparse.SUPPRESS,
    )

    r2lab_release = r2lab_commands.add_parser(
        "release", help="power off only resources owned by one SynthRAN R2Lab run"
    )
    _add_physical_authority_arguments(r2lab_release)
    r2lab_release.add_argument("--run-id", required=True)
    r2lab_release.add_argument(
        "--timeout", type=int, default=DEFAULT_R2LAB_TIMEOUT_SECONDS
    )
    r2lab_release.add_argument(
        "--run-root",
        type=Path,
        default=Path(".synthran/r2lab"),
        help=argparse.SUPPRESS,
    )

    r2lab_foundation = r2lab_commands.add_parser(
        "foundation",
        help="verify and bind the stopped SLICES and Open5GS foundation",
    )
    _add_physical_authority_arguments(r2lab_foundation)
    r2lab_foundation.add_argument("--run-id", required=True)
    r2lab_foundation.add_argument("--previous-run-id", required=True)
    r2lab_foundation.add_argument("--timeout", type=int, default=120)
    r2lab_foundation.add_argument("--json", action="store_true")
    r2lab_foundation.add_argument(
        "--run-root",
        type=Path,
        default=Path(".synthran/r2lab"),
        help=argparse.SUPPRESS,
    )

    r2lab_gnb_stage = r2lab_commands.add_parser(
        "gnb-stage",
        help="render and stage the physical gNB while it remains stopped",
    )
    _add_physical_authority_arguments(r2lab_gnb_stage)
    r2lab_gnb_stage.add_argument("--run-id", required=True)
    r2lab_gnb_stage.add_argument("--amf-n2-address")
    r2lab_gnb_stage.add_argument("--gnb-n2-address")
    r2lab_gnb_stage.add_argument("--n300-address")
    r2lab_gnb_stage.add_argument("--ru-pod-address")
    r2lab_gnb_stage.add_argument("--ru-subnet")
    r2lab_gnb_stage.add_argument(
        "--lock", type=Path, default=Path("dependencies.lock.yml")
    )
    r2lab_gnb_stage.add_argument("--deps-root", type=Path, default=Path(".deps"))
    r2lab_gnb_stage.add_argument("--timeout", type=int, default=120)
    r2lab_gnb_stage.add_argument("--json", action="store_true")
    r2lab_gnb_stage.add_argument(
        "--run-root",
        type=Path,
        default=Path(".synthran/r2lab"),
        help=argparse.SUPPRESS,
    )

    r2lab_gnb_start = r2lab_commands.add_parser(
        "gnb-start",
        help="start one physical gNB and prove its N2 association",
    )
    _add_physical_authority_arguments(r2lab_gnb_start)
    r2lab_gnb_start.add_argument("--run-id", required=True)
    r2lab_gnb_start.add_argument("--timeout", type=int, default=120)
    r2lab_gnb_start.add_argument("--n2-attempts", type=int, default=12)
    r2lab_gnb_start.add_argument("--n2-interval", type=float, default=5.0)
    r2lab_gnb_start.add_argument("--json", action="store_true")
    r2lab_gnb_start.add_argument(
        "--run-root",
        type=Path,
        default=Path(".synthran/r2lab"),
        help=argparse.SUPPRESS,
    )

    deps = subcommands.add_parser("deps", help="manage immutable external dependencies")
    deps_commands = deps.add_subparsers(dest="deps_command", required=True)
    sync = deps_commands.add_parser("sync", help="synchronize detached pinned checkouts")
    sync.add_argument("--lock", type=Path, default=Path("dependencies.lock.yml"))
    sync.add_argument("--root", type=Path, default=Path(".deps"))
    dependency_selection = sync.add_mutually_exclusive_group()
    dependency_selection.add_argument(
        "--all", action="store_true", help="include transitive repositories"
    )
    dependency_selection.add_argument(
        "--name",
        action="append",
        dest="dependency_names",
        metavar="DEPENDENCY",
        help="synchronize one named dependency; repeat to select more than one",
    )
    sync.add_argument("--dry-run", action="store_true")

    privacy = subcommands.add_parser("privacy", help="scan or redact sensitive context")
    privacy_commands = privacy.add_subparsers(dest="privacy_command", required=True)
    scan = privacy_commands.add_parser("scan", help="fail when sensitive context is detected")
    scan_mode = scan.add_mutually_exclusive_group()
    scan_mode.add_argument("--worktree", action="store_true", help="scan tracked and unignored files")
    scan_mode.add_argument("--history", action="store_true", help="scan every Git commit")
    scan_mode.add_argument(
        "--outgoing",
        action="store_true",
        help="scan pre-push updates read from standard input",
    )
    scan.add_argument("--remote", default="origin", help="remote name used with --outgoing")

    redact = privacy_commands.add_parser("redact", help="write a sanitized text derivative")
    redact.add_argument("source", type=Path)
    redact.add_argument("destination", type=Path)
    redact.add_argument("--dry-run", action="store_true")

    hooks = subcommands.add_parser("hooks", help="configure repository-local Git hooks")
    hooks_commands = hooks.add_subparsers(dest="hooks_command", required=True)
    install = hooks_commands.add_parser("install", help="activate the tracked .githooks directory")
    install.add_argument("--dry-run", action="store_true")

    doctor = subcommands.add_parser("doctor", help="validate deployment prerequisites")
    _add_slices_context(doctor)
    doctor.add_argument("--inventory", type=Path, required=True)
    doctor.add_argument("--lock", type=Path, default=Path("dependencies.lock.yml"))
    doctor.add_argument("--deps-root", type=Path, default=Path(".deps"))
    doctor.add_argument(
        "--offline",
        action="store_true",
        help="validate only inventory, lock, and pinned checkout state",
    )
    doctor.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_CONTROLLER_TIMEOUT_SECONDS,
        help="timeout in seconds for each read-only live probe",
    )
    doctor.add_argument(
        "--owner",
        default=os.environ.get("SYNTHRAN_OWNER"),
        help="expected current SLICES/POS owner",
    )
    doctor.add_argument(
        "--reservation-id",
        default=os.environ.get("SYNTHRAN_RESERVATION_ID"),
        help="expected active reservation identifier",
    )
    doctor.add_argument(
        "--allocation-id",
        default=os.environ.get("SYNTHRAN_ALLOCATION_ID"),
        help="expected current allocation identifier",
    )
    doctor.add_argument(
        "--evidence-out",
        type=Path,
        help="write sanitized live readiness evidence (required for live doctor)",
    )

    network = subcommands.add_parser("network", help="plan or deploy the 5G network")
    network_commands = network.add_subparsers(dest="network_command", required=True)
    prepare = network_commands.add_parser(
        "prepare",
        help="explicitly reserve, allocate, image, and prepare a SLICES node pair",
    )
    _add_slices_context(prepare)
    prepare.add_argument("--core-node", default="sopnode-f2")
    prepare.add_argument("--ran-node", default="sopnode-f3")
    prepare.add_argument(
        "--owner",
        default=os.environ.get("SYNTHRAN_OWNER"),
        help="expected SLICES/POS owner (or SYNTHRAN_OWNER)",
    )
    prepare.add_argument(
        "--reservation-id",
        default=os.environ.get("SYNTHRAN_RESERVATION_ID"),
        help="reuse an active reservation instead of creating one",
    )
    prepare.add_argument(
        "--duration-minutes",
        type=int,
        default=DEFAULT_DURATION_MINUTES,
    )
    prepare.add_argument("--run-id", required=True)
    prepare.add_argument("--lock", type=Path, default=Path("dependencies.lock.yml"))
    prepare.add_argument("--deps-root", type=Path, default=Path(".deps"))
    prepare.add_argument("--dry-run", action="store_true")
    prepare.add_argument("--json", action="store_true", help="emit a redacted JSON plan")
    prepare.add_argument(
        "--run-root",
        type=Path,
        default=Path(".synthran/preparations"),
        help=argparse.SUPPRESS,
    )
    prepare.add_argument(
        "--timeout",
        type=int,
        default=3600,
        help="timeout in seconds for each preparation stage",
    )

    deploy = network_commands.add_parser(
        "deploy", help="plan the explicit 5G network deployment"
    )
    _add_slices_context(deploy)
    deploy.add_argument("--inventory", type=Path, required=True)
    deploy.add_argument("--profile", default="default")
    deploy.add_argument("--lock", type=Path, default=Path("dependencies.lock.yml"))
    deploy.add_argument("--deps-root", type=Path, default=Path(".deps"))
    deploy.add_argument("--dry-run", action="store_true")
    deploy.add_argument("--json", action="store_true", help="emit a redacted JSON plan")
    deploy.add_argument(
        "--owner",
        default=os.environ.get("SYNTHRAN_OWNER"),
        help="expected current SLICES/POS owner",
    )
    deploy.add_argument(
        "--reservation-id",
        default=os.environ.get("SYNTHRAN_RESERVATION_ID"),
        help="expected active reservation identifier",
    )
    deploy.add_argument(
        "--allocation-id",
        default=os.environ.get("SYNTHRAN_ALLOCATION_ID"),
        help="expected current allocation identifier",
    )
    deploy.add_argument(
        "--preflight-evidence",
        type=Path,
        help="fresh READY evidence written by the live doctor",
    )
    deploy.add_argument("--run-id", help="unique lowercase run identifier")
    deploy.add_argument(
        "--run-root",
        type=Path,
        default=Path(".synthran/runs"),
        help=argparse.SUPPRESS,
    )
    deploy.add_argument(
        "--timeout",
        type=int,
        default=3600,
        help="timeout in seconds for each deployment stage",
    )

    verify = network_commands.add_parser(
        "verify", help="record gNB, srsUE, PDU tunnel, and UPF route evidence"
    )
    _add_slices_context(verify)
    verify.add_argument("--inventory", type=Path, required=True)
    verify.add_argument("--lock", type=Path, default=Path("dependencies.lock.yml"))
    verify.add_argument("--deps-root", type=Path, default=Path(".deps"))
    verify.add_argument("--run-id", required=True)
    verify.add_argument(
        "--run-root",
        type=Path,
        default=Path(".synthran/runs"),
        help=argparse.SUPPRESS,
    )
    verify.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="timeout in seconds for each read-only proof command",
    )
    return parser


def _require_slices_context(
    args: argparse.Namespace, operation: str
) -> tuple[str, str]:
    required = {
        "--slices-project": args.slices_project,
        "--slices-experiment": args.slices_experiment,
    }
    missing = [name for name, value in required.items() if value is None]
    if missing:
        raise SlicesControllerError(f"{operation} requires " + ", ".join(missing))
    return args.slices_project, args.slices_experiment


def _slices_doctor(args: argparse.Namespace) -> int:
    project, experiment = _require_slices_context(args, "SLICES doctor")
    lock = load_lock(args.lock)
    report = verify_slices_controller(
        lock=lock,
        project=project,
        experiment=experiment,
        timeout_seconds=args.timeout,
    )
    print(report.render())
    return 0


def _r2lab_selection(args: argparse.Namespace) -> R2LabSelection:
    if args.r2lab_slice is None:
        raise R2LabResourceError(
            "R2Lab control requires --slice or SYNTHRAN_R2LAB_SLICE"
        )
    return R2LabSelection.build(
        slice_name=args.r2lab_slice,
        radio=args.radio,
        ue=args.ue,
    )


def _require_physical_authority(
    args: argparse.Namespace, operation: str
) -> tuple[str, str, str | None, str | None, Path]:
    required = {
        "--slice or SYNTHRAN_R2LAB_SLICE": args.r2lab_slice,
        "--owner or SYNTHRAN_OWNER": args.owner,
        "--known-hosts or SYNTHRAN_SLICES_KNOWN_HOSTS": args.known_hosts,
    }
    missing = [name for name, value in required.items() if value is None]
    if missing:
        raise R2LabPhysicalGnbError(
            f"{operation} requires " + ", ".join(missing)
        )
    return (
        args.r2lab_slice,
        args.owner,
        args.reservation_id,
        args.allocation_id,
        Path(args.known_hosts),
    )


def _physical_chart_bindings(args: argparse.Namespace) -> PhysicalChartBindings | None:
    values = {
        "--amf-n2-address": args.amf_n2_address,
        "--gnb-n2-address": args.gnb_n2_address,
        "--n300-address": args.n300_address,
        "--ru-pod-address": args.ru_pod_address,
        "--ru-subnet": args.ru_subnet,
    }
    supplied = {name for name, value in values.items() if value is not None}
    if not supplied:
        return None
    missing = [name for name, value in values.items() if value is None]
    if missing:
        raise R2LabPhysicalGnbError(
            "explicit physical chart bindings require " + ", ".join(missing)
        )
    return PhysicalChartBindings(
        amf_n2_address=args.amf_n2_address,
        gnb_n2_address=args.gnb_n2_address,
        n300_address=args.n300_address,
        ru_pod_address=args.ru_pod_address,
        ru_subnet=args.ru_subnet,
    )


def _dispatch_r2lab(args: argparse.Namespace) -> int:
    if args.r2lab_command == "doctor":
        report = run_r2lab_doctor(
            selection=_r2lab_selection(args),
            timeout_seconds=args.timeout,
        )
        print(
            json.dumps(report.to_dict(), indent=2, sort_keys=True)
            if args.json
            else report.render()
        )
        return 0 if report.ready else 2
    if args.r2lab_command == "plan":
        plan = build_r2lab_plan(
            run_id=args.run_id,
            selection=_r2lab_selection(args),
        )
        print(plan.render(as_json=args.json))
        return 0
    if args.r2lab_command == "prepare":
        plan = build_r2lab_plan(
            run_id=args.run_id,
            selection=_r2lab_selection(args),
        )
        result = execute_r2lab_prepare(
            plan=plan,
            run_root=args.run_root,
            timeout_seconds=args.timeout,
            progress=sys.stdout,
        )
        print(f"R2Lab resources prepared for run {result.run_id}.")
        print(f"Sanitized manifest: {result.manifest_path}")
        print(f"Sanitized log: {result.log_path}")
        return 0
    if args.r2lab_command == "release":
        if args.r2lab_slice is None:
            raise R2LabResourceError(
                "R2Lab release requires --slice or SYNTHRAN_R2LAB_SLICE"
            )
        result = execute_r2lab_release(
            run_id=args.run_id,
            slice_name=args.r2lab_slice,
            run_root=args.run_root,
            timeout_seconds=args.timeout,
            progress=sys.stdout,
            owner=args.owner,
            reservation_id=args.reservation_id,
            allocation_id=args.allocation_id,
            known_hosts=(Path(args.known_hosts) if args.known_hosts else None),
        )
        print(f"R2Lab resources released for run {result.run_id}.")
        print(f"Sanitized manifest: {result.manifest_path}")
        return 0
    if args.r2lab_command == "foundation":
        required = {
            "--slice or SYNTHRAN_R2LAB_SLICE": args.r2lab_slice,
            "--owner or SYNTHRAN_OWNER": args.owner,
            "--reservation-id or SYNTHRAN_RESERVATION_ID": args.reservation_id,
            "--allocation-id or SYNTHRAN_ALLOCATION_ID": args.allocation_id,
            "--known-hosts or SYNTHRAN_SLICES_KNOWN_HOSTS": args.known_hosts,
        }
        missing = [name for name, value in required.items() if value is None]
        if missing:
            raise R2LabPhysicalFoundationError(
                "R2Lab foundation requires " + ", ".join(missing)
            )
        result = execute_physical_foundation_acceptance(
            run_id=args.run_id,
            previous_run_id=args.previous_run_id,
            slice_name=args.r2lab_slice,
            owner=args.owner,
            reservation_id=args.reservation_id,
            allocation_id=args.allocation_id,
            known_hosts=Path(args.known_hosts),
            now=datetime.now(timezone.utc),
            run_root=args.run_root,
            timeout_seconds=args.timeout,
        )
        print(
            json.dumps(result.to_dict(), indent=2, sort_keys=True)
            if args.json
            else (
                f"Physical foundation accepted for run {result.run_id}.\n"
                f"Sanitized evidence: {result.evidence_path}"
            )
        )
        return 0
    if args.r2lab_command == "gnb-stage":
        slice_name, owner, reservation_id, allocation_id, known_hosts = (
            _require_physical_authority(args, "physical gNB staging")
        )
        result = execute_physical_gnb_staging(
            run_id=args.run_id,
            slice_name=slice_name,
            owner=owner,
            reservation_id=reservation_id,
            allocation_id=allocation_id,
            known_hosts=known_hosts,
            now=datetime.now(timezone.utc),
            bindings=_physical_chart_bindings(args),
            lock_path=args.lock,
            deps_root=args.deps_root,
            run_root=args.run_root,
            timeout_seconds=args.timeout,
        )
        print(
            json.dumps(result.to_dict(), indent=2, sort_keys=True)
            if args.json
            else (
                f"Physical gNB staged and proven stopped for run {result.run_id}.\n"
                f"Sanitized evidence: {result.evidence_path}"
            )
        )
        return 0
    if args.r2lab_command == "gnb-start":
        slice_name, owner, reservation_id, allocation_id, known_hosts = (
            _require_physical_authority(args, "physical gNB start")
        )
        result = execute_physical_gnb_n2_acceptance(
            run_id=args.run_id,
            slice_name=slice_name,
            owner=owner,
            reservation_id=reservation_id,
            allocation_id=allocation_id,
            known_hosts=known_hosts,
            now=datetime.now(timezone.utc),
            run_root=args.run_root,
            timeout_seconds=args.timeout,
            attempts=args.n2_attempts,
            poll_interval_seconds=args.n2_interval,
        )
        print(
            json.dumps(result.to_dict(), indent=2, sort_keys=True)
            if args.json
            else (
                f"Physical gNB and N2 accepted for run {result.run_id}.\n"
                f"Sanitized evidence: {result.evidence_path}"
                if result.proven
                else (
                    f"Physical gNB N2 was not proven for run {result.run_id}.\n"
                    f"Sanitized evidence: {result.evidence_path}"
                )
            )
        )
        return 0 if result.proven else 2
    raise AssertionError("unreachable R2Lab command")


def _deps_sync(args: argparse.Namespace) -> int:
    lock = load_lock(args.lock)
    sync_dependencies(
        lock,
        args.root,
        include_transitive=args.all,
        names=args.dependency_names,
        dry_run=args.dry_run,
        output=sys.stdout,
    )
    return 0


def _privacy_scan(args: argparse.Namespace) -> int:
    repo = repository_root()
    if args.outgoing:
        commits = outgoing_commits(repo, args.remote, sys.stdin)
        findings = scan_commits(repo, commits)
    elif args.history:
        findings = scan_history(repo)
    else:
        findings = scan_worktree(repo)
    return report_findings(findings, sys.stdout)


def _hooks_install(args: argparse.Namespace) -> int:
    repo = repository_root()
    hook = repo / ".githooks" / "pre-push"
    if not hook.is_file():
        raise PrivacyError("tracked pre-push hook is missing")
    if args.dry_run:
        print("[dry-run] make .githooks/pre-push executable when required")
        print("[dry-run] git config core.hooksPath .githooks")
        return 0
    if os.name != "nt":
        hook.chmod(hook.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    try:
        subprocess.run(
            ["git", "config", "core.hooksPath", ".githooks"],
            cwd=repo,
            check=True,
            stdout=subprocess.DEVNULL,
        )
    except (FileNotFoundError, subprocess.CalledProcessError) as exc:
        raise PrivacyError("unable to configure repository hooks") from exc
    print("repository hooks activated")
    return 0


def _doctor(args: argparse.Namespace) -> int:
    offline_report = run_offline_doctor(
        inventory_path=args.inventory,
        lock_path=args.lock,
        dependency_root=args.deps_root,
    )
    print(offline_report.render())
    if args.offline:
        return 0 if offline_report.ready else 2
    if not offline_report.ready:
        print("Live probes were not run because offline readiness failed.")
        return 2
    required = {
        "--slices-project": args.slices_project,
        "--slices-experiment": args.slices_experiment,
        "--owner": args.owner,
        "--reservation-id": args.reservation_id,
        "--allocation-id": args.allocation_id,
        "--evidence-out": args.evidence_out,
    }
    missing = [name for name, value in required.items() if value is None]
    if missing:
        raise LivePreflightError("live doctor requires " + ", ".join(missing))
    inventory = load_inventory(args.inventory)
    lock = load_lock(args.lock)
    live_report = run_live_preflight(
        inventory=inventory,
        lock=lock,
        owner=args.owner,
        reservation_id=args.reservation_id,
        allocation_id=args.allocation_id,
        slices_project=args.slices_project,
        slices_experiment=args.slices_experiment,
        timeout_seconds=args.timeout,
    )
    print()
    print(live_report.render())
    save_live_evidence(live_report, args.evidence_out)
    print(f"Sanitized evidence: {args.evidence_out.name}")
    return 0 if live_report.ready else 2


def _network_prepare(args: argparse.Namespace) -> int:
    lock = load_lock(args.lock)
    plan = build_resource_preparation_plan(
        lock=lock,
        core_node=args.core_node,
        ran_node=args.ran_node,
        duration_minutes=args.duration_minutes,
        run_id=args.run_id,
        reservation_id=args.reservation_id,
    )
    if args.dry_run:
        print(plan.render(as_json=args.json))
        return 0
    if args.json:
        raise ResourcePreparationError("--json is supported only with --dry-run")
    project, experiment = _require_slices_context(args, "live preparation")
    if args.owner is None:
        raise ResourcePreparationError(
            "live preparation requires --owner or SYNTHRAN_OWNER"
        )
    result = execute_resource_preparation(
        plan=plan,
        lock=lock,
        dependency_root=args.deps_root,
        owner=args.owner,
        slices_project=project,
        slices_experiment=experiment,
        reservation_id=args.reservation_id,
        run_root=args.run_root,
        repository_root=repository_root(),
        timeout_seconds=args.timeout,
        progress=sys.stdout,
    )
    print(f"SLICES resources prepared for run {result.run_id}.")
    print("Open5GS and srsRAN were not deployed.")
    print(f"Generated inventory: {result.inventory_path}")
    print(f"Private authority: {result.authority_path}")
    print(f"Sanitized manifest: {result.manifest_path}")
    print(f"Sanitized log: {result.log_path}")
    return 0


def _network_deploy(args: argparse.Namespace) -> int:
    if not args.dry_run:
        if args.json:
            raise FiveGAnsibleError("--json is supported only with --dry-run")
        required = {
            "--slices-project": args.slices_project,
            "--slices-experiment": args.slices_experiment,
            "--owner": args.owner,
            "--reservation-id": args.reservation_id,
            "--allocation-id": args.allocation_id,
            "--preflight-evidence": args.preflight_evidence,
            "--run-id": args.run_id,
        }
        missing = [name for name, value in required.items() if value is None]
        if missing:
            raise NetworkRuntimeError(
                "live deployment requires " + ", ".join(missing)
            )

    report = run_offline_doctor(
        inventory_path=args.inventory,
        lock_path=args.lock,
        dependency_root=args.deps_root,
    )
    if not report.ready:
        print(report.render(), file=sys.stderr)
        return 2
    lock = load_lock(args.lock)
    inventory = load_inventory(args.inventory)
    plan = build_network_plan(lock=lock, inventory=inventory, profile=args.profile)
    if not args.json:
        print(report.render())
        print()
    if args.dry_run:
        print(plan.render(as_json=args.json))
        return 0
    result = execute_network_deployment(
        plan=plan,
        lock=lock,
        dependency_root=args.deps_root,
        live_evidence_path=args.preflight_evidence,
        owner=args.owner,
        reservation_id=args.reservation_id,
        allocation_id=args.allocation_id,
        slices_project=args.slices_project,
        slices_experiment=args.slices_experiment,
        run_id=args.run_id,
        run_root=args.run_root,
        repository_root=repository_root(),
        timeout_seconds=args.timeout,
        progress=sys.stdout,
    )
    print(f"Deployment completed for run {result.run_id}; path proof is still required.")
    print(f"Sanitized manifest: {result.manifest_path}")
    print(f"Sanitized log: {result.log_path}")
    return 0


def _network_verify(args: argparse.Namespace) -> int:
    report = run_offline_doctor(
        inventory_path=args.inventory,
        lock_path=args.lock,
        dependency_root=args.deps_root,
    )
    if not report.ready:
        print(report.render(), file=sys.stderr)
        return 2
    lock = load_lock(args.lock)
    project, experiment = _require_slices_context(args, "network verification")
    active_controller = verify_slices_controller(
        lock=lock,
        project=project,
        experiment=experiment,
        timeout_seconds=args.timeout,
    )
    inventory = load_inventory(args.inventory)
    run_directory = args.run_root.resolve() / args.run_id
    manifest_path = run_directory / "manifest.json"
    manifest = load_deployment_manifest(
        path=manifest_path,
        run_id=args.run_id,
        inventory=inventory,
        lock=lock,
        slices_project=project,
        slices_experiment=experiment,
    )
    if manifest.get("slices_controller") != active_controller.to_dict():
        raise NetworkRuntimeError(
            "deployment manifest controller versions do not match the active shell"
        )
    verification = verify_network_path(
        inventory=inventory,
        lock=lock,
        run_id=args.run_id,
        timeout_seconds=args.timeout,
    )
    evidence_path = run_directory / "network-evidence.json"
    save_network_evidence(verification, evidence_path, manifest_path)
    print(verification.render())
    print(f"Sanitized evidence: {evidence_path}")
    return 0 if verification.ready else 2


def _experiment_plan(args: argparse.Namespace) -> int:
    manifest, evidence = _network_paths(
        args.network_run_root, args.network_run_id
    )
    scenario = build_scenario(
        run_id=args.run_id,
        network_manifest=manifest,
        network_evidence=evidence,
    )
    print(json.dumps(scenario.to_dict(), indent=2, sort_keys=True))
    print(
        "\nPDU note: the displayed address is accepted network evidence;\n"
        "experiment execution rediscovers the live address after the srsUE rollout."
    )
    print("\nExecution action: none")
    print("Reservation action: none")
    print("Network deployment action: none")
    return 0


def _experiment_run(args: argparse.Namespace) -> int:
    manifest, evidence = _network_paths(
        args.network_run_root, args.network_run_id
    )
    result = execute_experiment(
        inventory=load_inventory(args.inventory),
        lock=load_lock(args.lock),
        dependency_root=args.deps_root,
        network_manifest=manifest,
        network_evidence=evidence,
        run_id=args.run_id,
        repository_root=repository_root(),
        run_root=args.run_root,
        collection_seconds=args.collection_seconds,
        minimum_per_sensor=args.minimum_per_sensor,
        progress=sys.stdout,
    )
    print(f"Run directory: {result.run_directory}")
    if result.evidence_path.is_file():
        print(f"Sanitized evidence: {result.evidence_path}")
    return 0 if result.ready else 2


def _experiment_verify(args: argparse.Namespace) -> int:
    run_directory = args.run_root.resolve() / args.run_id
    evidence_path = run_directory / "experiment-evidence.json"
    manifest_path = run_directory / "manifest.json"
    try:
        evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ExperimentError("experiment manifest/evidence is missing") from exc
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ExperimentError(
            "experiment manifest/evidence must be readable JSON"
        ) from exc

    if (
        not isinstance(evidence, dict)
        or evidence.get("schema") != "synthran/iot-evidence/v1alpha1"
    ):
        raise ExperimentError("experiment evidence schema is unsupported")
    if not isinstance(manifest, dict) or manifest.get("run_id") != args.run_id:
        raise ExperimentError(
            "experiment manifest does not match the requested run"
        )

    print(f"SynthRAN experiment verification ({args.run_id})")
    checks = evidence.get("checks")
    if not isinstance(checks, list):
        raise ExperimentError("experiment evidence checks are malformed")
    for check in checks:
        if not isinstance(check, dict):
            raise ExperimentError(
                "experiment evidence contains a malformed check"
            )
        state = "PASS" if check.get("passed") is True else "FAIL"
        print(f"[{state}] {check.get('name')}: {check.get('detail')}")
    ready = (
        evidence.get("ready") is True
        and manifest.get("status") == "iot-to-5g-path-proven"
    )
    print(f"Result: {'IOT-TO-5G PATH PROVEN' if ready else 'NOT PROVEN'}")
    return 0 if ready else 2


def _research_spec(args: argparse.Namespace) -> ResearchExperimentSpec:
    loaded = args.condition != "baseline"
    return ResearchExperimentSpec(
        campaign_id=args.campaign_id,
        run_id=args.run_id,
        network_run_id=args.network_run_id,
        condition=args.condition,
        cooja_seed=args.seed,
        sensor_period_seconds=args.sensor_period,
        measurement=MeasurementSpec(
            warmup_seconds=args.warmup_seconds,
            duration_seconds=args.duration_seconds,
            sample_interval_seconds=args.sample_interval,
            probe_interval_seconds=args.probe_interval,
        ),
        load=LoadSpec(
            enabled=loaded,
            target_bps=args.target_bps if loaded else None,
            target_fraction=args.target_fraction if loaded else None,
            reference_capacity_bps=(
                args.reference_capacity_bps if loaded else None
            ),
            parallel_flows=args.parallel_flows,
            server_port=args.load_port,
        ),
        probe_target=args.probe_target,
    )


def _parse_seeds(value: str) -> tuple[int, ...]:
    try:
        seeds = tuple(
            int(item.strip()) for item in value.split(",") if item.strip()
        )
    except ValueError as exc:
        raise ResearchError(
            "campaign seeds must be comma-separated integers"
        ) from exc
    if not seeds:
        raise ResearchError("campaign seeds must not be empty")
    return seeds


def _parse_conditions(value: str) -> tuple[CampaignCondition, ...]:
    result: list[CampaignCondition] = []
    for item in value.split(","):
        text = item.strip()
        if not text:
            continue
        if text == "baseline":
            result.append(CampaignCondition("baseline"))
            continue
        if "=" not in text:
            raise ResearchError(
                "loaded conditions must use name=fraction or name=bps:<integer>"
            )
        name, raw = (part.strip() for part in text.split("=", 1))
        try:
            result.append(
                CampaignCondition(name, target_bps=int(raw[4:]))
                if raw.startswith("bps:")
                else CampaignCondition(name, load_fraction=float(raw))
            )
        except ValueError as exc:
            raise ResearchError("campaign load target is malformed") from exc
    if not result:
        raise ResearchError("campaign conditions must not be empty")
    return tuple(result)


def _load_campaign(path: Path) -> ResearchCampaign:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (
        FileNotFoundError,
        OSError,
        UnicodeDecodeError,
        json.JSONDecodeError,
    ) as exc:
        raise ResearchError(
            "campaign specification must be readable JSON"
        ) from exc
    if (
        not isinstance(value, Mapping)
        or value.get("schema") != "synthran/research-campaign/v1alpha1"
    ):
        raise ResearchError("campaign specification schema is unsupported")
    raw_conditions = value.get("conditions")
    raw_runs = value.get("runs")
    if not isinstance(raw_conditions, list) or not isinstance(raw_runs, list):
        raise ResearchError("campaign conditions and runs must be arrays")
    conditions: list[CampaignCondition] = []
    for item in raw_conditions:
        if not isinstance(item, Mapping) or not isinstance(
            item.get("name"), str
        ):
            raise ResearchError("campaign contains a malformed condition")
        conditions.append(
            CampaignCondition(
                str(item["name"]),
                load_fraction=(
                    float(item["load_fraction"])
                    if item.get("load_fraction") is not None
                    else None
                ),
                target_bps=(
                    int(item["target_bps"])
                    if item.get("target_bps") is not None
                    else None
                ),
            )
        )
    campaign = build_campaign(
        campaign_id=str(value.get("campaign_id")),
        network_run_id=str(value.get("network_run_id")),
        seeds=tuple(int(seed) for seed in value.get("seeds", [])),
        conditions=tuple(conditions),
        campaign_seed=int(value.get("campaign_seed")),
    )
    expected_runs = [run.to_dict() for run in campaign.runs]
    if raw_runs != expected_runs:
        raise ResearchError(
            "persisted campaign run schedule does not match its deterministic campaign contract"
        )
    return campaign


def _dispatch_research(args: argparse.Namespace) -> int:
    if args.research_command == "plan":
        print(json.dumps(_research_spec(args).to_dict(), indent=2, sort_keys=True))
        print(
            "\nExecution action: none\nReservation action: none\nNetwork deployment action: none"
        )
        return 0

    if args.research_command == "run":
        spec = _research_spec(args)
        manifest, evidence = _network_paths(
            args.network_run_root, args.network_run_id
        )
        result = execute_research_experiment(
            spec=spec,
            inventory=load_inventory(args.inventory),
            lock=load_lock(args.lock),
            dependency_root=args.deps_root,
            network_manifest=manifest,
            network_evidence=evidence,
            repository_root=repository_root(),
            run_root=args.run_root,
            progress=sys.stdout,
        )
        print(f"Run directory: {result.run_directory}")
        print(f"Research summary: {result.summary_path}")
        print(
            "Research result: "
            + (
                "READY FOR CAMPAIGN ANALYSIS"
                if result.ready_for_campaign_analysis
                else "INVALID"
            )
        )
        print(
            "Path acceptance: "
            + (
                "IOT-TO-5G PATH PROVEN"
                if result.path_acceptance_ready
                else "NOT PROVEN"
            )
        )
        return 0 if result.ready_for_campaign_analysis else 2

    if args.research_command == "calibrate":
        payload = calibrate_capacity(
            inventory=load_inventory(args.inventory),
            lock=load_lock(args.lock),
            network_run_id=args.network_run_id,
            target=args.target,
            repository_root=repository_root(),
            output_path=args.out,
            duration_seconds=args.duration_seconds,
            server_port=args.server_port,
        )
        print(json.dumps(payload, indent=2, sort_keys=True))
        print(f"Capacity evidence: {args.out}")
        return 0

    if args.research_command == "campaign-plan":
        campaign = build_campaign(
            campaign_id=args.campaign_id,
            network_run_id=args.network_run_id,
            seeds=_parse_seeds(args.seeds),
            conditions=_parse_conditions(args.conditions),
            campaign_seed=args.campaign_seed,
        )
        save_campaign(campaign, args.out)
        print(json.dumps(campaign.to_dict(), indent=2, sort_keys=True))
        print(f"Campaign schedule: {args.out}")
        return 0

    if args.research_command == "campaign-run":
        campaign = _load_campaign(args.campaign)
        manifest, evidence = _network_paths(
            args.network_run_root, campaign.network_run_id
        )
        inventory = load_inventory(args.inventory)
        lock = load_lock(args.lock)
        conditions = {
            condition.name: condition for condition in campaign.conditions
        }
        for scheduled in campaign.runs:
            if (args.run_root.resolve() / scheduled.run_id).exists():
                raise ResearchError(
                    f"campaign run directory already exists for {scheduled.run_id}; "
                    "run IDs are never reused"
                )
            condition = conditions[scheduled.condition]
            if condition.name == "baseline":
                load = LoadSpec(enabled=False)
            elif condition.target_bps is not None:
                load = LoadSpec(
                    enabled=True,
                    target_bps=condition.target_bps,
                    parallel_flows=args.parallel_flows,
                    server_port=args.load_port,
                )
            else:
                if args.reference_capacity_bps is None:
                    raise ResearchError(
                        "fractional campaign conditions require --reference-capacity-bps"
                    )
                load = LoadSpec(
                    enabled=True,
                    target_fraction=condition.load_fraction,
                    reference_capacity_bps=args.reference_capacity_bps,
                    parallel_flows=args.parallel_flows,
                    server_port=args.load_port,
                )
            spec = ResearchExperimentSpec(
                campaign_id=campaign.campaign_id,
                run_id=scheduled.run_id,
                network_run_id=campaign.network_run_id,
                condition=scheduled.condition,
                cooja_seed=scheduled.seed,
                sensor_period_seconds=args.sensor_period,
                measurement=MeasurementSpec(
                    args.warmup_seconds,
                    args.duration_seconds,
                    args.sample_interval,
                    args.probe_interval,
                ),
                load=load,
                probe_target=args.target,
            )
            print(
                f"Campaign {campaign.campaign_id}: "
                f"{scheduled.ordinal}/{len(campaign.runs)} {scheduled.run_id} "
                f"({scheduled.condition}, seed={scheduled.seed})"
            )
            result = execute_research_experiment(
                spec=spec,
                inventory=inventory,
                lock=lock,
                dependency_root=args.deps_root,
                network_manifest=manifest,
                network_evidence=evidence,
                repository_root=repository_root(),
                run_root=args.run_root,
                progress=sys.stdout,
            )
            if not result.ready_for_campaign_analysis:
                print(
                    f"Campaign stopped: {scheduled.run_id} is invalid for analysis"
                )
                return 2
        print(f"Campaign complete: {campaign.campaign_id}")
        return 0

    if args.research_command == "analyze":
        campaign = _load_campaign(args.campaign)
        summaries = [
            load_run_summary(path)
            for run in campaign.runs
            if (
                path := args.run_root.resolve()
                / run.run_id
                / "research-summary.json"
            ).is_file()
        ]
        analysis = analyze_campaign(campaign, summaries)
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(
            json.dumps(analysis, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        print(json.dumps(analysis, indent=2, sort_keys=True))
        print(f"Campaign analysis: {args.out}")
        return 0 if analysis["usable_runs"] == analysis["expected_runs"] else 2

    raise AssertionError("unreachable research command")


def _dispatch_experiment(args: argparse.Namespace) -> int:
    if args.experiment_command == "plan":
        return _experiment_plan(args)
    if args.experiment_command == "run":
        return _experiment_run(args)
    if args.experiment_command == "verify":
        return _experiment_verify(args)
    if args.experiment_command == "research":
        return _dispatch_research(args)
    raise AssertionError("unreachable experiment command")


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "experiment":
            return _dispatch_experiment(args)
        if args.command == "slices" and args.slices_command == "doctor":
            return _slices_doctor(args)
        if args.command == "r2lab":
            return _dispatch_r2lab(args)
        if args.command == "deps" and args.deps_command == "sync":
            return _deps_sync(args)
        if args.command == "privacy" and args.privacy_command == "scan":
            return _privacy_scan(args)
        if args.command == "privacy" and args.privacy_command == "redact":
            redact_file(args.source, args.destination, dry_run=args.dry_run, output=sys.stdout)
            return 0
        if args.command == "hooks" and args.hooks_command == "install":
            return _hooks_install(args)
        if args.command == "doctor":
            return _doctor(args)
        if args.command == "network" and args.network_command == "prepare":
            return _network_prepare(args)
        if args.command == "network" and args.network_command == "deploy":
            return _network_deploy(args)
        if args.command == "network" and args.network_command == "verify":
            return _network_verify(args)
    except (
        DependencyError,
        ExperimentError,
        FiveGAnsibleError,
        LivePreflightError,
        NetworkRuntimeError,
        PrivacyError,
        R2LabPhysicalFoundationError,
        R2LabPhysicalGnbError,
        R2LabResourceError,
        ResearchError,
        ResourcePreparationError,
        SlicesControllerError,
        OSError,
    ) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    raise AssertionError("unreachable command dispatch")


if __name__ == "__main__":
    raise SystemExit(main())
