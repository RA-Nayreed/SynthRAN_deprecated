"""Internal runtime support used by the unified SynthRAN operator surface."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
from typing import Mapping

from synthran.dependencies import load_lock, sync_dependencies
from synthran.experiment.runtime import execute_experiment
from synthran.fiveg_ansible import build_network_plan, load_inventory, run_offline_doctor
from synthran.live_preflight import run_live_preflight, save_live_evidence
from synthran.network.resources import (
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
from synthran.slices_controller import SlicesControllerError, verify_slices_controller


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
        help="plan, run, schedule, or analyze controlled measurements",
    )
    sub = research.add_subparsers(dest="research_command", required=True)

    plan = sub.add_parser("plan", help="render one immutable measurement specification")
    _add_research_spec_arguments(plan, require_target=False)

    run = sub.add_parser("run", help="execute one controlled fixed-window measurement")
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

    calibrate = sub.add_parser("calibrate", help="measure reference UE-path capacity")
    calibrate.add_argument("--inventory", type=Path, required=True)
    calibrate.add_argument("--network-run-id", required=True)
    calibrate.add_argument("--target", required=True)
    calibrate.add_argument("--duration-seconds", type=int, default=10)
    calibrate.add_argument("--server-port", type=int, default=5201)
    calibrate.add_argument("--lock", type=Path, default=Path("dependencies.lock.yml"))
    calibrate.add_argument("--out", type=Path, required=True)

    energy_calibrate = sub.add_parser(
        "energy-calibrate",
        help="calibrate Ambient-IoT harvested-energy treatment offline",
    )
    energy_calibrate.add_argument("--calibration-id", required=True)
    energy_calibrate.add_argument("--network-run-id", required=True)
    energy_calibrate.add_argument(
        "--scales",
        default="1.0,0.75,0.5,0.33,0.25",
        help="comma-separated external harvested-power multipliers",
    )
    energy_calibrate.add_argument("--seed", type=int, default=424242)
    energy_calibrate.add_argument("--sensor-period", type=int, default=10)
    energy_calibrate.add_argument("--warmup-seconds", type=int, default=30)
    energy_calibrate.add_argument("--duration-seconds", type=int, default=180)
    energy_calibrate.add_argument(
        "--energy-node-variation",
        type=float,
        default=0.0,
        help="deterministic per-node harvested-power variation fraction in [0,0.5]",
    )
    energy_calibrate.add_argument(
        "--target-energy-loss-min",
        type=float,
        default=0.15,
        help="lower bound of the desired measurement-window energy-loss ratio",
    )
    energy_calibrate.add_argument(
        "--target-energy-loss-max",
        type=float,
        default=0.35,
        help="upper bound of the desired measurement-window energy-loss ratio",
    )
    energy_calibrate.add_argument("--deps-root", type=Path, default=Path(".deps"))
    energy_calibrate.add_argument(
        "--calibration-root",
        type=Path,
        default=Path(".synthran/energy-calibrations"),
        help=argparse.SUPPRESS,
    )

    campaign_plan = sub.add_parser(
        "campaign-plan", help="create a deterministic blocked campaign schedule"
    )
    campaign_plan.add_argument("--campaign-id", required=True)
    campaign_plan.add_argument("--network-run-id", required=True)
    campaign_plan.add_argument("--seeds", required=True)
    campaign_plan.add_argument("--conditions", required=True)
    campaign_plan.add_argument("--campaign-seed", type=int, required=True)
    campaign_plan.add_argument("--out", type=Path, required=True)

    campaign_run = sub.add_parser("campaign-run", help="execute a persisted campaign")
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
    campaign_run.add_argument("--lock", type=Path, default=Path("dependencies.lock.yml"))
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

    analyze = sub.add_parser("analyze", help="analyze persisted valid runs")
    analyze.add_argument("--campaign", type=Path, required=True)
    analyze.add_argument(
        "--run-root",
        type=Path,
        default=Path(".synthran/experiments"),
        help=argparse.SUPPRESS,
    )
    analyze.add_argument("--out", type=Path, required=True)


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
    """Internal RFSIM live preflight used by the unified run."""

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
        raise NetworkRuntimeError("live preflight requires " + ", ".join(missing))
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
    project, experiment = _require_slices_context(args, "resource preparation")
    if args.owner is None:
        raise ResourcePreparationError("resource preparation requires an owner")
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
    print(f"Generated inventory: {result.inventory_path}")
    print(f"Private authority: {result.authority_path}")
    print(f"Sanitized manifest: {result.manifest_path}")
    print(f"Sanitized log: {result.log_path}")
    return 0


def _network_deploy(args: argparse.Namespace) -> int:
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


def _experiment_run(args: argparse.Namespace) -> int:
    manifest, evidence = _network_paths(args.network_run_root, args.network_run_id)
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
        seeds = tuple(int(item.strip()) for item in value.split(",") if item.strip())
    except ValueError as exc:
        raise ResearchError("campaign seeds must be comma-separated integers") from exc
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
    except (FileNotFoundError, OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ResearchError("campaign specification must be readable JSON") from exc
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
        if not isinstance(item, Mapping) or not isinstance(item.get("name"), str):
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
    if raw_runs != [run.to_dict() for run in campaign.runs]:
        raise ResearchError(
            "persisted campaign run schedule does not match its deterministic contract"
        )
    return campaign


def _dispatch_research(args: argparse.Namespace) -> int:
    if args.research_command == "plan":
        print(json.dumps(_research_spec(args).to_dict(), indent=2, sort_keys=True))
        print("\nExecution action: none")
        return 0

    if args.research_command == "run":
        spec = _research_spec(args)
        manifest, evidence = _network_paths(args.network_run_root, args.network_run_id)
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
            + ("READY FOR ANALYSIS" if result.ready_for_campaign_analysis else "INVALID")
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
        manifest, evidence = _network_paths(args.network_run_root, campaign.network_run_id)
        inventory = load_inventory(args.inventory)
        lock = load_lock(args.lock)
        conditions = {condition.name: condition for condition in campaign.conditions}
        for scheduled in campaign.runs:
            if (args.run_root.resolve() / scheduled.run_id).exists():
                raise ResearchError(
                    f"campaign run directory already exists for {scheduled.run_id}; run IDs are never reused"
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
                f"Campaign {campaign.campaign_id}: {scheduled.ordinal}/{len(campaign.runs)} "
                f"{scheduled.run_id} ({scheduled.condition}, seed={scheduled.seed})"
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
                print(f"Campaign stopped: {scheduled.run_id} is invalid for analysis")
                return 2
        print(f"Campaign complete: {campaign.campaign_id}")
        return 0

    if args.research_command == "analyze":
        campaign = _load_campaign(args.campaign)
        summaries = [
            load_run_summary(path)
            for run in campaign.runs
            if (path := args.run_root.resolve() / run.run_id / "research-summary.json").is_file()
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
