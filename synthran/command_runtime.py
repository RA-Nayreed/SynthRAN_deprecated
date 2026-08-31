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

from synthran.amber_experiment_runtime import execute_amber_experiment
from synthran.ambient_contract import (
    DEFAULT_ENERGY_NODE_VARIATION,
    DEFAULT_ENERGY_POWER_SCALE,
)
from synthran.dependencies import load_lock, sync_dependencies
from synthran.fiveg_ansible import build_network_plan, load_inventory, run_offline_doctor
from synthran.iot_source import AMBIENT_PROFILE, DEFAULT_IOT_SEED
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
    ResearchCampaign,
    ResearchError,
    analyze_campaign,
    build_campaign,
    load_run_summary,
    save_campaign,
)
from synthran.research.runtime import calibrate_capacity
from synthran.slices_controller import SlicesControllerError, verify_slices_controller


def _network_paths(root: Path, run_id: str) -> tuple[Path, Path]:
    directory = root.resolve() / run_id
    return directory / "manifest.json", directory / "network-evidence.json"


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
    print(f"Deployment completed for run {result.run_id}; network verification is still required.")
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
    """Execute the only supported IoT source: AMBER."""

    manifest, evidence = _network_paths(args.network_run_root, args.network_run_id)
    result = execute_amber_experiment(
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
        iot_profile=getattr(args, "iot_profile", AMBIENT_PROFILE),
        iot_seed=getattr(args, "iot_seed", DEFAULT_IOT_SEED),
        sensor_period_seconds=getattr(args, "sensor_period", 10),
        energy_power_scale=getattr(args, "energy_power_scale", DEFAULT_ENERGY_POWER_SCALE),
        energy_node_variation=getattr(args, "energy_node_variation", DEFAULT_ENERGY_NODE_VARIATION),
        progress=sys.stdout,
    )
    print(f"Run directory: {result.run_directory}")
    if result.evidence_path.is_file():
        print(f"Sanitized evidence: {result.evidence_path}")
    return 0 if result.ready else 2


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
