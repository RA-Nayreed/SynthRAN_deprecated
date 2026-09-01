"""Single public command surface for SynthRAN."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import tempfile
from typing import Mapping, Sequence

from synthran.ambient_contract import (
    DEFAULT_ENERGY_NODE_VARIATION,
    DEFAULT_ENERGY_POWER_SCALE,
)
from synthran.dependencies import DependencyError, load_lock, sync_dependencies
from synthran.errors import SynthRANError
from synthran.fiveg_ansible import FiveGAnsibleError, load_inventory, run_offline_doctor
from synthran.iot_source import AMBIENT_PROFILE, DEFAULT_IOT_SEED, TRANSPORT_PROFILE
from synthran.lifecycle import configure_run_parser, execute_run
from synthran.network.resources import SUPPORTED_NODES, build_preparation_inventory
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
from synthran.r2lab.acceptance import PhysicalRunEvidence
from synthran.r2lab.controller import gateway_command, subprocess_runner as r2lab_runner
from synthran.r2lab.hardware import RADIOS, UES, PhysicalTopology, capabilities
from synthran.r2lab.n3xx import stop_n3xx_gnb
from synthran.r2lab.resources import (
    R2LabTopologyResourceError,
    load_topology,
    release_physical_resources,
)
from synthran.r2lab.stale_claim import retire_if_lease_absent
from synthran.research import (
    CampaignCondition,
    LoadSpec,
    MeasurementSpec,
    ResearchCampaign,
    ResearchError,
    analyze_campaign,
    build_campaign,
    load_run_summary,
    save_campaign,
)
from synthran.research.amber_campaign import analyze_amber_campaign, execute_amber_campaign
from synthran.research.amber_runtime import execute_amber_research_experiment
from synthran.research.calibration import calibrate_capacity
from synthran.research.v2 import AmberResearchSpec
from synthran.run_events import RunEventStream
from synthran.slices_controller import SlicesControllerError, verify_slices_controller


PUBLIC_COMMANDS = (
    "run",
    "doctor",
    "calibrate",
    "inspect",
    "analyze",
    "release",
    "deps",
    "dev",
)

_EXECUTABLE_DEVICES = tuple(sorted(name for name, profile in RADIOS.items() if profile.executable))
_EXECUTABLE_UES = tuple(sorted(name for name, profile in UES.items() if profile.executable))


def _subparsers(parser: argparse.ArgumentParser) -> argparse._SubParsersAction:
    for action in parser._actions:
        if isinstance(action, argparse._SubParsersAction):
            return action
    raise SynthRANError("SynthRAN parser does not expose command choices")


def _make_optional(parser: argparse.ArgumentParser, *destinations: str) -> None:
    wanted = set(destinations)
    for action in parser._actions:
        if getattr(action, "dest", None) in wanted:
            action.required = False


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


def _add_operator_commands(root: argparse._SubParsersAction) -> None:
    doctor = root.add_parser("doctor", help="run read-only readiness checks")
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

    inspect = root.add_parser("inspect", help="show capabilities or persisted run evidence")
    inspect.add_argument("--radio", choices=("rfsim", "r2lab"))
    inspect.add_argument("--run-id")
    inspect.add_argument("--json", action="store_true")

    deps = root.add_parser("deps", help="manage immutable external dependencies")
    deps_commands = deps.add_subparsers(dest="deps_command", required=True)
    sync = deps_commands.add_parser("sync", help="synchronize pinned checkouts")
    sync.add_argument("--lock", type=Path, default=Path("dependencies.lock.yml"))
    sync.add_argument("--root", type=Path, default=Path(".deps"))
    selection = sync.add_mutually_exclusive_group()
    selection.add_argument("--all", action="store_true")
    selection.add_argument("--name", action="append", dest="dependency_names")
    sync.add_argument("--dry-run", action="store_true")

    dev = root.add_parser("dev", help="repository maintenance commands")
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


def _add_run_experiment_options(run: argparse.ArgumentParser) -> None:
    # Full lifecycle runs require these fields; controlled runs reuse an accepted
    # network and validate their smaller contract at dispatch time.
    _make_optional(run, "radio", "run_id", "core_node", "ran_node")

    run.add_argument(
        "--iot-profile",
        choices=(AMBIENT_PROFILE, TRANSPORT_PROFILE),
        default=AMBIENT_PROFILE,
        help="AMBER source profile",
    )
    run.add_argument(
        "--iot-seed",
        "--seed",
        dest="iot_seed",
        type=int,
        default=DEFAULT_IOT_SEED,
        help="AMBER source seed",
    )
    run.add_argument(
        "--sensor-period",
        type=int,
        default=10,
        help="Ambient-IoT sensing/publication period in seconds",
    )
    run.add_argument(
        "--energy-power-scale",
        type=float,
        default=DEFAULT_ENERGY_POWER_SCALE,
        help=(
            "ambient-v1 external harvested-power multiplier in (0,1]; "
            "1.0 preserves the energy-sufficient control"
        ),
    )
    run.add_argument(
        "--energy-node-variation",
        type=float,
        default=DEFAULT_ENERGY_NODE_VARIATION,
        help=(
            "ambient-v1 deterministic per-node harvested-power variation "
            "fraction in [0,0.5]"
        ),
    )

    run.add_argument(
        "--network-run-id",
        help="accepted RFSIM network run to reuse for controlled execution",
    )
    run.add_argument("--campaign-id")
    run.add_argument("--condition")
    run.add_argument(
        "--plan",
        action="store_true",
        help="persist/render the immutable controlled run or campaign plan only",
    )
    run.add_argument("--inventory", type=Path)
    run.add_argument("--warmup-seconds", type=int, default=30)
    run.add_argument("--duration-seconds", type=int, default=180)
    run.add_argument("--sample-interval", type=float, default=1.0)
    run.add_argument("--probe-interval", type=float, default=1.0)
    run.add_argument("--target-bps", type=int)
    run.add_argument("--target-fraction", type=float)
    run.add_argument("--reference-capacity-bps", type=int)
    run.add_argument("--parallel-flows", type=int, default=1)
    run.add_argument("--load-port", type=int, default=5201)
    run.add_argument("--probe-target")

    campaign = run.add_argument_group("campaign")
    campaign.add_argument("--campaign", type=Path, help="reuse an immutable campaign file")
    campaign.add_argument("--seeds", help="comma-separated campaign AMBER seeds")
    campaign.add_argument(
        "--conditions",
        help="comma-separated conditions, e.g. baseline,load50=0.5,load80=0.8",
    )
    campaign.add_argument("--campaign-seed", type=int)
    campaign.add_argument(
        "--campaign-root",
        type=Path,
        default=Path(".synthran/campaigns"),
        help=argparse.SUPPRESS,
    )


def _add_top_level_experiment_commands(root: argparse._SubParsersAction) -> None:
    calibrate = root.add_parser(
        "calibrate",
        help="measure reference capacity of an accepted UE path",
        description="Measure RAN/UE-path capacity and persist calibration evidence.",
    )
    calibrate.add_argument("--inventory", type=Path, required=True)
    calibrate.add_argument("--network-run-id", required=True)
    calibrate.add_argument("--target", required=True)
    calibrate.add_argument("--duration-seconds", type=int, default=10)
    calibrate.add_argument("--server-port", type=int, default=5201)
    calibrate.add_argument("--lock", type=Path, default=Path("dependencies.lock.yml"))
    calibrate.add_argument("--out", type=Path, required=True)

    analyze = root.add_parser("analyze", help="analyze a completed persisted campaign")
    analyze.add_argument("--campaign", type=Path, required=True)
    analyze.add_argument(
        "--run-root",
        type=Path,
        default=Path(".synthran/experiments"),
        help=argparse.SUPPRESS,
    )
    analyze.add_argument("--out", type=Path, required=True)

    release = root.add_parser(
        "release",
        help="release persistent resources owned by one physical run",
    )
    release.add_argument("--run-id", required=True)
    release.add_argument(
        "--slice",
        dest="r2lab_slice",
        default=os.environ.get("SYNTHRAN_R2LAB_SLICE"),
    )
    release.add_argument("--owner", default=os.environ.get("SYNTHRAN_OWNER"))
    release.add_argument(
        "--allocation-id",
        default=os.environ.get("SYNTHRAN_ALLOCATION_ID"),
    )
    release.add_argument(
        "--known-hosts",
        type=Path,
        default=os.environ.get("SYNTHRAN_SLICES_KNOWN_HOSTS"),
    )
    release.add_argument("--timeout", type=int, default=300)
    release.add_argument("--json", action="store_true")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="synthran",
        description=(
            "Run and inspect reproducible AMBER Ambient-IoT experiments across "
            "virtual and physical radio backends."
        ),
    )
    parser.add_subparsers(dest="command", required=True)
    configure_run_parser(parser)
    root = _subparsers(parser)
    _add_operator_commands(root)
    _add_top_level_experiment_commands(root)
    run = root.choices.get("run")
    if run is None:
        raise SynthRANError("SynthRAN parser does not expose the run command")
    _add_run_experiment_options(run)
    return parser


def _read_json_object(path: Path, *, label: str) -> Mapping[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SynthRANError(f"{label} is unreadable") from exc
    if not isinstance(value, dict):
        raise SynthRANError(f"{label} is malformed")
    return value


def _network_paths(root: Path, run_id: str) -> tuple[Path, Path]:
    directory = root.resolve() / run_id
    return directory / "manifest.json", directory / "network-evidence.json"


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


def _is_campaign_run(args: argparse.Namespace) -> bool:
    return bool(
        getattr(args, "campaign", None) is not None
        or getattr(args, "seeds", None) is not None
        or getattr(args, "conditions", None) is not None
        or getattr(args, "campaign_seed", None) is not None
    )


def _is_controlled_run(args: argparse.Namespace) -> bool:
    if args.command != "run":
        return False
    return bool(
        _is_campaign_run(args)
        or getattr(args, "network_run_id", None) is not None
        or getattr(args, "condition", None) is not None
    )


def _validate_persisted_iot_identity(args: argparse.Namespace) -> None:
    if args.command != "run" or getattr(args, "radio", None) not in {"rfsim", "r2lab"}:
        return
    if _is_controlled_run(args):
        return
    run_id = getattr(args, "run_id", None)
    if not run_id:
        return

    manifest_path: Path | None = None
    if args.radio == "rfsim":
        candidate = Path(args.experiment_root).expanduser().resolve() / run_id / "manifest.json"
        if candidate.is_file():
            manifest_path = candidate
    else:
        result_path = (
            Path(args.r2lab_run_root).expanduser().resolve()
            / run_id
            / "physical"
            / "physical-workload-result.json"
        )
        if result_path.is_file():
            result = _read_json_object(result_path, label="persisted physical workload result")
            workload_id = result.get("workload_id")
            if not isinstance(workload_id, str) or not workload_id:
                raise SynthRANError("persisted physical workload result has no workload ID")
            candidate = (
                Path(args.r2lab_experiment_root).expanduser().resolve()
                / workload_id
                / "manifest.json"
            )
            if not candidate.is_file():
                raise SynthRANError("persisted physical workload manifest is unavailable")
            manifest_path = candidate

    if manifest_path is None:
        return
    manifest = _read_json_object(manifest_path, label="persisted AMBER workload manifest")
    if manifest.get("iot_source") != "amber":
        raise SynthRANError("persisted workload is not an AMBER workload")
    expected = {
        "iot_profile": args.iot_profile,
        "iot_seed": args.iot_seed,
        "sensor_period_seconds": args.sensor_period,
    }
    for key, requested in expected.items():
        if manifest.get(key) != requested:
            raise SynthRANError(f"persisted AMBER workload {key} does not match the requested value")


def _require_controlled_common(args: argparse.Namespace) -> None:
    if args.radio not in {None, "rfsim"}:
        raise ResearchError("controlled runs currently support the RFSIM backend only")
    if args.inventory is None and not args.plan:
        raise ResearchError("controlled run requires --inventory")
    if args.probe_target is None and not args.plan:
        raise ResearchError("controlled run requires --probe-target")


def _amber_research_spec(args: argparse.Namespace) -> AmberResearchSpec:
    required = {
        "--campaign-id": args.campaign_id,
        "--network-run-id": args.network_run_id,
        "--run-id": args.run_id,
        "--condition": args.condition,
    }
    missing = [name for name, value in required.items() if not value]
    if missing:
        raise ResearchError("controlled run requires " + ", ".join(missing))
    loaded = args.condition != "baseline"
    return AmberResearchSpec(
        campaign_id=args.campaign_id,
        run_id=args.run_id,
        network_run_id=args.network_run_id,
        condition=args.condition,
        iot_profile=args.iot_profile,
        iot_seed=args.iot_seed,
        sensor_period_seconds=args.sensor_period,
        energy_power_scale=args.energy_power_scale,
        energy_node_variation=args.energy_node_variation,
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


def _campaign_path(args: argparse.Namespace) -> Path:
    if args.campaign is not None:
        return args.campaign.expanduser().resolve()
    if not args.campaign_id:
        raise ResearchError("campaign run requires --campaign-id or --campaign")
    return args.campaign_root.expanduser().resolve() / f"{args.campaign_id}.json"


def _load_or_create_campaign(args: argparse.Namespace):
    path = _campaign_path(args)
    if args.campaign is not None:
        return _load_campaign(path), path
    required = {
        "--campaign-id": args.campaign_id,
        "--network-run-id": args.network_run_id,
        "--seeds": args.seeds,
        "--conditions": args.conditions,
        "--campaign-seed": args.campaign_seed,
    }
    missing = [name for name, value in required.items() if value is None]
    if missing:
        raise ResearchError("new campaign requires " + ", ".join(missing))
    campaign = build_campaign(
        campaign_id=args.campaign_id,
        network_run_id=args.network_run_id,
        seeds=_parse_seeds(args.seeds),
        conditions=_parse_conditions(args.conditions),
        campaign_seed=args.campaign_seed,
    )
    if path.is_file():
        persisted = _load_campaign(path)
        if persisted.to_dict() != campaign.to_dict():
            raise ResearchError("persisted campaign differs from the requested immutable schedule")
        return persisted, path
    path.parent.mkdir(parents=True, exist_ok=True)
    save_campaign(campaign, path)
    return campaign, path


def _dispatch_controlled_run(args: argparse.Namespace) -> int:
    _require_controlled_common(args)
    if _is_campaign_run(args):
        campaign, campaign_path = _load_or_create_campaign(args)
        if args.plan:
            print(json.dumps(campaign.to_dict(), indent=2, sort_keys=True))
            print(f"\nCampaign schedule: {campaign_path}")
            print("Execution action: none")
            return 0
        stream = RunEventStream(run_id=campaign.campaign_id, radio="rfsim", terminal=sys.stdout)
        stream.emit("→ workload: controlled AMBER campaign", stage="workload", event="started")
        try:
            manifest, evidence = _network_paths(
                args.network_run_root, campaign.network_run_id
            )
            result_path = execute_amber_campaign(
                campaign=campaign,
                iot_profile=args.iot_profile,
                energy_power_scale=args.energy_power_scale,
                energy_node_variation=args.energy_node_variation,
                inventory=load_inventory(args.inventory),
                lock=load_lock(args.lock),
                dependency_root=args.deps_root,
                network_manifest=manifest,
                network_evidence=evidence,
                repository_root=repository_root(),
                run_root=args.experiment_root,
                target=args.probe_target,
                reference_capacity_bps=args.reference_capacity_bps,
                sensor_period_seconds=args.sensor_period,
                measurement=MeasurementSpec(
                    warmup_seconds=args.warmup_seconds,
                    duration_seconds=args.duration_seconds,
                    sample_interval_seconds=args.sample_interval,
                    probe_interval_seconds=args.probe_interval,
                ),
                parallel_flows=args.parallel_flows,
                load_port=args.load_port,
                progress=stream,
            )
            stream.emit("✓ workload: campaign accepted", stage="workload", event="completed")
            stream.emit(f"  campaign: {campaign_path}", stage="acceptance")
            stream.emit(f"  result: {result_path}", stage="acceptance")
            return 0
        finally:
            stream.flush()

    spec = _amber_research_spec(args)
    if args.plan:
        print(
            json.dumps(
                {"schema": "synthran/research-request/v2alpha1", **spec.to_request_dict()},
                indent=2,
                sort_keys=True,
            )
        )
        print("\nExecution action: none")
        return 0

    stream = RunEventStream(run_id=spec.run_id, radio="rfsim", terminal=sys.stdout)
    stream.emit(
        f"→ workload: controlled AMBER {spec.condition}",
        stage="workload",
        event="started",
    )
    try:
        manifest, evidence = _network_paths(
            args.network_run_root, spec.network_run_id
        )
        summary_path = execute_amber_research_experiment(
            spec=spec,
            inventory=load_inventory(args.inventory),
            lock=load_lock(args.lock),
            dependency_root=args.deps_root,
            network_manifest=manifest,
            network_evidence=evidence,
            repository_root=repository_root(),
            run_root=args.experiment_root,
            progress=stream,
        )
        stream.emit("✓ workload: accepted", stage="workload", event="completed")
        stream.emit("✓ experiment accepted", stage="acceptance", event="completed")
        stream.emit(f"  evidence: {summary_path}", stage="acceptance")
        return 0
    finally:
        stream.flush()


def _dispatch_capacity_calibration(args: argparse.Namespace) -> int:
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


def _dispatch_analysis(args: argparse.Namespace) -> int:
    campaign = _load_campaign(args.campaign)
    run_root = args.run_root.expanduser().resolve()
    first_v2 = run_root / campaign.runs[0].run_id / "research-summary-v2.json"
    if first_v2.is_file():
        result = analyze_amber_campaign(
            campaign=campaign,
            run_root=run_root,
            output_path=args.out,
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        print(f"Campaign analysis: {args.out}")
        return 0
    summaries = [
        load_run_summary(path)
        for scheduled in campaign.runs
        if (path := run_root / scheduled.run_id / "research-summary.json").is_file()
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


def _doctor_r2lab(args: argparse.Namespace) -> int:
    if args.device is None or args.ue is None:
        raise SynthRANError("doctor --radio r2lab requires --device and --ue")
    if not args.r2lab_slice:
        raise SynthRANError("doctor --radio r2lab requires --slice or SYNTHRAN_R2LAB_SLICE")
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
        raise SynthRANError("core and RAN nodes must differ")
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
            raise SynthRANError("provider verification requires --slices-project or SYNTHRAN_SLICES_PROJECT")
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


def _doctor(args: argparse.Namespace) -> int:
    return _doctor_r2lab(args) if args.radio == "r2lab" else _doctor_rfsim(args)


def _candidate_evidence(run_id: str) -> tuple[Path, ...]:
    return (
        Path(".synthran/r2lab") / run_id / "physical-run.json",
        Path(".synthran/experiments-r2lab") / run_id / "experiment-evidence.json",
        Path(".synthran/experiments") / run_id / "experiment-evidence.json",
        Path(".synthran/runs") / run_id / "network-evidence.json",
        Path(".synthran/preparations") / run_id / "manifest.json",
    )


def _inspect(args: argparse.Namespace) -> int:
    if args.run_id is None:
        if args.radio != "r2lab":
            raise SynthRANError("inspect without --run-id currently supports --radio r2lab capabilities")
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
            raise SynthRANError(f"persisted run evidence is unreadable: {path}") from exc
        found.append({"path": str(path), "payload": payload})
    if not found:
        raise SynthRANError(f"no persisted evidence found for run {args.run_id}")
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


def _release(args: argparse.Namespace) -> int:
    run_root = Path(".synthran/r2lab")
    run_directory = run_root / args.run_id
    if not run_directory.exists():
        payload = {
            "schema": "synthran/release/v1",
            "run_id": args.run_id,
            "released": False,
            "detail": "no active physical claim found; virtual workloads clean up within their run",
        }
        print(json.dumps(payload, indent=2, sort_keys=True) if args.json else payload["detail"])
        return 0
    if not args.r2lab_slice:
        raise SynthRANError(
            "physical release requires --slice or SYNTHRAN_R2LAB_SLICE"
        )

    topology = load_topology(run_root=run_root, run_id=args.run_id).validate()
    retirement = retire_if_lease_absent(
        run_root=run_root,
        run_id=args.run_id,
        slice_name=args.r2lab_slice,
        topology=topology,
        runner=r2lab_runner,
        timeout_seconds=min(args.timeout, 300),
    )
    if retirement is not None:
        result = {
            "schema": "synthran/release/v1",
            "run_id": args.run_id,
            "radio": topology.radio,
            "ue": topology.ue,
            "released": False,
            "retired": True,
            "hardware_mutated": False,
            "detail": (
                "current R2Lab lease is not held; retired the stale local claim "
                "without touching provider hardware"
            ),
            "retirement": retirement.to_dict(),
        }
        print(
            json.dumps(result, indent=2, sort_keys=True)
            if args.json
            else result["detail"]
        )
        return 0

    if not args.owner or args.known_hosts is None:
        raise SynthRANError(
            "physical release with a current lease requires --owner/SYNTHRAN_OWNER "
            "and --known-hosts/SYNTHRAN_SLICES_KNOWN_HOSTS"
        )
    known_hosts = Path(args.known_hosts).expanduser().resolve()
    if not known_hosts.is_file():
        raise SynthRANError("strict SLICES known-hosts file is missing")
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
        "schema": "synthran/release/v1",
        "run_id": args.run_id,
        "radio": topology.radio,
        "ue": topology.ue,
        "released": True,
        "retired": False,
        "release": payload,
    }
    print(json.dumps(result, indent=2, sort_keys=True) if args.json else "Selected run resources released.")
    return 0


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


def _validate_lifecycle_run(args: argparse.Namespace) -> None:
    if args.plan:
        raise SynthRANError(
            "run --plan is for controlled runs/campaigns; full lifecycle runs execute directly"
        )
    required = {
        "--radio": args.radio,
        "--run-id": args.run_id,
        "--core-node": args.core_node,
        "--ran-node": args.ran_node,
    }
    missing = [name for name, value in required.items() if not value]
    if missing:
        raise SynthRANError("full lifecycle run requires " + ", ".join(missing))
    if args.radio == "r2lab" and (
        args.energy_power_scale != DEFAULT_ENERGY_POWER_SCALE
        or args.energy_node_variation != DEFAULT_ENERGY_NODE_VARIATION
    ):
        raise SynthRANError("Ambient energy treatment is currently supported on RFSIM only")


def _dispatch(args: argparse.Namespace) -> int:
    if args.command == "run":
        payload = execute_run(args)
        if args.json:
            print(json.dumps(payload, indent=2, sort_keys=True))
        return 0
    if args.command == "doctor":
        return _doctor(args)
    if args.command == "inspect":
        return _inspect(args)
    if args.command == "release":
        return _release(args)
    if args.command == "deps":
        return _deps_sync(args)
    if args.command == "dev" and args.dev_command == "privacy":
        if args.privacy_command == "scan":
            return _privacy_scan(args)
        if args.privacy_command == "redact":
            redact_file(
                args.source,
                args.destination,
                dry_run=args.dry_run,
                output=sys.stdout,
            )
            return 0
    if args.command == "dev" and args.dev_command == "hooks" and args.hooks_command == "install":
        return _hooks_install(args)
    raise SynthRANError("unsupported SynthRAN command")


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(list(sys.argv[1:] if argv is None else argv))
    try:
        if args.command == "run" and _is_controlled_run(args):
            return _dispatch_controlled_run(args)
        if args.command == "calibrate":
            return _dispatch_capacity_calibration(args)
        if args.command == "analyze":
            return _dispatch_analysis(args)
        if args.command == "run":
            _validate_lifecycle_run(args)
            _validate_persisted_iot_identity(args)
        return _dispatch(args)
    except (
        SynthRANError,
        ResearchError,
        R2LabTopologyResourceError,
        DependencyError,
        FiveGAnsibleError,
        PrivacyError,
        SlicesControllerError,
        OSError,
        ValueError,
    ) as exc:
        prefix = "[synthran] ✗ run: " if args.command == "run" else "error: "
        print(f"{prefix}{exc}", file=sys.stderr)
        return 2
