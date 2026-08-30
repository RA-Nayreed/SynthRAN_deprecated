"""Single public command surface for SynthRAN."""

from __future__ import annotations

import argparse
from contextlib import contextmanager
import json
from pathlib import Path
import sys
from typing import Iterator, Mapping, Sequence

from synthran import command_runtime
from synthran.amber_experiment_runtime import execute_amber_experiment
from synthran.ambient_contract import (
    DEFAULT_ENERGY_NODE_VARIATION,
    DEFAULT_ENERGY_POWER_SCALE,
)
from synthran.backends import run as run_backend
from synthran.backends.base import BackendError
from synthran.iot_source import (
    AMBIENT_PROFILE,
    DEFAULT_IOT_SEED,
    TRANSPORT_PROFILE,
)
from synthran.operator import configure_operator_parser, dispatch, stop_command
from synthran.r2lab.iot_lifecycle import run_physical_iot_workload
from synthran.r2lab.resources import R2LabTopologyResourceError
from synthran.research import LoadSpec, MeasurementSpec, ResearchError
from synthran.research.amber_campaign import (
    analyze_amber_campaign,
    execute_amber_campaign,
)
from synthran.research.amber_runtime import execute_amber_research_experiment
from synthran.research.v2 import AmberResearchSpec


PUBLIC_COMMANDS = (
    "run",
    "doctor",
    "calibrate",
    "inspect",
    "logs",
    "analyze",
    "release",
    "deps",
    "dev",
)


def _subparsers(parser: argparse.ArgumentParser) -> argparse._SubParsersAction:
    for action in parser._actions:
        if isinstance(action, argparse._SubParsersAction):
            return action
    raise BackendError("SynthRAN parser does not expose command choices")


def _top_level_subparsers(parser: argparse.ArgumentParser) -> argparse._SubParsersAction:
    return _subparsers(parser)


def _remove_command(root: argparse._SubParsersAction, name: str) -> None:
    """Remove one legacy public parser, including its help-list entry."""

    root.choices.pop(name, None)
    choices_actions = getattr(root, "_choices_actions", None)
    if isinstance(choices_actions, list):
        root._choices_actions = [
            action for action in choices_actions if getattr(action, "dest", None) != name
        ]


def _make_optional(parser: argparse.ArgumentParser, *destinations: str) -> None:
    wanted = set(destinations)
    for action in parser._actions:
        if getattr(action, "dest", None) in wanted:
            action.required = False


def _add_run_measurement_options(run: argparse.ArgumentParser) -> None:
    # The same run verb handles a full lifecycle run or a controlled measurement
    # over an already accepted network. These four fields are required only by
    # the full lifecycle path and are validated at dispatch time.
    _make_optional(run, "radio", "run_id", "core_node", "ran_node")

    run.add_argument(
        "--iot-source",
        choices=("cooja", "amber"),
        default="cooja",
        help="IoT source for a full lifecycle run; controlled measurements use Amber",
    )
    run.add_argument(
        "--iot-profile",
        choices=(TRANSPORT_PROFILE, AMBIENT_PROFILE),
        default=TRANSPORT_PROFILE,
        help="Amber source profile",
    )
    run.add_argument(
        "--iot-seed",
        "--seed",
        dest="iot_seed",
        type=int,
        default=DEFAULT_IOT_SEED,
        help="Amber source seed",
    )
    run.add_argument(
        "--sensor-period",
        type=int,
        default=10,
        help="IoT sensing/publication period in seconds",
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
        help="accepted network run to reuse for a controlled measurement or campaign",
    )
    run.add_argument("--campaign-id")
    run.add_argument("--condition")
    run.add_argument(
        "--plan",
        action="store_true",
        help="persist/render the immutable run or campaign plan without executing it",
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
    campaign.add_argument("--seeds", help="comma-separated campaign IoT seeds")
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
        description="Measure RAN/UE-path capacity and persist the calibration evidence.",
    )
    calibrate.add_argument("--inventory", type=Path, required=True)
    calibrate.add_argument("--network-run-id", required=True)
    calibrate.add_argument("--target", required=True)
    calibrate.add_argument("--duration-seconds", type=int, default=10)
    calibrate.add_argument("--server-port", type=int, default=5201)
    calibrate.add_argument("--lock", type=Path, default=Path("dependencies.lock.yml"))
    calibrate.add_argument("--out", type=Path, required=True)

    analyze = root.add_parser(
        "analyze",
        help="analyze a completed persisted campaign",
    )
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
        default=None,
        help="R2Lab slice name; SYNTHRAN_R2LAB_SLICE is also honored by the operator",
    )
    release.add_argument("--owner")
    release.add_argument("--allocation-id")
    release.add_argument("--known-hosts", type=Path)
    release.add_argument("--timeout", type=int, default=300)
    release.add_argument("--json", action="store_true")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="synthran",
        description="Run and inspect reproducible SynthRAN experiments across virtual and physical radio backends.",
    )
    parser.add_subparsers(dest="command", required=True)
    configure_operator_parser(parser)

    root = _top_level_subparsers(parser)
    _remove_command(root, "research")
    _remove_command(root, "stop")
    _add_top_level_experiment_commands(root)

    run = root.choices.get("run")
    if run is None:
        raise BackendError("SynthRAN parser does not expose the run command")
    _add_run_measurement_options(run)
    return parser


def _read_json_object(path: Path, *, label: str) -> Mapping[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise BackendError(f"{label} is unreadable") from exc
    if not isinstance(value, dict):
        raise BackendError(f"{label} is malformed")
    return value


def _validate_persisted_iot_identity(args: argparse.Namespace) -> None:
    if args.command != "run" or getattr(args, "radio", None) not in {"rfsim", "r2lab"}:
        return
    if _is_controlled_run(args):
        return

    manifest_path: Path | None = None
    if args.radio == "rfsim":
        candidate = Path(args.experiment_root).expanduser().resolve() / args.run_id / "manifest.json"
        if candidate.is_file():
            manifest_path = candidate
    else:
        result_path = (
            Path(args.r2lab_run_root).expanduser().resolve()
            / args.run_id
            / "physical"
            / "physical-workload-result.json"
        )
        if result_path.is_file():
            result = _read_json_object(
                result_path,
                label="persisted physical workload result",
            )
            workload_id = result.get("workload_id")
            if not isinstance(workload_id, str) or not workload_id:
                raise BackendError("persisted physical workload result has no workload ID")
            candidate = (
                Path(args.r2lab_experiment_root).expanduser().resolve()
                / workload_id
                / "manifest.json"
            )
            if not candidate.is_file():
                raise BackendError("persisted physical workload manifest is unavailable")
            manifest_path = candidate

    if manifest_path is None:
        return
    manifest = _read_json_object(manifest_path, label="persisted IoT workload manifest")
    observed_source = manifest.get("iot_source", "cooja")
    requested_source = getattr(args, "iot_source", "cooja")
    if observed_source != requested_source:
        raise BackendError(
            f"persisted workload uses IoT source {observed_source!r}, "
            f"but this run requested {requested_source!r}"
        )
    if requested_source != "amber":
        return

    expected = {
        "iot_profile": getattr(args, "iot_profile", TRANSPORT_PROFILE),
        "iot_seed": getattr(args, "iot_seed", DEFAULT_IOT_SEED),
        "sensor_period_seconds": getattr(args, "sensor_period", 10),
    }
    for key, requested in expected.items():
        if manifest.get(key) != requested:
            raise BackendError(
                f"persisted Amber workload {key} does not match the requested value"
            )


def _run_amber_workload(
    experiment_args: argparse.Namespace,
    *,
    iot_profile: str,
    iot_seed: int,
    sensor_period_seconds: int,
    energy_power_scale: float,
    energy_node_variation: float,
) -> int:
    manifest, evidence = command_runtime._network_paths(
        experiment_args.network_run_root,
        experiment_args.network_run_id,
    )
    result = execute_amber_experiment(
        inventory=command_runtime.load_inventory(experiment_args.inventory),
        lock=command_runtime.load_lock(experiment_args.lock),
        dependency_root=experiment_args.deps_root,
        network_manifest=manifest,
        network_evidence=evidence,
        run_id=experiment_args.run_id,
        repository_root=command_runtime.repository_root(),
        run_root=experiment_args.run_root,
        collection_seconds=experiment_args.collection_seconds,
        minimum_per_sensor=experiment_args.minimum_per_sensor,
        iot_profile=iot_profile,
        iot_seed=iot_seed,
        sensor_period_seconds=sensor_period_seconds,
        energy_power_scale=energy_power_scale,
        energy_node_variation=energy_node_variation,
        progress=sys.stdout,
    )
    print(f"Run directory: {result.run_directory}")
    if result.evidence_path.is_file():
        print(f"Sanitized evidence: {result.evidence_path}")
    return 0 if result.ready else 2


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
        or getattr(args, "plan", False)
    )


def _require_controlled_common(args: argparse.Namespace) -> None:
    if args.radio not in {None, "rfsim"}:
        raise ResearchError("controlled measurements currently support the RFSIM backend only")
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
        return args.campaign
    if not args.campaign_id:
        raise ResearchError("campaign run requires --campaign-id or --campaign")
    return args.campaign_root.expanduser().resolve() / f"{args.campaign_id}.json"


def _load_or_create_campaign(args: argparse.Namespace):
    path = _campaign_path(args)
    if args.campaign is not None:
        return command_runtime._load_campaign(path), path

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

    campaign = command_runtime.build_campaign(
        campaign_id=args.campaign_id,
        network_run_id=args.network_run_id,
        seeds=command_runtime._parse_seeds(args.seeds),
        conditions=command_runtime._parse_conditions(args.conditions),
        campaign_seed=args.campaign_seed,
    )
    if path.is_file():
        persisted = command_runtime._load_campaign(path)
        if persisted.to_dict() != campaign.to_dict():
            raise ResearchError("persisted campaign differs from the requested immutable schedule")
        return persisted, path
    path.parent.mkdir(parents=True, exist_ok=True)
    command_runtime.save_campaign(campaign, path)
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
        if args.inventory is None or args.probe_target is None:
            raise ResearchError("campaign execution requires --inventory and --probe-target")
        manifest, evidence = command_runtime._network_paths(
            args.network_run_root,
            campaign.network_run_id,
        )
        result_path = execute_amber_campaign(
            campaign=campaign,
            iot_profile=args.iot_profile,
            energy_power_scale=args.energy_power_scale,
            energy_node_variation=args.energy_node_variation,
            inventory=command_runtime.load_inventory(args.inventory),
            lock=command_runtime.load_lock(args.lock),
            dependency_root=args.deps_root,
            network_manifest=manifest,
            network_evidence=evidence,
            repository_root=command_runtime.repository_root(),
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
            progress=sys.stdout,
        )
        print(f"Campaign schedule: {campaign_path}")
        print(f"Amber campaign result: {result_path}")
        return 0

    spec = _amber_research_spec(args)
    if args.plan:
        value = {
            "schema": "synthran/research-request/v2alpha1",
            **spec.to_request_dict(),
        }
        print(json.dumps(value, indent=2, sort_keys=True))
        print("\nExecution action: none")
        return 0
    if args.inventory is None:
        raise ResearchError("controlled run requires --inventory")
    manifest, evidence = command_runtime._network_paths(
        args.network_run_root,
        spec.network_run_id,
    )
    summary_path = execute_amber_research_experiment(
        spec=spec,
        inventory=command_runtime.load_inventory(args.inventory),
        lock=command_runtime.load_lock(args.lock),
        dependency_root=args.deps_root,
        network_manifest=manifest,
        network_evidence=evidence,
        repository_root=command_runtime.repository_root(),
        run_root=args.experiment_root,
        progress=sys.stdout,
    )
    print(f"Amber research summary: {summary_path}")
    return 0


def _dispatch_capacity_calibration(args: argparse.Namespace) -> int:
    payload = command_runtime.calibrate_capacity(
        inventory=command_runtime.load_inventory(args.inventory),
        lock=command_runtime.load_lock(args.lock),
        network_run_id=args.network_run_id,
        target=args.target,
        repository_root=command_runtime.repository_root(),
        output_path=args.out,
        duration_seconds=args.duration_seconds,
        server_port=args.server_port,
    )
    print(json.dumps(payload, indent=2, sort_keys=True))
    print(f"Capacity evidence: {args.out}")
    return 0


def _dispatch_analysis(args: argparse.Namespace) -> int:
    campaign = command_runtime._load_campaign(args.campaign)
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
        command_runtime.load_run_summary(path)
        for scheduled in campaign.runs
        if (path := run_root / scheduled.run_id / "research-summary.json").is_file()
    ]
    analysis = command_runtime.analyze_campaign(campaign, summaries)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(
        json.dumps(analysis, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(analysis, indent=2, sort_keys=True))
    print(f"Campaign analysis: {args.out}")
    return 0 if analysis["usable_runs"] == analysis["expected_runs"] else 2


def _dispatch_release(args: argparse.Namespace) -> int:
    # Preserve the mature ownership-bound cleanup implementation while the
    # public command is named for what it actually does.
    if args.r2lab_slice is None:
        import os

        args.r2lab_slice = os.environ.get("SYNTHRAN_R2LAB_SLICE")
    if args.owner is None:
        import os

        args.owner = os.environ.get("SYNTHRAN_OWNER")
    if args.allocation_id is None:
        import os

        args.allocation_id = os.environ.get("SYNTHRAN_ALLOCATION_ID")
    if args.known_hosts is None:
        import os

        value = os.environ.get("SYNTHRAN_SLICES_KNOWN_HOSTS")
        args.known_hosts = Path(value) if value else None
    return stop_command(args)


def _validate_lifecycle_run(args: argparse.Namespace) -> None:
    required = {
        "--radio": args.radio,
        "--run-id": args.run_id,
        "--core-node": args.core_node,
        "--ran-node": args.ran_node,
    }
    missing = [name for name, value in required.items() if not value]
    if missing:
        raise BackendError("full lifecycle run requires " + ", ".join(missing))


@contextmanager
def _selected_iot_runtime(args: argparse.Namespace) -> Iterator[None]:
    if args.command != "run" or getattr(args, "iot_source", None) != "amber":
        yield
        return
    if _is_controlled_run(args):
        yield
        return

    if args.radio == "rfsim":
        original = command_runtime._experiment_run

        def amber_experiment_run(experiment_args: argparse.Namespace) -> int:
            return _run_amber_workload(
                experiment_args,
                iot_profile=args.iot_profile,
                iot_seed=args.iot_seed,
                sensor_period_seconds=args.sensor_period,
                energy_power_scale=args.energy_power_scale,
                energy_node_variation=args.energy_node_variation,
            )

        command_runtime._experiment_run = amber_experiment_run
        try:
            yield
        finally:
            command_runtime._experiment_run = original
        return

    if args.radio == "r2lab":
        if (
            args.energy_power_scale != DEFAULT_ENERGY_POWER_SCALE
            or args.energy_node_variation != DEFAULT_ENERGY_NODE_VARIATION
        ):
            raise BackendError("Ambient energy treatment is currently supported on RFSIM only")
        original = run_backend.run_physical_workload

        def amber_physical_workload(**kwargs):
            return run_physical_iot_workload(
                **kwargs,
                iot_profile=args.iot_profile,
                iot_seed=args.iot_seed,
                sensor_period_seconds=args.sensor_period,
            )

        run_backend.run_physical_workload = amber_physical_workload
        try:
            yield
        finally:
            run_backend.run_physical_workload = original
        return

    raise BackendError(f"unsupported radio backend: {args.radio}")


def main(argv: Sequence[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    args = _parser().parse_args(arguments)
    try:
        _validate_persisted_iot_identity(args)
        if args.command == "run" and _is_controlled_run(args):
            return _dispatch_controlled_run(args)
        if args.command == "calibrate":
            return _dispatch_capacity_calibration(args)
        if args.command == "analyze":
            return _dispatch_analysis(args)
        if args.command == "release":
            return _dispatch_release(args)
        if args.command == "run":
            _validate_lifecycle_run(args)
        with _selected_iot_runtime(args):
            return dispatch(args)
    except (BackendError, ResearchError, R2LabTopologyResourceError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
