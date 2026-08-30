"""Single public command surface for SynthRAN."""

from __future__ import annotations

import argparse
from contextlib import contextmanager
import json
import sys
from typing import Iterator, Sequence

from synthran import command_runtime
from synthran.amber_experiment_runtime import execute_amber_experiment
from synthran.backends.base import BackendError
from synthran.iot_source import (
    AMBIENT_PROFILE,
    DEFAULT_IOT_SEED,
    TRANSPORT_PROFILE,
)
from synthran.operator import configure_operator_parser, dispatch
from synthran.r2lab.resources import R2LabTopologyResourceError
from synthran.research import LoadSpec, MeasurementSpec, ResearchError
from synthran.research.amber_runtime import execute_amber_research_experiment
from synthran.research.v2 import AmberResearchSpec


def _subparsers(parser: argparse.ArgumentParser) -> argparse._SubParsersAction:
    for action in parser._actions:
        if isinstance(action, argparse._SubParsersAction):
            return action
    raise BackendError("SynthRAN parser does not expose command choices")


def _top_level_subparsers(parser: argparse.ArgumentParser) -> argparse._SubParsersAction:
    return _subparsers(parser)


def _augment_iot_options(parser: argparse.ArgumentParser) -> None:
    root = _top_level_subparsers(parser)
    run = root.choices.get("run")
    if run is None:
        raise BackendError("SynthRAN parser does not expose the run command")
    run.add_argument(
        "--iot-source",
        choices=("cooja", "amber"),
        default="cooja",
        help="IoT source; Cooja remains the transitional default until Amber cutover",
    )
    run.add_argument(
        "--iot-profile",
        choices=(TRANSPORT_PROFILE, AMBIENT_PROFILE),
        default=TRANSPORT_PROFILE,
        help="Amber source profile",
    )
    run.add_argument(
        "--iot-seed",
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

    research = root.choices.get("research")
    if research is None:
        raise BackendError("SynthRAN parser does not expose the research command")
    research_sub = _subparsers(research)
    for name in ("plan", "run", "campaign-run"):
        child = research_sub.choices.get(name)
        if child is None:
            raise BackendError(f"SynthRAN research parser does not expose {name}")
        child.add_argument(
            "--iot-profile",
            choices=(TRANSPORT_PROFILE, AMBIENT_PROFILE),
            default=None,
            help=(
                "select Amber research v2; omit for immutable legacy Cooja v1 behavior"
            ),
        )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="synthran",
        description="Run and inspect reproducible SynthRAN experiments across virtual and physical radio backends.",
    )
    parser.add_subparsers(dest="command", required=True)
    configure_operator_parser(parser)
    _augment_iot_options(parser)
    return parser


def _run_amber_workload(
    experiment_args: argparse.Namespace,
    *,
    iot_profile: str,
    iot_seed: int,
    sensor_period_seconds: int,
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
        progress=sys.stdout,
    )
    print(f"Run directory: {result.run_directory}")
    if result.evidence_path.is_file():
        print(f"Sanitized evidence: {result.evidence_path}")
    return 0 if result.ready else 2


def _amber_research_spec(args: argparse.Namespace) -> AmberResearchSpec:
    loaded = args.condition != "baseline"
    return AmberResearchSpec(
        campaign_id=args.campaign_id,
        run_id=args.run_id,
        network_run_id=args.network_run_id,
        condition=args.condition,
        iot_profile=args.iot_profile,
        iot_seed=args.seed,
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


def _dispatch_amber_research(args: argparse.Namespace) -> int:
    if args.research_command == "plan":
        value = {
            "schema": "synthran/research-request/v2alpha1",
            **_amber_research_spec(args).to_request_dict(),
        }
        print(json.dumps(value, indent=2, sort_keys=True))
        print("\nExecution action: none")
        return 0
    if args.research_command == "run":
        spec = _amber_research_spec(args)
        manifest, evidence = command_runtime._network_paths(
            args.network_run_root,
            args.network_run_id,
        )
        summary_path = execute_amber_research_experiment(
            spec=spec,
            inventory=command_runtime.load_inventory(args.inventory),
            lock=command_runtime.load_lock(args.lock),
            dependency_root=args.deps_root,
            network_manifest=manifest,
            network_evidence=evidence,
            repository_root=command_runtime.repository_root(),
            run_root=args.run_root,
            progress=sys.stdout,
        )
        print(f"Amber research summary: {summary_path}")
        return 0
    if args.research_command == "campaign-run":
        raise ResearchError(
            "Amber campaign execution is not enabled until the v2 campaign runner is installed"
        )
    raise ResearchError(
        "--iot-profile is supported on research plan, run, and campaign-run only"
    )


@contextmanager
def _selected_iot_runtime(args: argparse.Namespace) -> Iterator[None]:
    if args.command != "run" or getattr(args, "iot_source", "cooja") == "cooja":
        yield
        return
    if args.radio != "rfsim":
        raise BackendError(
            "--iot-source amber is not enabled for R2Lab until the physical Amber adapter is merged"
        )

    original = command_runtime._experiment_run

    def amber_experiment_run(experiment_args: argparse.Namespace) -> int:
        return _run_amber_workload(
            experiment_args,
            iot_profile=args.iot_profile,
            iot_seed=args.iot_seed,
            sensor_period_seconds=args.sensor_period,
        )

    command_runtime._experiment_run = amber_experiment_run
    try:
        yield
    finally:
        command_runtime._experiment_run = original


def main(argv: Sequence[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    args = _parser().parse_args(arguments)
    try:
        if args.command == "research" and getattr(args, "iot_profile", None) is not None:
            return _dispatch_amber_research(args)
        with _selected_iot_runtime(args):
            return dispatch(args)
    except (BackendError, ResearchError, R2LabTopologyResourceError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
