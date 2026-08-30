"""Single public command surface for SynthRAN."""

from __future__ import annotations

import argparse
from contextlib import contextmanager
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


def _top_level_subparsers(parser: argparse.ArgumentParser) -> argparse._SubParsersAction:
    for action in parser._actions:
        if isinstance(action, argparse._SubParsersAction):
            return action
    raise BackendError("SynthRAN parser does not expose top-level commands")


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
        with _selected_iot_runtime(args):
            return dispatch(args)
    except (BackendError, R2LabTopologyResourceError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
