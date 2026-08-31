"""Unified operator rendering around the proven backend run implementations."""

from __future__ import annotations

import argparse
import json

from synthran.backends import run as backend_run
from synthran.backends.base import BackendError
from synthran.run_events import RunProgress


def configure_run_parser(parser: argparse.ArgumentParser) -> None:
    """Reuse the established run argument contract."""

    backend_run.configure_run_parser(parser)


def _persist_failure(progress: RunProgress, detail: str) -> str:
    """Persist the stage failure; the CLI prints the single terminal error line."""

    stage = progress.current_stage or "run"
    normalized = (
        "network"
        if stage == "path" or stage in progress._R2LAB_NETWORK_STAGES
        else stage
    )
    terminal_enabled = progress.stream.terminal_enabled
    progress.stream.terminal_enabled = False
    try:
        progress.fail(detail)
    finally:
        progress.stream.terminal_enabled = terminal_enabled
    return normalized


def _stage_error(stage: str, detail: str) -> BackendError:
    return BackendError(detail if stage == "run" else f"{stage}: {detail}")


class RunCommandAdapter:
    """Execute a backend run through the canonical SynthRAN event stream."""

    def configure_parser(self, parser: argparse.ArgumentParser) -> None:
        configure_run_parser(parser)

    def dispatch(self, args: argparse.Namespace) -> int:
        if args.command != "run":
            raise BackendError("unsupported run command")

        experiment_root = (
            args.experiment_root
            if args.radio == "rfsim"
            else args.r2lab_experiment_root
        )
        network_root = (
            args.network_run_root
            if args.radio == "rfsim"
            else args.r2lab_run_root
        )
        progress = RunProgress(
            enabled=not args.quiet,
            run_id=args.run_id,
            radio=args.radio,
            network_root=network_root,
            experiment_root=experiment_root,
        )
        try:
            backend_run.validate_run_id(args.run_id)
            if args.core_node == args.ran_node:
                raise BackendError("core and RAN nodes must differ")
            payload = (
                backend_run._run_r2lab(args, progress)
                if args.radio == "r2lab"
                else backend_run._run_rfsim(args, progress)
            )
            if args.json:
                print(json.dumps(payload, indent=2, sort_keys=True))
            elif payload.get("released") is True:
                progress.stream.emit(
                    "  physical resources released",
                    stage="cleanup",
                    event="detail",
                )
            return 0
        except BackendError as exc:
            stage = _persist_failure(progress, str(exc))
            raise _stage_error(stage, str(exc)) from exc
        except Exception as exc:
            stage = _persist_failure(progress, str(exc))
            raise _stage_error(stage, str(exc)) from exc
        finally:
            progress.close()
