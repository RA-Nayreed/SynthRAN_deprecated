from __future__ import annotations

import argparse
from pathlib import Path
import tomllib
import unittest
from unittest.mock import patch

from synthran.backends.base import BackendError
from synthran.cli import _parser, _selected_iot_runtime
from synthran.operator import PUBLIC_COMMANDS


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


def _top_level_choices(parser: argparse.ArgumentParser) -> set[str]:
    action = next(
        item for item in parser._actions if isinstance(item, argparse._SubParsersAction)
    )
    return set(action.choices)


class CliTests(unittest.TestCase):
    def test_package_exposes_one_synthran_executable(self) -> None:
        project = tomllib.loads(
            (REPOSITORY_ROOT / "pyproject.toml").read_text(encoding="utf-8")
        )
        self.assertEqual(
            project["project"]["scripts"],
            {"synthran": "synthran.launcher:main"},
        )

    def test_public_surface_is_intentionally_small(self) -> None:
        self.assertEqual(set(PUBLIC_COMMANDS), _top_level_choices(_parser()))
        self.assertEqual(
            {"run", "doctor", "inspect", "logs", "stop", "research", "deps", "dev"},
            set(PUBLIC_COMMANDS),
        )

    def test_removed_command_groups_do_not_parse(self) -> None:
        parser = _parser()
        for command in ("r2lab", "network", "experiment", "slices", "privacy", "hooks"):
            with self.subTest(command=command), self.assertRaises(SystemExit):
                parser.parse_args([command])

    def test_run_selects_rfsim_and_keeps_cooja_transition_default(self) -> None:
        args = _parser().parse_args(
            [
                "run",
                "--radio",
                "rfsim",
                "--core-node",
                "sopnode-f2",
                "--ran-node",
                "sopnode-f3",
                "--run-id",
                "virtual-001",
            ]
        )
        self.assertEqual("run", args.command)
        self.assertEqual("rfsim", args.radio)
        self.assertIsNone(args.device)
        self.assertIsNone(args.ue)
        self.assertEqual("cooja", args.iot_source)
        self.assertEqual("transport-v1", args.iot_profile)
        self.assertEqual(424242, args.iot_seed)
        self.assertEqual(10, args.sensor_period)

    def test_run_parses_explicit_amber_profile_seed_and_period(self) -> None:
        args = _parser().parse_args(
            [
                "run",
                "--radio",
                "rfsim",
                "--core-node",
                "sopnode-f2",
                "--ran-node",
                "sopnode-f3",
                "--run-id",
                "amber-001",
                "--iot-source",
                "amber",
                "--iot-profile",
                "ambient-v1",
                "--iot-seed",
                "17",
                "--sensor-period",
                "12",
            ]
        )
        self.assertEqual("amber", args.iot_source)
        self.assertEqual("ambient-v1", args.iot_profile)
        self.assertEqual(17, args.iot_seed)
        self.assertEqual(12, args.sensor_period)

    def test_cooja_selection_does_not_replace_experiment_runtime(self) -> None:
        args = _parser().parse_args(
            [
                "run",
                "--radio",
                "rfsim",
                "--core-node",
                "sopnode-f2",
                "--ran-node",
                "sopnode-f3",
                "--run-id",
                "virtual-001",
            ]
        )
        from synthran import command_runtime

        original = command_runtime._experiment_run
        with _selected_iot_runtime(args):
            self.assertIs(original, command_runtime._experiment_run)
        self.assertIs(original, command_runtime._experiment_run)

    def test_amber_runtime_override_is_restored_after_scope(self) -> None:
        args = _parser().parse_args(
            [
                "run",
                "--radio",
                "rfsim",
                "--core-node",
                "sopnode-f2",
                "--ran-node",
                "sopnode-f3",
                "--run-id",
                "amber-001",
                "--iot-source",
                "amber",
            ]
        )
        from synthran import command_runtime

        original = command_runtime._experiment_run
        with _selected_iot_runtime(args):
            self.assertIsNot(original, command_runtime._experiment_run)
        self.assertIs(original, command_runtime._experiment_run)

    def test_amber_physical_backend_fails_closed_until_adapter_exists(self) -> None:
        args = _parser().parse_args(
            [
                "run",
                "--radio",
                "r2lab",
                "--device",
                "n300",
                "--ue",
                "qfit07",
                "--core-node",
                "sopnode-f2",
                "--ran-node",
                "sopnode-f3",
                "--run-id",
                "physical-001",
                "--iot-source",
                "amber",
            ]
        )
        with self.assertRaisesRegex(BackendError, "not enabled for R2Lab"):
            with _selected_iot_runtime(args):
                pass

    def test_run_selects_physical_backend(self) -> None:
        args = _parser().parse_args(
            [
                "run",
                "--radio",
                "r2lab",
                "--device",
                "n300",
                "--ue",
                "qfit07",
                "--core-node",
                "sopnode-f2",
                "--ran-node",
                "sopnode-f3",
                "--run-id",
                "physical-001",
            ]
        )
        self.assertEqual("r2lab", args.radio)
        self.assertEqual("n300", args.device)
        self.assertEqual("qfit07", args.ue)

    def test_research_is_top_level(self) -> None:
        args = _parser().parse_args(
            [
                "research",
                "campaign-plan",
                "--campaign-id",
                "campaign-001",
                "--network-run-id",
                "virtual-001",
                "--seeds",
                "1,2,3",
                "--conditions",
                "baseline,load50=0.5",
                "--campaign-seed",
                "123",
                "--out",
                "campaign.json",
            ]
        )
        self.assertEqual("research", args.command)
        self.assertEqual("campaign-plan", args.research_command)

    def test_repository_maintenance_is_namespaced(self) -> None:
        args = _parser().parse_args(["dev", "privacy", "scan", "--worktree"])
        self.assertEqual("dev", args.command)
        self.assertEqual("privacy", args.dev_command)
        self.assertEqual("scan", args.privacy_command)


if __name__ == "__main__":
    unittest.main()
