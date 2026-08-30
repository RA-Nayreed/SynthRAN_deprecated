from __future__ import annotations

import argparse
from pathlib import Path
import tomllib
import unittest
from unittest.mock import patch

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

    def test_run_defaults_to_amber_transport_profile(self) -> None:
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
        self.assertFalse(hasattr(args, "iot_source"))
        self.assertEqual("transport-v1", args.iot_profile)
        self.assertEqual(424242, args.iot_seed)
        self.assertEqual(10, args.sensor_period)

    def test_removed_source_selector_is_rejected(self) -> None:
        parser = _parser()
        for source in ("cooja", "amber"):
            with self.subTest(source=source), self.assertRaises(SystemExit):
                parser.parse_args(
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
                        "--iot-source",
                        source,
                    ]
                )

    def test_run_parses_explicit_profile_seed_and_period(self) -> None:
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
                "--iot-profile",
                "ambient-v1",
                "--iot-seed",
                "17",
                "--sensor-period",
                "12",
            ]
        )
        self.assertFalse(hasattr(args, "iot_source"))
        self.assertEqual("ambient-v1", args.iot_profile)
        self.assertEqual(17, args.iot_seed)
        self.assertEqual(12, args.sensor_period)

    def test_rfsim_runtime_is_restored_after_scope(self) -> None:
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
            ]
        )
        from synthran import command_runtime

        original = command_runtime._experiment_run
        with _selected_iot_runtime(args):
            self.assertIsNot(original, command_runtime._experiment_run)
        self.assertIs(original, command_runtime._experiment_run)

    def test_physical_runtime_passes_source_settings_and_restores_scope(self) -> None:
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
                "--iot-profile",
                "ambient-v1",
                "--iot-seed",
                "17",
                "--sensor-period",
                "12",
            ]
        )
        from synthran.backends import run as run_backend

        original = run_backend.run_physical_workload
        with patch("synthran.cli.run_physical_iot_workload", return_value="ok") as execute:
            with _selected_iot_runtime(args):
                self.assertIsNot(original, run_backend.run_physical_workload)
                result = run_backend.run_physical_workload(run_id="physical-001")
            self.assertIs(original, run_backend.run_physical_workload)
        self.assertEqual("ok", result)
        execute.assert_called_once_with(
            run_id="physical-001",
            iot_profile="ambient-v1",
            iot_seed=17,
            sensor_period_seconds=12,
        )

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

    def test_research_run_defaults_to_transport_profile(self) -> None:
        args = _parser().parse_args(
            [
                "research",
                "run",
                "--campaign-id",
                "campaign-001",
                "--network-run-id",
                "virtual-001",
                "--run-id",
                "research-001",
                "--condition",
                "baseline",
                "--probe-target",
                "198.51.100.1",
                "--inventory",
                "hosts.ini",
            ]
        )
        self.assertEqual("transport-v1", args.iot_profile)
        self.assertEqual(424242, args.seed)
        self.assertEqual(10, args.sensor_period)

    def test_research_keeps_seed_flag_for_iot_seed(self) -> None:
        args = _parser().parse_args(
            [
                "research",
                "run",
                "--campaign-id",
                "campaign-001",
                "--network-run-id",
                "virtual-001",
                "--run-id",
                "research-001",
                "--condition",
                "baseline",
                "--probe-target",
                "198.51.100.1",
                "--inventory",
                "hosts.ini",
                "--iot-profile",
                "ambient-v1",
                "--seed",
                "77",
                "--sensor-period",
                "20",
            ]
        )
        self.assertEqual("ambient-v1", args.iot_profile)
        self.assertEqual(77, args.seed)
        self.assertEqual(20, args.sensor_period)

    def test_campaign_and_analysis_accept_profile_selection(self) -> None:
        campaign = _parser().parse_args(
            [
                "research",
                "campaign-run",
                "--campaign",
                "campaign.json",
                "--inventory",
                "hosts.ini",
                "--target",
                "198.51.100.1",
                "--iot-profile",
                "transport-v1",
            ]
        )
        analyze = _parser().parse_args(
            [
                "research",
                "analyze",
                "--campaign",
                "campaign.json",
                "--out",
                "analysis.json",
                "--iot-profile",
                "transport-v1",
            ]
        )
        self.assertEqual("transport-v1", campaign.iot_profile)
        self.assertEqual("transport-v1", analyze.iot_profile)

    def test_repository_maintenance_is_namespaced(self) -> None:
        args = _parser().parse_args(["dev", "privacy", "scan", "--worktree"])
        self.assertEqual("dev", args.command)
        self.assertEqual("privacy", args.dev_command)
        self.assertEqual("scan", args.privacy_command)


if __name__ == "__main__":
    unittest.main()
