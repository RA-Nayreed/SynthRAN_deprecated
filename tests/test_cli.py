from __future__ import annotations

import argparse
from pathlib import Path
import tomllib
import unittest
from unittest.mock import patch

from synthran.cli import PUBLIC_COMMANDS, _parser, _selected_iot_runtime


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
        expected = {
            "run",
            "doctor",
            "calibrate",
            "inspect",
            "analyze",
            "release",
            "deps",
            "dev",
        }
        self.assertEqual(expected, set(PUBLIC_COMMANDS))
        self.assertEqual(expected, _top_level_choices(_parser()))

    def test_removed_command_groups_do_not_parse(self) -> None:
        parser = _parser()
        for command in (
            "logs",
            "research",
            "stop",
            "r2lab",
            "network",
            "experiment",
            "slices",
            "privacy",
            "hooks",
        ):
            with self.subTest(command=command), self.assertRaises(SystemExit):
                parser.parse_args([command])

    def test_full_lifecycle_run_selects_rfsim_and_defaults_to_cooja(self) -> None:
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
        self.assertEqual("cooja", args.iot_source)
        self.assertEqual("transport-v1", args.iot_profile)
        self.assertEqual(424242, args.iot_seed)
        self.assertEqual(10, args.sensor_period)
        self.assertEqual(1.0, args.energy_power_scale)
        self.assertEqual(0.0, args.energy_node_variation)

    def test_controlled_ambient_measurement_is_a_run(self) -> None:
        args = _parser().parse_args(
            [
                "run",
                "--campaign-id",
                "campaign-001",
                "--network-run-id",
                "virtual-001",
                "--run-id",
                "measurement-001",
                "--condition",
                "baseline",
                "--inventory",
                "hosts.ini",
                "--probe-target",
                "198.51.100.1",
                "--iot-profile",
                "ambient-v1",
                "--seed",
                "77",
                "--sensor-period",
                "20",
                "--energy-power-scale",
                "0.42",
            ]
        )
        self.assertEqual("run", args.command)
        self.assertIsNone(args.radio)
        self.assertEqual("virtual-001", args.network_run_id)
        self.assertEqual("baseline", args.condition)
        self.assertEqual("ambient-v1", args.iot_profile)
        self.assertEqual(77, args.iot_seed)
        self.assertEqual(20, args.sensor_period)
        self.assertEqual(0.42, args.energy_power_scale)

    def test_run_plan_replaces_research_plan(self) -> None:
        args = _parser().parse_args(
            [
                "run",
                "--plan",
                "--campaign-id",
                "campaign-001",
                "--network-run-id",
                "virtual-001",
                "--run-id",
                "measurement-001",
                "--condition",
                "baseline",
                "--iot-profile",
                "ambient-v1",
            ]
        )
        self.assertTrue(args.plan)
        self.assertEqual("run", args.command)

    def test_campaign_plan_and_execution_share_run_surface(self) -> None:
        args = _parser().parse_args(
            [
                "run",
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
                "--inventory",
                "hosts.ini",
                "--probe-target",
                "198.51.100.1",
                "--iot-profile",
                "ambient-v1",
            ]
        )
        self.assertEqual("run", args.command)
        self.assertEqual("1,2,3", args.seeds)
        self.assertEqual("baseline,load50=0.5", args.conditions)
        self.assertEqual(123, args.campaign_seed)

    def test_capacity_calibration_is_top_level(self) -> None:
        args = _parser().parse_args(
            [
                "calibrate",
                "--inventory",
                "hosts.ini",
                "--network-run-id",
                "virtual-001",
                "--target",
                "198.51.100.1",
                "--out",
                "capacity.json",
            ]
        )
        self.assertEqual("calibrate", args.command)
        self.assertEqual("virtual-001", args.network_run_id)
        self.assertEqual(Path("capacity.json"), args.out)

    def test_analysis_is_top_level(self) -> None:
        args = _parser().parse_args(
            [
                "analyze",
                "--campaign",
                "campaign.json",
                "--out",
                "analysis.json",
            ]
        )
        self.assertEqual("analyze", args.command)
        self.assertEqual(Path("campaign.json"), args.campaign)

    def test_release_replaces_stop(self) -> None:
        args = _parser().parse_args(["release", "--run-id", "physical-001"])
        self.assertEqual("release", args.command)
        self.assertEqual("physical-001", args.run_id)

    def test_cooja_lifecycle_does_not_replace_experiment_runtime(self) -> None:
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

    def test_amber_rfsim_lifecycle_runtime_is_restored_after_scope(self) -> None:
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
                "--energy-power-scale",
                "0.42",
            ]
        )
        from synthran import command_runtime

        original = command_runtime._experiment_run
        with _selected_iot_runtime(args):
            self.assertIsNot(original, command_runtime._experiment_run)
        self.assertIs(original, command_runtime._experiment_run)

    def test_controlled_run_does_not_patch_lifecycle_runtime(self) -> None:
        args = _parser().parse_args(
            [
                "run",
                "--campaign-id",
                "campaign-001",
                "--network-run-id",
                "virtual-001",
                "--run-id",
                "measurement-001",
                "--condition",
                "baseline",
                "--iot-profile",
                "ambient-v1",
            ]
        )
        from synthran import command_runtime

        original = command_runtime._experiment_run
        with _selected_iot_runtime(args):
            self.assertIs(original, command_runtime._experiment_run)

    def test_amber_physical_runtime_passes_source_settings_and_restores_scope(self) -> None:
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

    def test_repository_maintenance_remains_namespaced(self) -> None:
        args = _parser().parse_args(["dev", "privacy", "scan", "--worktree"])
        self.assertEqual("dev", args.command)
        self.assertEqual("privacy", args.dev_command)
        self.assertEqual("scan", args.privacy_command)


if __name__ == "__main__":
    unittest.main()
