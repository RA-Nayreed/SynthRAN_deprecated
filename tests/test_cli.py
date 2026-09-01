from __future__ import annotations

import argparse
from pathlib import Path
import tomllib
import unittest
from unittest.mock import patch

from synthran.cli import PUBLIC_COMMANDS, _parser, main as cli_main


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
            {"synthran": "synthran.cli:main"},
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

    def test_full_lifecycle_run_defaults_to_ambient_amber(self) -> None:
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
        self.assertFalse(hasattr(args, "iot_source"))
        self.assertEqual("ambient-v1", args.iot_profile)
        self.assertEqual(424242, args.iot_seed)
        self.assertEqual(10, args.sensor_period)
        self.assertEqual(1.0, args.energy_power_scale)
        self.assertEqual(0.0, args.energy_node_variation)

    def test_removed_iot_source_selector_does_not_parse(self) -> None:
        parser = _parser()
        with self.assertRaises(SystemExit):
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
                    "cooja",
                ]
            )

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
            ["analyze", "--campaign", "campaign.json", "--out", "analysis.json"]
        )
        self.assertEqual("analyze", args.command)
        self.assertEqual(Path("analysis.json"), args.out)

    def test_release_replaces_stop(self) -> None:
        args = _parser().parse_args(["release", "--run-id", "physical-001"])
        self.assertEqual("release", args.command)
        self.assertEqual("physical-001", args.run_id)

    @patch("synthran.cli.execute_run", return_value={"accepted": True})
    def test_rfsim_amber_settings_pass_directly_to_lifecycle(self, execute) -> None:
        self.assertEqual(
            0,
            cli_main(
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
                    "--slices-project",
                    "project-test",
                    "--iot-profile",
                    "ambient-v1",
                    "--iot-seed",
                    "17",
                    "--sensor-period",
                    "12",
                    "--energy-power-scale",
                    "0.42",
                ]
            ),
        )
        args = execute.call_args.args[0]
        self.assertEqual("project-test", args.slices_project)
        self.assertEqual("ambient-v1", args.iot_profile)
        self.assertEqual(17, args.iot_seed)
        self.assertEqual(12, args.sensor_period)
        self.assertEqual(0.42, args.energy_power_scale)

    @patch("synthran.cli.execute_run", return_value={"accepted": True})
    def test_physical_amber_settings_pass_directly_to_lifecycle(self, execute) -> None:
        self.assertEqual(
            0,
            cli_main(
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
                    "--slices-project",
                    "project-test",
                    "--iot-profile",
                    "ambient-v1",
                    "--iot-seed",
                    "17",
                    "--sensor-period",
                    "12",
                ]
            ),
        )
        args = execute.call_args.args[0]
        self.assertEqual("project-test", args.slices_project)
        self.assertEqual("ambient-v1", args.iot_profile)
        self.assertEqual(17, args.iot_seed)
        self.assertEqual(12, args.sensor_period)

    def test_physical_energy_treatment_remains_fail_closed(self) -> None:
        self.assertEqual(
            2,
            cli_main(
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
                    "physical-energy-001",
                    "--slices-project",
                    "project-test",
                    "--energy-power-scale",
                    "0.42",
                ]
            ),
        )

    def test_repository_maintenance_remains_namespaced(self) -> None:
        args = _parser().parse_args(["dev", "privacy", "scan", "--worktree"])
        self.assertEqual("dev", args.command)
        self.assertEqual("privacy", args.dev_command)
        self.assertEqual("scan", args.privacy_command)


if __name__ == "__main__":
    unittest.main()
