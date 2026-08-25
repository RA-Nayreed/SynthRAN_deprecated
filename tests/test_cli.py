from __future__ import annotations

import argparse
from pathlib import Path
import tomllib
import unittest

from synthran.cli import _parser
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

    def test_run_selects_rfsim(self) -> None:
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
