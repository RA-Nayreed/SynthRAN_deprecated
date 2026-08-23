from __future__ import annotations

import tomllib
from pathlib import Path
import unittest
from unittest.mock import patch

from synthran.cli import _parser
from synthran.launcher import main as launch


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


class CliTests(unittest.TestCase):
    def test_package_exposes_one_synthran_executable(self) -> None:
        project = tomllib.loads(
            (REPOSITORY_ROOT / "pyproject.toml").read_text(encoding="utf-8")
        )
        self.assertEqual(
            project["project"]["scripts"],
            {"synthran": "synthran.launcher:main"},
        )

    def test_empty_argv_opens_interactive_terminal(self) -> None:
        with patch("synthran.terminal.shell.run_terminal", return_value=0) as terminal:
            self.assertEqual(launch([]), 0)
        terminal.assert_called_once_with()

    def test_explicit_argv_preserves_scripted_cli(self) -> None:
        with patch("synthran.cli.main", return_value=7) as cli_main:
            self.assertEqual(launch(["privacy", "scan", "--worktree"]), 7)
        cli_main.assert_called_once_with(["privacy", "scan", "--worktree"])

    def test_parser_contains_experiment_commands(self) -> None:
        parser = _parser()
        args = parser.parse_args(
            [
                "experiment",
                "plan",
                "--network-run-id",
                "network-accepted-01",
                "--run-id",
                "experiment-01",
            ]
        )
        self.assertEqual(args.command, "experiment")
        self.assertEqual(args.experiment_command, "plan")

    def test_parser_keeps_network_commands(self) -> None:
        parser = _parser()
        args = parser.parse_args(
            [
                "network",
                "verify",
                "--inventory",
                "hosts.ini",
                "--run-id",
                "network-accepted-01",
            ]
        )
        self.assertEqual(args.command, "network")
        self.assertEqual(args.network_command, "verify")

    def test_parser_contains_r2lab_commands(self) -> None:
        parser = _parser()
        args = parser.parse_args(
            [
                "r2lab",
                "plan",
                "--slice",
                "oulu_user",
                "--radio",
                "n300",
                "--ue",
                "qhat01",
                "--run-id",
                "r2lab-test-01",
            ]
        )
        self.assertEqual(args.command, "r2lab")
        self.assertEqual(args.r2lab_command, "plan")
        self.assertEqual(args.radio, "n300")
        self.assertEqual(args.ue, "qhat01")

    def test_parser_contains_physical_foundation_command(self) -> None:
        args = _parser().parse_args(
            [
                "r2lab",
                "foundation",
                "--slice",
                "oulu_rnayreed",
                "--run-id",
                "r2lab-current-run",
                "--previous-run-id",
                "r2lab-previous-run",
                "--owner",
                "rnayreed",
                "--reservation-id",
                "6360",
                "--allocation-id",
                "rnayreed_260824_090000_000001",
                "--known-hosts",
                "known_hosts",
            ]
        )

        self.assertEqual("foundation", args.r2lab_command)
        self.assertEqual(Path("known_hosts"), args.known_hosts)

    def test_parser_contains_stopped_gnb_and_n2_commands(self) -> None:
        stage = _parser().parse_args(
            [
                "r2lab",
                "gnb-stage",
                "--slice",
                "oulu_rnayreed",
                "--run-id",
                "r2lab-current-run",
                "--owner",
                "rnayreed",
                "--reservation-id",
                "6366",
                "--allocation-id",
                "allocation-1",
                "--known-hosts",
                "known_hosts",
                "--amf-n2-address",
                "10.10.3.200",
                "--gnb-n2-address",
                "10.10.3.234",
                "--n300-address",
                "10.10.4.203",
                "--ru-pod-address",
                "10.10.4.234",
                "--ru-subnet",
                "10.10.4.0/24",
            ]
        )
        start = _parser().parse_args(
            [
                "r2lab",
                "gnb-start",
                "--slice",
                "oulu_rnayreed",
                "--run-id",
                "r2lab-current-run",
                "--owner",
                "rnayreed",
                "--reservation-id",
                "6366",
                "--allocation-id",
                "allocation-1",
                "--known-hosts",
                "known_hosts",
            ]
        )

        self.assertEqual("gnb-stage", stage.r2lab_command)
        self.assertEqual("10.10.3.234", stage.gnb_n2_address)
        self.assertEqual("gnb-start", start.r2lab_command)
        self.assertEqual(12, start.n2_attempts)


if __name__ == "__main__":
    unittest.main()
