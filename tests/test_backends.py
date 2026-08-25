from __future__ import annotations

import argparse
import io
from pathlib import Path
import unittest
from unittest.mock import Mock, patch

from synthran.backends import (
    BackendContract,
    BackendError,
    LIFECYCLE_STAGES,
    LifecycleStage,
    R2LabBackend,
    RfsimBackend,
    backend_for_argv,
    get_backend,
)
from synthran.cli import _parser, main as cli_main


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


class BackendContractTests(unittest.TestCase):
    def test_backend_capability_must_follow_lifecycle_order(self) -> None:
        with self.assertRaises(ValueError):
            BackendContract(
                name="rfsim",
                radio_mode="virtual",
                implemented_stages=(LifecycleStage.ACCESS, LifecycleStage.GNB),
            )

    def test_rfsim_implements_the_full_reference_contract(self) -> None:
        backend = get_backend("rfsim")
        self.assertIsInstance(backend, RfsimBackend)
        self.assertEqual("virtual", backend.contract.radio_mode)
        self.assertEqual(LIFECYCLE_STAGES, backend.contract.implemented_stages)

    def test_r2lab_implements_the_same_static_lifecycle_contract(self) -> None:
        backend = get_backend("r2lab")
        self.assertIsInstance(backend, R2LabBackend)
        self.assertEqual("physical", backend.contract.radio_mode)
        self.assertEqual(LIFECYCLE_STAGES, backend.contract.implemented_stages)
        self.assertTrue(backend.contract.supports(LifecycleStage.CLEANUP))

    def test_cli_backend_resolution_keeps_experiment_above_boundary(self) -> None:
        self.assertIsInstance(backend_for_argv(["doctor"]), RfsimBackend)
        self.assertIsInstance(backend_for_argv(["network", "verify"]), RfsimBackend)
        self.assertIsInstance(backend_for_argv(["r2lab", "path-up"]), R2LabBackend)
        self.assertIsNone(backend_for_argv(["experiment", "plan"]))
        self.assertIsNone(backend_for_argv(["privacy", "scan"]))

    def test_single_parser_contains_topology_path_and_workload_commands(self) -> None:
        plan = _parser().parse_args(
            [
                "r2lab",
                "plan",
                "--slice",
                "oulu_user",
                "--radio",
                "n320",
                "--ue",
                "qhat23",
                "--core-node",
                "sopnode-f1",
                "--ran-node",
                "sopnode-w3",
                "--run-id",
                "r2lab-run-001",
            ]
        )
        path = _parser().parse_args(
            [
                "r2lab",
                "path-up",
                "--slice",
                "oulu_user",
                "--known-hosts",
                "known_hosts",
                "--run-id",
                "r2lab-run-001",
                "--peer",
                "198.51.100.10",
            ]
        )
        workload = _parser().parse_args(
            [
                "r2lab",
                "workload-run",
                "--slice",
                "oulu_user",
                "--known-hosts",
                "known_hosts",
                "--run-id",
                "r2lab-run-001",
                "--workload-id",
                "physical-iot-001",
                "--inventory",
                "hosts.ini",
            ]
        )
        self.assertEqual("n320", plan.radio)
        self.assertEqual("qhat23", plan.ue)
        self.assertEqual("sopnode-f1", plan.core_node)
        self.assertEqual("sopnode-w3", plan.ran_node)
        self.assertEqual("path-up", path.r2lab_command)
        self.assertEqual("198.51.100.10", path.peer)
        self.assertEqual("workload-run", workload.r2lab_command)
        self.assertEqual(Path("hosts.ini"), workload.inventory)

    def test_rfsim_adapter_owns_virtual_command_execution(self) -> None:
        args = argparse.Namespace(command="network", network_command="verify")
        with patch(
            "synthran.backends.rfsim.command_runtime._network_verify",
            return_value=7,
        ) as verify:
            self.assertEqual(7, RfsimBackend().dispatch(args))
        verify.assert_called_once_with(args)

    @patch("synthran.backends.r2lab.continue_physical_path")
    def test_r2lab_adapter_owns_physical_path_execution(self, continue_path) -> None:
        summary = Mock()
        summary.ready_for_workload = True
        summary.evidence_path = Path("physical-run.json")
        continue_path.return_value = summary
        args = argparse.Namespace(
            command="r2lab",
            r2lab_command="path-up",
            r2lab_slice="oulu_user",
            owner="rnayreed",
            allocation_id=None,
            known_hosts=Path("known_hosts"),
            run_id="r2lab-run-001",
            peer="198.51.100.10",
            run_root=Path(".synthran/r2lab"),
            timeout=30,
            json=False,
        )
        self.assertEqual(0, R2LabBackend().dispatch(args))
        continue_path.assert_called_once()

    @patch("synthran.backends.r2lab.run_physical_workload")
    def test_r2lab_adapter_owns_physical_workload_execution(self, run_workload) -> None:
        summary = Mock()
        summary.accepted = True
        summary.workload_result_path = Path("physical-workload-result.json")
        run_workload.return_value = summary
        args = argparse.Namespace(
            command="r2lab",
            r2lab_command="workload-run",
            r2lab_slice="oulu_user",
            owner="rnayreed",
            allocation_id=None,
            known_hosts=Path("known_hosts"),
            run_id="r2lab-run-001",
            workload_id="physical-iot-001",
            inventory=Path("hosts.ini"),
            lock=Path("dependencies.lock.yml"),
            deps_root=Path(".deps"),
            run_root=Path(".synthran/r2lab"),
            experiment_root=Path(".synthran/experiments-r2lab"),
            collection_seconds=180,
            minimum_per_sensor=3,
            timeout=30,
            json=False,
        )
        self.assertEqual(0, R2LabBackend().dispatch(args))
        run_workload.assert_called_once()


class CliBoundaryTests(unittest.TestCase):
    def test_backend_commands_dispatch_through_backend_boundary(self) -> None:
        backend = Mock()
        backend.dispatch.return_value = 5
        parsed = argparse.Namespace(command="network", network_command="verify")
        parser = Mock()
        parser.parse_args.return_value = parsed
        with (
            patch("synthran.cli.backend_for_argv", return_value=backend),
            patch("synthran.cli._parser", return_value=parser),
        ):
            self.assertEqual(5, cli_main(["network", "verify"]))
        backend.dispatch.assert_called_once_with(parsed)

    def test_non_backend_commands_delegate_to_command_runtime(self) -> None:
        with (
            patch("synthran.cli.backend_for_argv", return_value=None),
            patch("synthran.cli.command_runtime.main", return_value=3) as runtime_main,
        ):
            self.assertEqual(3, cli_main(["privacy", "scan", "--worktree"]))
        runtime_main.assert_called_once_with(["privacy", "scan", "--worktree"])

    def test_backend_errors_use_the_single_cli_error_surface(self) -> None:
        backend = Mock()
        backend.dispatch.side_effect = BackendError("boundary failure")
        parser = Mock()
        parser.parse_args.return_value = argparse.Namespace(command="r2lab")
        stderr = io.StringIO()
        with (
            patch("synthran.cli.backend_for_argv", return_value=backend),
            patch("synthran.cli._parser", return_value=parser),
            patch("sys.stderr", stderr),
        ):
            self.assertEqual(2, cli_main(["r2lab", "path-up"]))
        self.assertEqual("error: boundary failure\n", stderr.getvalue())

    def test_cli_module_is_dispatch_only(self) -> None:
        source = (REPOSITORY_ROOT / "synthran" / "cli.py").read_text(encoding="utf-8")
        self.assertLessEqual(len(source.splitlines()), 45)
        self.assertNotIn("synthran.r2lab", source)
        self.assertNotIn("synthran.network", source)
        self.assertNotIn("synthran.experiment", source)
        self.assertNotIn("synthran.research", source)


if __name__ == "__main__":
    unittest.main()
