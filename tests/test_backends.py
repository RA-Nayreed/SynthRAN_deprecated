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
from synthran.cli import main as cli_main


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

    def test_r2lab_capability_is_explicitly_bounded_at_n2(self) -> None:
        backend = get_backend("r2lab")
        self.assertIsInstance(backend, R2LabBackend)
        self.assertEqual("physical", backend.contract.radio_mode)
        self.assertTrue(backend.contract.supports(LifecycleStage.N2))
        self.assertFalse(backend.contract.supports(LifecycleStage.UE_MANAGEMENT))

    def test_cli_backend_resolution_keeps_experiment_above_boundary(self) -> None:
        self.assertIsInstance(backend_for_argv(["doctor"]), RfsimBackend)
        self.assertIsInstance(backend_for_argv(["network", "verify"]), RfsimBackend)
        self.assertIsInstance(backend_for_argv(["r2lab", "plan"]), R2LabBackend)
        self.assertIsNone(backend_for_argv(["experiment", "plan"]))
        self.assertIsNone(backend_for_argv(["privacy", "scan"]))

    def test_rfsim_adapter_owns_virtual_command_execution(self) -> None:
        args = argparse.Namespace(command="network", network_command="verify")
        with patch(
            "synthran.backends.rfsim.command_runtime._network_verify",
            return_value=7,
        ) as verify:
            self.assertEqual(7, RfsimBackend().dispatch(args))
        verify.assert_called_once_with(args)

    def test_r2lab_adapter_owns_physical_command_execution(self) -> None:
        args = argparse.Namespace(command="r2lab", r2lab_command="plan")
        with patch(
            "synthran.backends.r2lab.command_runtime._dispatch_r2lab",
            return_value=9,
        ) as dispatch:
            self.assertEqual(9, R2LabBackend().dispatch(args))
        dispatch.assert_called_once_with(args)


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
            self.assertEqual(2, cli_main(["r2lab", "plan"]))
        self.assertEqual("error: boundary failure\n", stderr.getvalue())

    def test_cli_module_is_dispatch_only(self) -> None:
        source = (REPOSITORY_ROOT / "synthran" / "cli.py").read_text(encoding="utf-8")
        self.assertLessEqual(len(source.splitlines()), 40)
        self.assertNotIn("synthran.r2lab", source)
        self.assertNotIn("synthran.network", source)
        self.assertNotIn("synthran.experiment", source)
        self.assertNotIn("synthran.research", source)


if __name__ == "__main__":
    unittest.main()
