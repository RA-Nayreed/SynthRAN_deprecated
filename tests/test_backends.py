from __future__ import annotations

import argparse
import io
import unittest
from unittest.mock import patch

from synthran.backends.base import BackendContract, BackendError, LifecycleStage
from synthran.cli import main as cli_main
from synthran.operator import dispatch


class BackendContractTests(unittest.TestCase):
    def test_backend_capability_must_follow_lifecycle_order(self) -> None:
        with self.assertRaises(ValueError):
            BackendContract(
                name="rfsim",
                radio_mode="virtual",
                implemented_stages=(LifecycleStage.ACCESS, LifecycleStage.GNB),
            )


class OperatorBoundaryTests(unittest.TestCase):
    @patch("synthran.operator.RunCommandAdapter.dispatch", return_value=7)
    def test_run_dispatches_through_single_run_adapter(self, run) -> None:
        args = argparse.Namespace(command="run")
        self.assertEqual(7, dispatch(args))
        run.assert_called_once_with(args)

    def test_cli_uses_one_prefixed_run_error_surface(self) -> None:
        stderr = io.StringIO()
        with (
            patch("synthran.cli.dispatch", side_effect=BackendError("boundary failure")),
            patch("sys.stderr", stderr),
        ):
            self.assertEqual(
                2,
                cli_main(
                    [
                        "run",
                        "--radio",
                        "rfsim",
                        "--run-id",
                        "boundary-test-001",
                        "--core-node",
                        "sopnode-f2",
                        "--ran-node",
                        "sopnode-f3",
                    ]
                ),
            )
        self.assertEqual("[synthran] ✗ run: boundary failure\n", stderr.getvalue())


if __name__ == "__main__":
    unittest.main()
