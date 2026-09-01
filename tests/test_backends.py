from __future__ import annotations

import io
import unittest
from unittest.mock import patch

from synthran.cli import main as cli_main
from synthran.errors import SynthRANError


class OrchestrationBoundaryTests(unittest.TestCase):
    def test_cli_uses_one_prefixed_run_error_surface(self) -> None:
        stderr = io.StringIO()
        with (
            patch("synthran.cli.execute_run", side_effect=SynthRANError("boundary failure")),
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
                        "--slices-project",
                        "project-test",
                    ]
                ),
            )
        self.assertEqual("[synthran] ✗ run: boundary failure\n", stderr.getvalue())

    @patch("synthran.cli.execute_run", return_value={"accepted": True})
    def test_cli_dispatches_full_run_directly_to_lifecycle(self, execute) -> None:
        self.assertEqual(
            0,
            cli_main(
                [
                    "run",
                    "--radio",
                    "rfsim",
                    "--run-id",
                    "boundary-test-002",
                    "--core-node",
                    "sopnode-f2",
                    "--ran-node",
                    "sopnode-f3",
                    "--slices-project",
                    "project-test",
                ]
            ),
        )
        execute.assert_called_once()
        args = execute.call_args.args[0]
        self.assertEqual("boundary-test-002", args.run_id)
        self.assertEqual("project-test", args.slices_project)


if __name__ == "__main__":
    unittest.main()
