from __future__ import annotations

import io
import unittest
from unittest.mock import patch

from synthran.experiment import ExperimentError
from synthran.launcher import main


class LauncherTests(unittest.TestCase):
    def test_launcher_delegates_every_command_to_cli(self) -> None:
        arguments = ["run", "--network-run-id", "network-01"]
        with patch("synthran.cli.main", return_value=0) as cli_main:
            result = main(arguments)

        self.assertEqual(result, 0)
        cli_main.assert_called_once_with(arguments)

    def test_experiment_error_is_rendered_without_traceback(self) -> None:
        stderr = io.StringIO()
        with (
            patch("synthran.cli.main", side_effect=ExperimentError("path proof failed")),
            patch("sys.stderr", stderr),
        ):
            result = main(["run"])

        self.assertEqual(result, 2)
        self.assertEqual(stderr.getvalue(), "error: path proof failed\n")


if __name__ == "__main__":
    unittest.main()
