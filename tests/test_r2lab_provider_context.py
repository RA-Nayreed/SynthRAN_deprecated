from __future__ import annotations

import argparse
from pathlib import Path
import unittest
from unittest.mock import Mock, patch

from synthran.backends.base import BackendError
from synthran.provider import ensure_slices_provider_context
from synthran.slices_controller import ControllerCommandResult
from synthran.workspace.model import WorkspaceError


class ProviderContextTests(unittest.TestCase):
    def args(self, **overrides) -> argparse.Namespace:
        values = {
            "slices_project": "post5g-beta",
            "slices_experiment": None,
            "slices_duration": "4h",
            "run_id": "run-live-001",
            "timeout": 30,
            "lock": Path("dependencies.lock.yml"),
        }
        values.update(overrides)
        return argparse.Namespace(**values)

    @patch("synthran.provider.verify_slices_controller")
    @patch("synthran.provider.load_lock")
    @patch("synthran.provider.slices_runner")
    def test_missing_experiment_is_created_then_verified(
        self, runner, load_lock, verify
    ) -> None:
        calls: list[tuple[str, ...]] = []

        def execute(command, timeout):
            argv = tuple(command)
            calls.append(argv)
            if argv == ("slices", "experiment", "show", "run-live-001"):
                return ControllerCommandResult(2, "", "not found")
            return ControllerCommandResult(0, "ok")

        runner.side_effect = execute
        load_lock.return_value = Mock()
        network = Mock()
        network.to_dict.return_value = {"subnet": "198.51.100.0/24"}
        report = Mock(ready=True, post5g_network=network)
        verify.return_value = report

        project, experiment, created, returned = ensure_slices_provider_context(self.args())

        self.assertEqual("post5g-beta", project)
        self.assertEqual("run-live-001", experiment)
        self.assertTrue(created)
        self.assertIs(report, returned)
        self.assertEqual(
            [
                ("slices", "project", "use", "post5g-beta"),
                ("slices", "experiment", "show", "run-live-001"),
                ("slices", "experiment", "create", "run-live-001", "--duration", "4h"),
                ("post5g", "experiment", "prefix", "run-live-001"),
            ],
            calls,
        )

    @patch("synthran.provider.verify_slices_controller")
    @patch("synthran.provider.load_lock")
    @patch("synthran.provider.slices_runner")
    def test_existing_experiment_is_reused(self, runner, load_lock, verify) -> None:
        runner.return_value = ControllerCommandResult(0, "ok")
        load_lock.return_value = Mock()
        verify.return_value = Mock(ready=True, post5g_network=Mock())

        _, experiment, created, _ = ensure_slices_provider_context(
            self.args(slices_experiment="provider-existing")
        )
        self.assertEqual("provider-existing", experiment)
        self.assertFalse(created)
        calls = [tuple(call.args[0]) for call in runner.call_args_list]
        self.assertNotIn(
            ("slices", "experiment", "create", "provider-existing", "--duration", "4h"),
            calls,
        )
        self.assertIn(("post5g", "experiment", "prefix", "provider-existing"), calls)

    @patch("synthran.provider.verify_slices_controller")
    @patch("synthran.provider.load_lock")
    @patch("synthran.provider.load_workspace")
    @patch("synthran.provider.find_workspace_root")
    @patch("synthran.provider.slices_runner")
    def test_project_defaults_from_workspace(
        self, runner, find_root, load_workspace, load_lock, verify
    ) -> None:
        runner.return_value = ControllerCommandResult(0, "ok")
        find_root.return_value = Path("/repo")
        load_workspace.return_value = Mock(project="post5g-beta")
        load_lock.return_value = Mock()
        verify.return_value = Mock(ready=True, post5g_network=Mock())

        project, _, _, _ = ensure_slices_provider_context(self.args(slices_project=None))
        self.assertEqual("post5g-beta", project)
        load_workspace.assert_called_once_with(Path("/repo"))

    @patch("synthran.provider.find_workspace_root")
    @patch("synthran.provider.slices_runner")
    def test_missing_project_fails_before_provider_mutation(self, runner, find_root) -> None:
        find_root.side_effect = WorkspaceError("no workspace")
        with self.assertRaisesRegex(BackendError, "workspace project"):
            ensure_slices_provider_context(self.args(slices_project=None))
        runner.assert_not_called()

    @patch("synthran.provider.slices_runner")
    def test_invalid_duration_fails_before_provider_mutation(self, runner) -> None:
        with self.assertRaisesRegex(BackendError, "duration"):
            ensure_slices_provider_context(self.args(slices_duration="four hours"))
        runner.assert_not_called()


if __name__ == "__main__":
    unittest.main()
