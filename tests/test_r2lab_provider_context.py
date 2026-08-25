from __future__ import annotations

import argparse
from pathlib import Path
import unittest
from unittest.mock import Mock, patch

from synthran.backends.base import BackendError
from synthran.backends.r2lab import R2LabBackend, _ensure_slices_provider_context
from synthran.cli import _parser
from synthran.slices_controller import ControllerCommandResult
from synthran.workspace.model import WorkspaceError


class R2LabProviderContextTests(unittest.TestCase):
    def args(self, **overrides) -> argparse.Namespace:
        values = {
            "slices_project": "post5g-beta",
            "slices_experiment": None,
            "slices_duration": "4h",
            "run_id": "r2lab-live-001",
            "timeout": 30,
            "lock": Path("dependencies.lock.yml"),
        }
        values.update(overrides)
        return argparse.Namespace(**values)

    @patch("synthran.backends.r2lab.verify_slices_controller")
    @patch("synthran.backends.r2lab.load_lock")
    @patch("synthran.backends.r2lab.slices_runner")
    def test_missing_experiment_is_created_then_prefix_and_doctor_are_verified(
        self, runner, load_lock, verify
    ) -> None:
        calls: list[tuple[str, ...]] = []

        def execute(command, timeout):
            argv = tuple(command)
            calls.append(argv)
            if argv == ("slices", "experiment", "show", "r2lab-live-001"):
                return ControllerCommandResult(2, "", "not found")
            return ControllerCommandResult(0, "ok")

        runner.side_effect = execute
        load_lock.return_value = Mock()
        network = Mock()
        network.to_dict.return_value = {
            "subnet": "172.28.6.32/27",
            "load_balancer_ip": "172.28.3.130",
            "expiration_time_utc": "2099-01-01T00:00:00Z",
        }
        report = Mock(ready=True, post5g_network=network)
        verify.return_value = report

        project, experiment, created, returned = _ensure_slices_provider_context(self.args())

        self.assertEqual("post5g-beta", project)
        self.assertEqual("r2lab-live-001", experiment)
        self.assertTrue(created)
        self.assertIs(report, returned)
        self.assertEqual(
            [
                ("slices", "project", "use", "post5g-beta"),
                ("slices", "experiment", "show", "r2lab-live-001"),
                (
                    "slices",
                    "experiment",
                    "create",
                    "r2lab-live-001",
                    "--duration",
                    "4h",
                ),
                ("post5g", "experiment", "prefix", "r2lab-live-001"),
            ],
            calls,
        )
        verify.assert_called_once_with(
            lock=load_lock.return_value,
            project="post5g-beta",
            experiment="r2lab-live-001",
            timeout_seconds=60,
        )

    @patch("synthran.backends.r2lab.verify_slices_controller")
    @patch("synthran.backends.r2lab.load_lock")
    @patch("synthran.backends.r2lab.slices_runner")
    def test_existing_experiment_is_reused_without_create(
        self, runner, load_lock, verify
    ) -> None:
        runner.return_value = ControllerCommandResult(0, "ok")
        load_lock.return_value = Mock()
        verify.return_value = Mock(ready=True, post5g_network=Mock())

        _, experiment, created, _ = _ensure_slices_provider_context(
            self.args(slices_experiment="provider-existing")
        )

        self.assertEqual("provider-existing", experiment)
        self.assertFalse(created)
        self.assertNotIn(
            (
                "slices",
                "experiment",
                "create",
                "provider-existing",
                "--duration",
                "4h",
            ),
            [tuple(call.args[0]) for call in runner.call_args_list],
        )
        self.assertIn(
            ("post5g", "experiment", "prefix", "provider-existing"),
            [tuple(call.args[0]) for call in runner.call_args_list],
        )

    @patch("synthran.backends.r2lab.verify_slices_controller")
    @patch("synthran.backends.r2lab.load_lock")
    @patch("synthran.backends.r2lab.load_workspace")
    @patch("synthran.backends.r2lab.find_workspace_root")
    @patch("synthran.backends.r2lab.slices_runner")
    def test_project_defaults_from_persisted_workspace(
        self, runner, find_root, load_workspace, load_lock, verify
    ) -> None:
        runner.return_value = ControllerCommandResult(0, "ok")
        find_root.return_value = Path("/repo")
        load_workspace.return_value = Mock(project="post5g-beta")
        load_lock.return_value = Mock()
        verify.return_value = Mock(ready=True, post5g_network=Mock())

        project, _, _, _ = _ensure_slices_provider_context(
            self.args(slices_project=None)
        )

        self.assertEqual("post5g-beta", project)
        load_workspace.assert_called_once_with(Path("/repo"))
        self.assertEqual(
            ("slices", "project", "use", "post5g-beta"),
            tuple(runner.call_args_list[0].args[0]),
        )

    @patch("synthran.backends.r2lab.find_workspace_root")
    @patch("synthran.backends.r2lab.slices_runner")
    def test_missing_project_and_workspace_fail_before_provider_mutation(
        self, runner, find_root
    ) -> None:
        find_root.side_effect = WorkspaceError("no workspace")
        with self.assertRaisesRegex(BackendError, "workspace project"):
            _ensure_slices_provider_context(self.args(slices_project=None))
        runner.assert_not_called()

    @patch("synthran.backends.r2lab.slices_runner")
    def test_invalid_duration_fails_before_provider_mutation(self, runner) -> None:
        with self.assertRaisesRegex(BackendError, "duration"):
            _ensure_slices_provider_context(self.args(slices_duration="four hours"))
        runner.assert_not_called()

    def test_prepare_parser_exposes_provider_context_controls(self) -> None:
        args = _parser().parse_args(
            [
                "r2lab",
                "prepare",
                "--slice",
                "oulu_user",
                "--core-node",
                "sopnode-f2",
                "--ran-node",
                "sopnode-f3",
                "--radio",
                "n300",
                "--ue",
                "qfit07",
                "--run-id",
                "r2lab-live-001",
                "--slices-project",
                "post5g-beta",
            ]
        )
        self.assertEqual("post5g-beta", args.slices_project)
        self.assertIsNone(args.slices_experiment)
        self.assertEqual("4h", args.slices_duration)
        self.assertEqual(Path("dependencies.lock.yml"), args.lock)

    @patch("synthran.backends.r2lab.prepare_physical_resources")
    @patch("synthran.backends.r2lab._ensure_slices_provider_context")
    def test_prepare_resolves_provider_context_before_hardware(
        self, ensure_provider, prepare_hardware
    ) -> None:
        network = Mock()
        network.to_dict.return_value = {
            "subnet": "172.28.6.32/27",
            "load_balancer_ip": "172.28.3.130",
            "expiration_time_utc": "2099-01-01T00:00:00Z",
        }
        ensure_provider.return_value = (
            "post5g-beta",
            "r2lab-live-001",
            True,
            Mock(ready=True, post5g_network=network),
        )
        prepared = Mock()
        prepared.to_dict.return_value = {"status": "ready"}
        prepare_hardware.return_value = prepared
        args = argparse.Namespace(
            command="r2lab",
            r2lab_command="prepare",
            r2lab_slice="oulu_user",
            core_node="sopnode-f2",
            ran_node="sopnode-f3",
            radio="n300",
            ue="qfit07",
            run_id="r2lab-live-001",
            run_root=Path(".synthran/r2lab"),
            timeout=30,
            slices_project="post5g-beta",
            slices_experiment=None,
            slices_duration="4h",
            lock=Path("dependencies.lock.yml"),
        )

        with patch("builtins.print"):
            self.assertEqual(0, R2LabBackend().dispatch(args))

        ensure_provider.assert_called_once_with(args)
        prepare_hardware.assert_called_once()


if __name__ == "__main__":
    unittest.main()
