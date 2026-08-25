from __future__ import annotations

import json
from pathlib import Path
import unittest

from synthran.cli import _parser
from synthran.dependencies import load_lock
from synthran.slices_controller import (
    DEFAULT_CONTROLLER_TIMEOUT_SECONDS,
    ControllerCommandResult,
    SlicesControllerError,
    verify_slices_controller,
)


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


class ControllerRunner:
    def __init__(
        self,
        *,
        ansible_version: str = "2.20.5",
        pos_version: str = "2.5.35",
        auth_ok: bool = True,
        project: str = "project-test",
        experiment: str = "experiment-test",
        post5g_payload: dict[str, str] | None = None,
    ) -> None:
        self.ansible_version = ansible_version
        self.pos_version = pos_version
        self.auth_ok = auth_ok
        self.project = project
        self.experiment = experiment
        self.post5g_payload = post5g_payload or {
            "subnet": "172.28.6.32/27",
            "lb": "172.28.3.130",
            "expiration_time": "2099-08-19 05:15:03Z",
        }
        self.calls: list[tuple[str, ...]] = []
        self.timeouts: list[int] = []

    def __call__(self, command, _timeout):
        argv = tuple(command)
        self.calls.append(argv)
        self.timeouts.append(_timeout)
        if argv == ("ansible-playbook", "--version"):
            return ControllerCommandResult(
                0, f"ansible-playbook [core {self.ansible_version}]\n"
            )
        if argv == ("ansible-galaxy", "--version"):
            return ControllerCommandResult(
                0, f"ansible-galaxy [core {self.ansible_version}]\n"
            )
        if argv == ("pos", "--version"):
            return ControllerCommandResult(0, f"pos {self.pos_version}\n")
        if argv == ("slices", "--version"):
            return ControllerCommandResult(0, "slices 1.4.0\n")
        if argv == ("slices", "auth", "show"):
            return ControllerCommandResult(
                0 if self.auth_ok else 2,
                "authenticated\n" if self.auth_ok else "",
            )
        if argv == ("slices", "project", "show"):
            return ControllerCommandResult(0, f"project: {self.project}\n")
        if argv[:3] == ("slices", "experiment", "show"):
            return ControllerCommandResult(0, f"experiment: {self.experiment}\n")
        if argv[:3] == ("post5g", "experiment", "prefix"):
            return ControllerCommandResult(0, json.dumps(self.post5g_payload))
        return ControllerCommandResult(2, "", "unsupported")


class SlicesControllerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.lock = load_lock(REPOSITORY_ROOT / "dependencies.lock.yml")

    def verify(self, runner: ControllerRunner | None = None, **overrides):
        arguments = {
            "lock": self.lock,
            "project": "project-test",
            "experiment": "experiment-test",
            "runner": runner or ControllerRunner(),
            "which": lambda _name: "/tool",
            "environment": {"CONDA_DEFAULT_ENV": "synthran"},
            "system_name": "Linux",
            "python_version": "3.12.13",
        }
        arguments.update(overrides)
        return verify_slices_controller(**arguments)

    def test_accepts_exact_locked_controller_and_slices_context(self) -> None:
        runner = ControllerRunner()
        report = self.verify(runner=runner)
        self.assertTrue(report.ready)
        self.assertEqual("2.20.5", report.ansible_version)
        self.assertEqual("2.5.35", report.pos_version)
        self.assertIsNotNone(report.post5g_network)
        assert report.post5g_network is not None
        self.assertEqual("172.28.6.32/27", report.post5g_network.subnet)
        self.assertEqual("172.28.3.130", report.post5g_network.load_balancer_ip)
        rendered = report.render()
        self.assertNotIn("project-test", rendered)
        self.assertNotIn("experiment-test", rendered)
        self.assertNotIn("172.28.6.32", rendered)
        self.assertIn("Post5G", rendered)
        self.assertIn(
            ("post5g", "experiment", "prefix", "experiment-test"),
            runner.calls,
        )
        self.assertEqual({DEFAULT_CONTROLLER_TIMEOUT_SECONDS}, set(runner.timeouts))

    def test_unified_doctor_uses_controller_timeout_default(self) -> None:
        args = _parser().parse_args(
            [
                "doctor",
                "--radio",
                "rfsim",
                "--slices-project",
                "project-test",
                "--slices-experiment",
                "experiment-test",
            ]
        )
        self.assertEqual(DEFAULT_CONTROLLER_TIMEOUT_SECONDS, args.timeout)

    def test_rejects_non_linux_controller(self) -> None:
        with self.assertRaisesRegex(SlicesControllerError, "Linux"):
            self.verify(system_name="Windows")

    def test_rejects_wrong_conda_environment(self) -> None:
        with self.assertRaisesRegex(SlicesControllerError, "synthran"):
            self.verify(environment={"CONDA_DEFAULT_ENV": "base"})

    def test_rejects_python_or_ansible_version_drift(self) -> None:
        with self.assertRaisesRegex(SlicesControllerError, "Python"):
            self.verify(python_version="3.12.10")
        with self.assertRaisesRegex(SlicesControllerError, "ansible-core"):
            self.verify(runner=ControllerRunner(ansible_version="2.20.4"))

    def test_rejects_pos_interface_drift(self) -> None:
        with self.assertRaisesRegex(SlicesControllerError, "POS"):
            self.verify(runner=ControllerRunner(pos_version="2.6.0"))

    def test_rejects_missing_authentication(self) -> None:
        with self.assertRaisesRegex(SlicesControllerError, "authentication"):
            self.verify(runner=ControllerRunner(auth_ok=False))

    def test_rejects_project_or_experiment_mismatch(self) -> None:
        with self.assertRaisesRegex(SlicesControllerError, "project"):
            self.verify(runner=ControllerRunner(project="another-project"))
        with self.assertRaisesRegex(SlicesControllerError, "experiment"):
            self.verify(runner=ControllerRunner(experiment="another-experiment"))
        with self.assertRaisesRegex(SlicesControllerError, "project"):
            self.verify(runner=ControllerRunner(project="project-test-extra"))
        with self.assertRaisesRegex(SlicesControllerError, "experiment"):
            self.verify(runner=ControllerRunner(experiment="experiment-test-extra"))

    def test_rejects_invalid_or_expired_post5g_network(self) -> None:
        with self.assertRaisesRegex(SlicesControllerError, "subnet"):
            self.verify(
                runner=ControllerRunner(
                    post5g_payload={
                        "subnet": "not-a-prefix",
                        "lb": "172.28.3.130",
                        "expiration_time": "2099-08-19 05:15:03Z",
                    }
                )
            )
        with self.assertRaisesRegex(SlicesControllerError, "expired"):
            self.verify(
                runner=ControllerRunner(
                    post5g_payload={
                        "subnet": "172.28.6.32/27",
                        "lb": "172.28.3.130",
                        "expiration_time": "2020-08-19 05:15:03Z",
                    }
                )
            )

    def test_timeout_names_the_slow_controller_probe(self) -> None:
        runner = ControllerRunner()

        def timeout_project(command, timeout):
            if tuple(command) == ("slices", "project", "show"):
                raise SlicesControllerError("a SLICES controller probe timed out")
            return runner(command, timeout)

        with self.assertRaisesRegex(
            SlicesControllerError,
            r"SLICES project probe failed: .*timed out",
        ):
            self.verify(runner=timeout_project)

    def test_missing_tool_fails_before_any_probe(self) -> None:
        runner = ControllerRunner()
        with self.assertRaisesRegex(SlicesControllerError, "missing required"):
            self.verify(
                runner=runner,
                which=lambda name: None if name == "post5g" else "/tool",
            )
        self.assertEqual([], runner.calls)


if __name__ == "__main__":
    unittest.main()
