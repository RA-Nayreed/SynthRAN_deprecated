from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from synthran.live_preflight import CommandResult
from synthran.network.r2lab import (
    R2LabResourceError,
    R2LabSelection,
    build_plan,
    execute_prepare,
    execute_release,
    gateway_command,
    run_doctor,
)


class FakeRunner:
    def __init__(self, *, lease_ok: bool = True, ping_failures: int = 0) -> None:
        self.lease_ok = lease_ok
        self.ping_failures = ping_failures
        self.commands: list[tuple[str, ...]] = []
        self.ping_attempts = 0
        self.power: dict[str, str] = {}

    @staticmethod
    def remote(command: tuple[str, ...]) -> tuple[str, ...]:
        split = command.index("--")
        return command[split + 2 :]

    @staticmethod
    def _qfit_node(qfit: str) -> int:
        return int(qfit.removeprefix("qfit"))

    def __call__(self, command, timeout_seconds: int) -> CommandResult:
        value = tuple(command)
        self.commands.append(value)
        remote = self.remote(value)

        if remote == ("true",):
            return CommandResult(0, "", "")
        if remote == ("rhubarbe", "leases", "--check"):
            return CommandResult(0 if self.lease_ok else 1, "", "")

        if remote[:3] == ("rhubarbe", "pdu", "on") and len(remote) == 4:
            resource = remote[3]
            self.power[resource] = "on"
            return CommandResult(
                0,
                f"pdu2 chain-0@outlet-1 ({resource}): ON (28W)\n",
                "",
            )
        if remote[:3] == ("rhubarbe", "pdu", "off") and len(remote) == 4:
            resource = remote[3]
            self.power[resource] = "off"
            # Live smoke 002 proved that a successful OFF may return rc=1.
            return CommandResult(
                1,
                f"pdu2 chain-0@outlet-1 ({resource}): OFF\n",
                "",
            )
        if remote[:3] == ("rhubarbe", "pdu", "status") and len(remote) == 4:
            resource = remote[3]
            state = self.power.get(resource)
            if state == "on":
                return CommandResult(
                    0,
                    f"pdu2 chain-0@outlet-1 ({resource}): ON (28W)\n",
                    "",
                )
            if state == "off":
                return CommandResult(
                    0,
                    f"pdu2 chain-0@outlet-1 ({resource}): OFF\n",
                    "",
                )
            return CommandResult(0, "", "")

        if remote[:2] == ("qfit", "on") and len(remote) == 3:
            qfit = remote[2]
            self.power[qfit] = "on"
            node = self._qfit_node(qfit)
            return CommandResult(0, f"reboot{node:02d}:ok\n", "")
        if remote[:2] == ("qfit", "off") and len(remote) == 3:
            qfit = remote[2]
            self.power[qfit] = "off"
            node = self._qfit_node(qfit)
            return CommandResult(0, f"reboot{node:02d}:ok\n", "")
        if remote[:2] == ("rhubarbe", "status") and len(remote) == 3:
            node = int(remote[2])
            qfit = f"qfit{node:02d}"
            state = self.power.get(qfit)
            if state in {"on", "off"}:
                return CommandResult(0, f"reboot{node:02d}:{state}\n", "")
            return CommandResult(0, "", "")

        if remote[:1] == ("ping",):
            self.ping_attempts += 1
            if self.ping_attempts <= self.ping_failures:
                return CommandResult(1, "", "")
            return CommandResult(0, "", "")

        return CommandResult(0, "", "")

    @property
    def remote_commands(self) -> list[tuple[str, ...]]:
        return [self.remote(command) for command in self.commands]


class R2LabTests(unittest.TestCase):
    def test_selection_accepts_reviewed_radios_and_modes(self) -> None:
        mbim = R2LabSelection.build(
            slice_name="oulu_user", radio="n300", ue="qhat01"
        )
        qmi = R2LabSelection.build(
            slice_name="oulu_user", radio="n320", ue="qhat20"
        )
        qfit = R2LabSelection.build(
            slice_name="oulu_user", radio="n320", ue="qfit07"
        )
        self.assertEqual(("qhat", "mbim"), (mbim.ue_kind, mbim.ue_mode))
        self.assertEqual(("qhat", "qmi"), (qmi.ue_kind, qmi.ue_mode))
        self.assertEqual(("qfit", "mbim"), (qfit.ue_kind, qfit.ue_mode))

    def test_selection_rejects_unreviewed_resources_and_unsafe_slice(self) -> None:
        with self.assertRaises(R2LabResourceError):
            R2LabSelection.build(
                slice_name="unsafe user", radio="n300", ue="qhat01"
            )
        with self.assertRaises(R2LabResourceError):
            R2LabSelection.build(
                slice_name="oulu_user", radio="benetel1", ue="qhat01"
            )
        with self.assertRaises(R2LabResourceError):
            R2LabSelection.build(
                slice_name="oulu_user", radio="n300", ue="phone1"
            )

    def test_gateway_command_uses_batch_ssh_and_strict_host_keys(self) -> None:
        command = gateway_command("oulu_user", "rhubarbe", "leases", "--check")
        self.assertEqual("ssh", command[0])
        self.assertIn("BatchMode=yes", command)
        self.assertIn("StrictHostKeyChecking=yes", command)
        self.assertIn("oulu_user@faraday.inria.fr", command)
        self.assertNotIn("password", " ".join(command).lower())

    def test_plan_redacts_slice_and_requires_state_verification(self) -> None:
        selection = R2LabSelection.build(
            slice_name="private_slice", radio="n300", ue="qhat01"
        )
        plan = build_plan(run_id="r2lab-test-01", selection=selection)
        rendered = plan.render(as_json=True)
        self.assertNotIn("private_slice", rendered)
        self.assertNotIn("all-off", rendered)
        self.assertNotIn("rhubarbe bye", rendered)
        payload = json.loads(rendered)
        self.assertEqual("reuse-active", payload["lease_action"])
        self.assertFalse(payload["safety"]["password_storage"])
        self.assertFalse(payload["safety"]["global_power_off"])
        self.assertFalse(payload["safety"]["mutation_returncode_is_state_truth"])
        self.assertTrue(payload["safety"]["claim_release_requires_proven_clean_state"])
        self.assertIn("pdu status n300", "\n".join(payload["commands"]))

    def test_doctor_is_read_only_and_requires_active_lease(self) -> None:
        selection = R2LabSelection.build(
            slice_name="oulu_user", radio="n300", ue="qhat01"
        )
        runner = FakeRunner()
        report = run_doctor(selection=selection, runner=runner)
        self.assertTrue(report.ready)
        self.assertEqual(
            [("true",), ("rhubarbe", "leases", "--check")],
            runner.remote_commands,
        )

        denied = FakeRunner(lease_ok=False)
        report = run_doctor(selection=selection, runner=denied)
        self.assertFalse(report.ready)

    def test_doctor_turns_transport_errors_into_not_ready(self) -> None:
        selection = R2LabSelection.build(
            slice_name="oulu_user", radio="n300", ue="qhat01"
        )

        def failing_runner(command, timeout_seconds: int) -> CommandResult:
            raise R2LabResourceError("transport failed")

        report = run_doctor(selection=selection, runner=failing_runner)
        self.assertFalse(report.ready)
        self.assertEqual("gateway", report.checks[-1].name)

    def test_prepare_checks_lease_and_proves_each_power_transition(self) -> None:
        selection = R2LabSelection.build(
            slice_name="oulu_user", radio="n300", ue="qhat01"
        )
        plan = build_plan(run_id="r2lab-test-02", selection=selection)
        runner = FakeRunner(ping_failures=1)
        waits: list[float] = []
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "r2lab"
            result = execute_prepare(
                plan=plan,
                run_root=root,
                runner=runner,
                sleeper=waits.append,
                power_settle_seconds=20,
                reachability_attempts=3,
                reachability_delay_seconds=10,
            )
            self.assertEqual("ready", result.status)
            self.assertTrue((root / "active.json").is_file())
            manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
            self.assertEqual("ready", manifest["status"])
            self.assertEqual("held", manifest["resource_claim"])
            self.assertNotIn("oulu_user", result.manifest_path.read_text(encoding="utf-8"))
            self.assertEqual([20, 10], waits)

        expected = [
            ("rhubarbe", "leases", "--check"),
            ("rhubarbe", "leases", "--check"),
            ("rhubarbe", "pdu", "on", "n300"),
            ("rhubarbe", "pdu", "status", "n300"),
            ("rhubarbe", "leases", "--check"),
            ("rhubarbe", "pdu", "off", "qhat01"),
            ("rhubarbe", "pdu", "status", "qhat01"),
            ("rhubarbe", "leases", "--check"),
            ("rhubarbe", "pdu", "on", "qhat01"),
            ("rhubarbe", "pdu", "status", "qhat01"),
            ("ping", "-c", "1", "-W", "1", "qhat01"),
            ("ping", "-c", "1", "-W", "1", "qhat01"),
            ("rhubarbe", "leases", "--check"),
        ]
        self.assertEqual(expected, runner.remote_commands)
        joined = "\n".join(" ".join(command) for command in runner.remote_commands)
        self.assertNotIn("all-off", joined)
        self.assertNotIn("rhubarbe bye", joined)

    def test_prepare_fails_before_mutation_without_a_lease(self) -> None:
        selection = R2LabSelection.build(
            slice_name="oulu_user", radio="n300", ue="qhat01"
        )
        plan = build_plan(run_id="r2lab-test-03", selection=selection)
        runner = FakeRunner(lease_ok=False)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "r2lab"
            with self.assertRaises(R2LabResourceError):
                execute_prepare(
                    plan=plan,
                    run_root=root,
                    runner=runner,
                    sleeper=lambda _: None,
                )
            self.assertFalse((root / "active.json").exists())
            manifest = json.loads(
                (root / plan.run_id / "manifest.json").read_text(encoding="utf-8")
            )
            self.assertEqual("failed", manifest["status"])
            self.assertEqual("lease-check", manifest["failure_stage"])
        self.assertEqual(
            [("rhubarbe", "leases", "--check")], runner.remote_commands
        )

    def test_prepare_records_transport_failure_without_leaking_command_output(self) -> None:
        selection = R2LabSelection.build(
            slice_name="oulu_user", radio="n300", ue="qhat01"
        )
        plan = build_plan(run_id="r2lab-test-transport", selection=selection)

        def failing_runner(command, timeout_seconds: int) -> CommandResult:
            raise R2LabResourceError("private remote details")

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "r2lab"
            with self.assertRaises(R2LabResourceError):
                execute_prepare(
                    plan=plan,
                    run_root=root,
                    runner=failing_runner,
                    sleeper=lambda _: None,
                )
            log = (root / plan.run_id / "r2lab.log").read_text(encoding="utf-8")
            self.assertIn("gateway command could not complete", log)
            self.assertNotIn("private remote details", log)

    def test_qfit_prepare_proves_selected_qfit_state(self) -> None:
        selection = R2LabSelection.build(
            slice_name="oulu_user", radio="n320", ue="qfit07"
        )
        plan = build_plan(run_id="r2lab-test-04", selection=selection)
        runner = FakeRunner()
        with tempfile.TemporaryDirectory() as directory:
            execute_prepare(
                plan=plan,
                run_root=Path(directory) / "r2lab",
                runner=runner,
                sleeper=lambda _: None,
                reachability_attempts=1,
            )
        remote = runner.remote_commands
        self.assertIn(("qfit", "off", "qfit07"), remote)
        self.assertIn(("qfit", "on", "qfit07"), remote)
        self.assertGreaterEqual(remote.count(("rhubarbe", "status", "7")), 2)
        self.assertNotIn(("rhubarbe", "pdu", "off", "qfit07"), remote)

    def test_release_requires_exact_claim_and_proves_both_resources_off(self) -> None:
        selection = R2LabSelection.build(
            slice_name="oulu_user", radio="n300", ue="qhat01"
        )
        plan = build_plan(run_id="r2lab-test-05", selection=selection)
        runner = FakeRunner()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "r2lab"
            execute_prepare(
                plan=plan,
                run_root=root,
                runner=runner,
                sleeper=lambda _: None,
                reachability_attempts=1,
            )
            runner.commands.clear()
            result = execute_release(
                run_id=plan.run_id,
                slice_name="oulu_user",
                run_root=root,
                runner=runner,
            )
            self.assertEqual("released", result.status)
            self.assertFalse((root / "active.json").exists())
            manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
            self.assertEqual("released", manifest["status"])
            self.assertEqual("released", manifest["resource_claim"])
            self.assertTrue(manifest["cleanup"]["claim_releasable"])

        self.assertEqual(
            [
                ("rhubarbe", "leases", "--check"),
                ("rhubarbe", "pdu", "off", "qhat01"),
                ("rhubarbe", "pdu", "status", "qhat01"),
                ("rhubarbe", "leases", "--check"),
                ("rhubarbe", "pdu", "off", "n300"),
                ("rhubarbe", "pdu", "status", "n300"),
            ],
            runner.remote_commands,
        )
        joined = "\n".join(" ".join(command) for command in runner.remote_commands)
        self.assertNotIn("all-off", joined)
        self.assertNotIn("rhubarbe bye", joined)

    def test_release_refuses_wrong_slice_or_missing_claim(self) -> None:
        selection = R2LabSelection.build(
            slice_name="oulu_user", radio="n300", ue="qhat01"
        )
        plan = build_plan(run_id="r2lab-test-06", selection=selection)
        runner = FakeRunner()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "r2lab"
            execute_prepare(
                plan=plan,
                run_root=root,
                runner=runner,
                sleeper=lambda _: None,
                reachability_attempts=1,
            )
            with self.assertRaises(R2LabResourceError):
                execute_release(
                    run_id=plan.run_id,
                    slice_name="other_user",
                    run_root=root,
                    runner=runner,
                )
            (root / "active.json").unlink()
            with self.assertRaises(R2LabResourceError):
                execute_release(
                    run_id=plan.run_id,
                    slice_name="oulu_user",
                    run_root=root,
                    runner=runner,
                )

    def test_rc1_off_is_accepted_when_exact_status_proves_off(self) -> None:
        selection = R2LabSelection.build(
            slice_name="oulu_user", radio="n300", ue="qhat01"
        )
        plan = build_plan(run_id="r2lab-test-live-rc1", selection=selection)
        runner = FakeRunner()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "r2lab"
            execute_prepare(
                plan=plan,
                run_root=root,
                runner=runner,
                sleeper=lambda _: None,
                reachability_attempts=1,
            )
            result = execute_release(
                run_id=plan.run_id,
                slice_name="oulu_user",
                run_root=root,
                runner=runner,
            )
            self.assertEqual("released", result.status)
            self.assertFalse((root / "active.json").exists())

    def test_wrong_radio_status_retains_claim(self) -> None:
        selection = R2LabSelection.build(
            slice_name="oulu_user", radio="n300", ue="qhat01"
        )
        plan = build_plan(run_id="r2lab-test-07", selection=selection)
        runner = FakeRunner()

        class RadioStatusFailure(FakeRunner):
            def __call__(self, command, timeout_seconds: int) -> CommandResult:
                remote = self.remote(tuple(command))
                if remote == ("rhubarbe", "pdu", "status", "n300"):
                    self.commands.append(tuple(command))
                    return CommandResult(
                        0,
                        "pdu2 chain-0@outlet-1 (n300): ON (28W)\n",
                        "",
                    )
                return super().__call__(command, timeout_seconds)

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "r2lab"
            execute_prepare(
                plan=plan,
                run_root=root,
                runner=runner,
                sleeper=lambda _: None,
                reachability_attempts=1,
            )
            failing = RadioStatusFailure()
            with self.assertRaises(R2LabResourceError):
                execute_release(
                    run_id=plan.run_id,
                    slice_name="oulu_user",
                    run_root=root,
                    runner=failing,
                )
            self.assertTrue((root / "active.json").is_file())
            manifest = json.loads(
                (root / plan.run_id / "manifest.json").read_text(encoding="utf-8")
            )
            self.assertEqual("release-failed", manifest["status"])
            self.assertEqual("held", manifest["resource_claim"])
            self.assertEqual(["n300"], manifest["cleanup"]["unresolved_resources"])

    def test_unresolved_ue_cleanup_still_attempts_exact_radio_cleanup(self) -> None:
        selection = R2LabSelection.build(
            slice_name="oulu_user", radio="n300", ue="qhat01"
        )
        plan = build_plan(run_id="r2lab-test-ue-unknown", selection=selection)
        setup = FakeRunner()

        class UnknownUeStatus(FakeRunner):
            def __call__(self, command, timeout_seconds: int) -> CommandResult:
                remote = self.remote(tuple(command))
                if remote == ("rhubarbe", "pdu", "status", "qhat01"):
                    self.commands.append(tuple(command))
                    return CommandResult(0, "", "")
                return super().__call__(command, timeout_seconds)

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "r2lab"
            execute_prepare(
                plan=plan,
                run_root=root,
                runner=setup,
                sleeper=lambda _: None,
                reachability_attempts=1,
            )
            failing = UnknownUeStatus()
            with self.assertRaises(R2LabResourceError):
                execute_release(
                    run_id=plan.run_id,
                    slice_name="oulu_user",
                    run_root=root,
                    runner=failing,
                )
            self.assertIn(("rhubarbe", "pdu", "off", "n300"), failing.remote_commands)
            self.assertIn(("rhubarbe", "pdu", "status", "n300"), failing.remote_commands)
            self.assertTrue((root / "active.json").exists())
            manifest = json.loads(
                (root / plan.run_id / "manifest.json").read_text(encoding="utf-8")
            )
            self.assertEqual(["qhat01"], manifest["cleanup"]["unresolved_resources"])

    def test_run_id_is_never_reused(self) -> None:
        selection = R2LabSelection.build(
            slice_name="oulu_user", radio="n300", ue="qhat01"
        )
        plan = build_plan(run_id="r2lab-test-08", selection=selection)
        runner = FakeRunner()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "r2lab"
            execute_prepare(
                plan=plan,
                run_root=root,
                runner=runner,
                sleeper=lambda _: None,
                reachability_attempts=1,
            )
            with self.assertRaises(R2LabResourceError):
                execute_prepare(
                    plan=plan,
                    run_root=root,
                    runner=runner,
                    sleeper=lambda _: None,
                    reachability_attempts=1,
                )


if __name__ == "__main__":
    unittest.main()
