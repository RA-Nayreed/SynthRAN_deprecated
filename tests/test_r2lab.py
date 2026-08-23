from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from synthran.live_preflight import CommandResult
from synthran.r2lab.controller import (
    QFIT_IMAGE,
    QFIT_INITIALIZER,
    R2LabResourceError,
    R2LabSelection,
    build_plan,
    execute_prepare,
    execute_release,
    gateway_command,
    qfit_gateway_command,
    run_doctor,
)


class FakeRunner:
    def __init__(
        self,
        *,
        lease_ok: bool = True,
        ping_failures: int = 0,
        qfit_ssh_ready: bool = True,
        qfit_initial_state: str | None = "off",
        qfit_image_load_ok: bool = True,
        qfit_state_after_load: str = "on",
        qfit_usb_state: str | None = "off",
        qfit_mbim_failures: int = 0,
    ) -> None:
        self.lease_ok = lease_ok
        self.ping_failures = ping_failures
        self.qfit_ssh_ready = qfit_ssh_ready
        self.qfit_initial_state = qfit_initial_state
        self.qfit_image_load_ok = qfit_image_load_ok
        self.qfit_state_after_load = qfit_state_after_load
        self.qfit_usb_state = qfit_usb_state
        self.qfit_mbim_failures = qfit_mbim_failures
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
            # Provider state remains authoritative when the mutation returns rc=1.
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
            state = self.power.get(qfit, self.qfit_initial_state)
            if state in {"on", "off"}:
                return CommandResult(0, f"reboot{node:02d}:{state}\n", "")
            return CommandResult(0, "", "")
        if (
            remote[:4] == ("rhubarbe", "load", "-i", QFIT_IMAGE)
            and len(remote) == 5
        ):
            if not self.qfit_image_load_ok:
                return CommandResult(1, "", "")
            node = int(remote[4])
            self.power[f"qfit{node:02d}"] = self.qfit_state_after_load
            return CommandResult(0, "", "")
        if remote[:2] == ("rhubarbe", "wait") and len(remote) == 3:
            return CommandResult(0 if self.qfit_ssh_ready else 1, "", "")

        if remote == ("curl", "-fsS", "http://reboot07/usrpstatus"):
            output = f"usrp{self.qfit_usb_state}\n" if self.qfit_usb_state else ""
            return CommandResult(0, output, "")
        if remote == ("curl", "-fsS", "http://reboot07/usrpon"):
            self.qfit_usb_state = "on"
            return CommandResult(0, "ok\n", "")
        if remote == ("curl", "-fsS", "http://reboot07/usrpoff"):
            self.qfit_usb_state = "off"
            return CommandResult(0, "ok\n", "")

        if remote[:1] == ("ssh",):
            if remote[-3:] == ("test", "-c", "/dev/cdc-wdm0"):
                if self.qfit_mbim_failures:
                    self.qfit_mbim_failures -= 1
                    return CommandResult(1, "", "")
                return CommandResult(
                    0 if self.qfit_usb_state == "on" else 1,
                    "",
                    "",
                )
            if remote[-3:] == ("test", "-c", "/dev/ttyUSB2"):
                return CommandResult(
                    0 if self.qfit_usb_state == "on" else 1,
                    "",
                    "",
                )
            if remote[-5:] == ("ip", "link", "show", "dev", "wwan0"):
                return CommandResult(
                    0 if self.qfit_usb_state == "on" else 1,
                    "",
                    "",
                )

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
        slice_name = "oulu_user"
        command = gateway_command(slice_name, "rhubarbe", "leases", "--check")
        self.assertEqual("ssh", command[0])
        self.assertIn("BatchMode=yes", command)
        self.assertIn("StrictHostKeyChecking=yes", command)
        self.assertIn(f"{slice_name}@faraday.inria.fr", command)
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

    def test_qfit_plan_reuses_on_state_without_power_off(self) -> None:
        selection = R2LabSelection.build(
            slice_name="oulu_user", radio="n300", ue="qfit07"
        )
        commands = "\n".join(
            build_plan(
                run_id="r2lab-test-qfit-plan", selection=selection
            ).to_dict()["commands"]
        )

        self.assertIn("rhubarbe status <qfit-node>", commands)
        self.assertIn(f"rhubarbe load -i {QFIT_IMAGE} <qfit-node>", commands)
        self.assertIn("rhubarbe wait <qfit-node>", commands)
        self.assertIn("http://reboot07/usrpstatus", commands)
        self.assertIn("http://reboot07/usrpon", commands)
        self.assertIn("test -c /dev/cdc-wdm0", commands)
        self.assertIn(QFIT_INITIALIZER, commands)
        self.assertNotIn("qfit off qfit07", commands)

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
        self.assertNotIn(("qfit", "off", "qfit07"), remote)
        self.assertNotIn(("qfit", "on", "qfit07"), remote)
        self.assertIn(("rhubarbe", "load", "-i", QFIT_IMAGE, "7"), remote)
        self.assertGreaterEqual(remote.count(("rhubarbe", "status", "7")), 2)
        self.assertNotIn(("rhubarbe", "pdu", "off", "qfit07"), remote)
        self.assertIn(("rhubarbe", "wait", "7"), remote)
        self.assertNotIn(("ping", "-c", "1", "-W", "1", "fit07"), remote)
        self.assertNotIn(("ping", "-c", "1", "-W", "1", "qfit07"), remote)

        image_readiness = [
            (index, command)
            for index, command in enumerate(remote)
            if command[:1] == ("ssh",)
            and ("test", "-x", QFIT_INITIALIZER) == command[-3:]
        ]
        initializer = [
            (index, command)
            for index, command in enumerate(remote)
            if command[:1] == ("ssh",) and command[-1:] == (QFIT_INITIALIZER,)
            and command[-3:] != ("test", "-x", QFIT_INITIALIZER)
        ]
        usb_on = [
            (index, command)
            for index, command in enumerate(remote)
            if command == ("curl", "-fsS", "http://reboot07/usrpon")
        ]
        mbim_readiness = [
            (index, command)
            for index, command in enumerate(remote)
            if command[:1] == ("ssh",)
            and command[-3:] == ("test", "-c", "/dev/cdc-wdm0")
        ]
        self.assertEqual(1, len(image_readiness))
        self.assertEqual(1, len(initializer))
        self.assertEqual(1, len(usb_on))
        self.assertEqual(2, len(mbim_readiness))
        load_index = remote.index(("rhubarbe", "load", "-i", QFIT_IMAGE, "7"))
        post_load_status_index = remote.index(
            ("rhubarbe", "status", "7"),
            load_index,
        )
        wait_index = remote.index(("rhubarbe", "wait", "7"))
        usb_status_index = remote.index(
            ("curl", "-fsS", "http://reboot07/usrpstatus")
        )
        self.assertLess(load_index, post_load_status_index)
        self.assertLess(post_load_status_index, wait_index)
        self.assertLess(wait_index, usb_status_index)
        self.assertLess(usb_status_index, usb_on[0][0])
        self.assertLess(usb_on[0][0], mbim_readiness[0][0])
        self.assertLess(mbim_readiness[0][0], image_readiness[0][0])
        self.assertLess(image_readiness[0][0], initializer[0][0])
        self.assertLess(initializer[0][0], mbim_readiness[1][0])
        self.assertFalse(
            any("--set-radio-state=on" in command for command in remote)
        )
        for _, command in (initializer[0],):
            rendered = " ".join(command)
            self.assertIn("root@fit07", rendered)
            self.assertNotIn("root@qfit07", rendered)
            self.assertIn(
                f"UserKnownHostsFile=/home/{selection.slice_name}/.ssh/known_hosts",
                rendered,
            )
            self.assertIn("GlobalKnownHostsFile=/dev/null", rendered)

    def test_qfit_prepare_reuses_provisioned_ready_node(self) -> None:
        selection = R2LabSelection.build(
            slice_name="oulu_user", radio="n300", ue="qfit07"
        )
        plan = build_plan(run_id="r2lab-test-qfit-reuse", selection=selection)
        runner = FakeRunner(qfit_initial_state="on", qfit_usb_state="on")

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "r2lab"
            execute_prepare(
                plan=plan,
                run_root=root,
                runner=runner,
                sleeper=lambda _: None,
            )
            log = (root / plan.run_id / "r2lab.log").read_text(encoding="utf-8")

        remote = runner.remote_commands
        self.assertNotIn(("qfit", "off", "qfit07"), remote)
        self.assertNotIn(("qfit", "on", "qfit07"), remote)
        self.assertNotIn(("rhubarbe", "load", "-i", QFIT_IMAGE, "7"), remote)
        self.assertIn(("rhubarbe", "status", "7"), remote)
        self.assertIn(("rhubarbe", "wait", "7"), remote)
        self.assertNotIn(
            ("curl", "-fsS", "http://reboot07/usrpon"),
            remote,
        )
        self.assertIn("ue-power-reuse: OK - state=on", log)
        self.assertIn("qfit-usb-power-reuse: OK - state=on", log)

    def test_qfit_prepare_stops_on_unknown_initial_power_state(self) -> None:
        selection = R2LabSelection.build(
            slice_name="oulu_user", radio="n300", ue="qfit07"
        )
        plan = build_plan(run_id="r2lab-test-qfit-unknown", selection=selection)
        runner = FakeRunner(qfit_initial_state=None)

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "r2lab"
            with self.assertRaises(R2LabResourceError):
                execute_prepare(
                    plan=plan,
                    run_root=root,
                    runner=runner,
                    sleeper=lambda _: None,
                )
            manifest = json.loads(
                (root / plan.run_id / "manifest.json").read_text(encoding="utf-8")
            )

        self.assertEqual("qfit-power-state", manifest["failure_stage"])
        self.assertNotIn(("qfit", "off", "qfit07"), runner.remote_commands)
        self.assertNotIn(("qfit", "on", "qfit07"), runner.remote_commands)
        self.assertNotIn(
            ("rhubarbe", "load", "-i", QFIT_IMAGE, "7"),
            runner.remote_commands,
        )

    def test_qfit_prepare_stops_when_image_load_fails(self) -> None:
        selection = R2LabSelection.build(
            slice_name="oulu_user", radio="n300", ue="qfit07"
        )
        plan = build_plan(run_id="r2lab-test-qfit-image-load", selection=selection)
        runner = FakeRunner(qfit_image_load_ok=False)

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "r2lab"
            with self.assertRaises(R2LabResourceError):
                execute_prepare(
                    plan=plan,
                    run_root=root,
                    runner=runner,
                    sleeper=lambda _: None,
                )
            manifest = json.loads(
                (root / plan.run_id / "manifest.json").read_text(encoding="utf-8")
            )

        self.assertEqual("qfit-image-load", manifest["failure_stage"])
        self.assertEqual("held", manifest["resource_claim"])
        self.assertIn(
            ("rhubarbe", "load", "-i", QFIT_IMAGE, "7"),
            runner.remote_commands,
        )
        self.assertNotIn(("rhubarbe", "wait", "7"), runner.remote_commands)

    def test_qfit_prepare_requires_on_state_after_image_load(self) -> None:
        selection = R2LabSelection.build(
            slice_name="oulu_user", radio="n300", ue="qfit07"
        )
        plan = build_plan(run_id="r2lab-test-qfit-image-state", selection=selection)
        runner = FakeRunner(qfit_state_after_load="off")

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "r2lab"
            with self.assertRaises(R2LabResourceError):
                execute_prepare(
                    plan=plan,
                    run_root=root,
                    runner=runner,
                    sleeper=lambda _: None,
                )
            manifest = json.loads(
                (root / plan.run_id / "manifest.json").read_text(encoding="utf-8")
            )

        self.assertEqual(
            "qfit-power-state-after-image-load",
            manifest["failure_stage"],
        )
        self.assertEqual("held", manifest["resource_claim"])
        self.assertNotIn(("rhubarbe", "wait", "7"), runner.remote_commands)

    def test_qfit_prepare_stops_before_initialization_without_ssh(self) -> None:
        selection = R2LabSelection.build(
            slice_name="oulu_user", radio="n300", ue="qfit07"
        )
        plan = build_plan(run_id="r2lab-test-qfit-ssh", selection=selection)
        runner = FakeRunner(qfit_ssh_ready=False)

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "r2lab"
            with self.assertRaises(R2LabResourceError):
                execute_prepare(
                    plan=plan,
                    run_root=root,
                    runner=runner,
                    sleeper=lambda _: None,
                )
            manifest = json.loads(
                (root / plan.run_id / "manifest.json").read_text(encoding="utf-8")
            )
            self.assertEqual("failed", manifest["status"])
            self.assertEqual("qfit-ssh-readiness", manifest["failure_stage"])
            self.assertEqual("held", manifest["resource_claim"])

        remote = runner.remote_commands
        self.assertIn(("rhubarbe", "wait", "7"), remote)
        self.assertFalse(
            any(
                command[:1] == ("ssh",) and QFIT_INITIALIZER in command
                for command in remote
            )
        )

    def test_qfit_prepare_fails_closed_on_unknown_usb_state(self) -> None:
        selection = R2LabSelection.build(
            slice_name="oulu_user", radio="n300", ue="qfit07"
        )
        plan = build_plan(run_id="r2lab-test-qfit-usb-unknown", selection=selection)
        runner = FakeRunner(qfit_usb_state=None)

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "r2lab"
            with self.assertRaises(R2LabResourceError):
                execute_prepare(
                    plan=plan,
                    run_root=root,
                    runner=runner,
                    sleeper=lambda _: None,
                )
            manifest = json.loads(
                (root / plan.run_id / "manifest.json").read_text(encoding="utf-8")
            )

        self.assertEqual("qfit-usb-power-state", manifest["failure_stage"])
        self.assertEqual("held", manifest["resource_claim"])
        self.assertNotIn(
            ("curl", "-fsS", "http://reboot07/usrpon"),
            runner.remote_commands,
        )

    def test_qfit_prepare_retries_modem_enumeration(self) -> None:
        selection = R2LabSelection.build(
            slice_name="oulu_user", radio="n300", ue="qfit07"
        )
        plan = build_plan(run_id="r2lab-test-qfit-enumeration", selection=selection)
        runner = FakeRunner(qfit_mbim_failures=1)
        waits: list[float] = []

        with tempfile.TemporaryDirectory() as directory:
            execute_prepare(
                plan=plan,
                run_root=Path(directory) / "r2lab",
                runner=runner,
                sleeper=waits.append,
                reachability_attempts=2,
                reachability_delay_seconds=5,
            )

        self.assertEqual([5], waits)
        mbim_checks = [
            command
            for command in runner.remote_commands
            if command[-3:] == ("test", "-c", "/dev/cdc-wdm0")
        ]
        self.assertEqual(3, len(mbim_checks))

    def test_qfit_prepare_stops_before_initializer_without_enumeration(self) -> None:
        selection = R2LabSelection.build(
            slice_name="oulu_user", radio="n300", ue="qfit07"
        )
        plan = build_plan(run_id="r2lab-test-qfit-enumeration-fail", selection=selection)
        runner = FakeRunner(qfit_mbim_failures=2)

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "r2lab"
            with self.assertRaises(R2LabResourceError):
                execute_prepare(
                    plan=plan,
                    run_root=root,
                    runner=runner,
                    sleeper=lambda _: None,
                    reachability_attempts=2,
                )
            manifest = json.loads(
                (root / plan.run_id / "manifest.json").read_text(encoding="utf-8")
            )

        self.assertEqual("qfit-enumeration-readiness", manifest["failure_stage"])
        self.assertFalse(
            any(command[-1:] == (QFIT_INITIALIZER,) for command in runner.remote_commands)
        )

    def test_qfit_release_proves_usb_and_host_off(self) -> None:
        selection = R2LabSelection.build(
            slice_name="oulu_user", radio="n300", ue="qfit07"
        )
        plan = build_plan(run_id="r2lab-test-qfit-release", selection=selection)
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
                slice_name=selection.slice_name,
                run_root=root,
                runner=runner,
            )
            manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))

        remote = runner.remote_commands
        usb_off = remote.index(("curl", "-fsS", "http://reboot07/usrpoff"))
        usb_status = remote.index(("curl", "-fsS", "http://reboot07/usrpstatus"))
        host_off = remote.index(("qfit", "off", "qfit07"))
        self.assertLess(usb_off, usb_status)
        self.assertLess(usb_status, host_off)
        self.assertEqual("released", manifest["status"])
        self.assertTrue(manifest["cleanup"]["claim_releasable"])

    def test_qfit_release_retains_claim_without_exact_usb_state(self) -> None:
        selection = R2LabSelection.build(
            slice_name="oulu_user", radio="n300", ue="qfit07"
        )
        plan = build_plan(run_id="r2lab-test-qfit-release-unknown", selection=selection)
        setup = FakeRunner()

        class UnknownUsbStatus(FakeRunner):
            def __call__(self, command, timeout_seconds: int) -> CommandResult:
                remote = self.remote(tuple(command))
                if remote == ("curl", "-fsS", "http://reboot07/usrpstatus"):
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
            failing = UnknownUsbStatus()
            with self.assertRaises(R2LabResourceError):
                execute_release(
                    run_id=plan.run_id,
                    slice_name=selection.slice_name,
                    run_root=root,
                    runner=failing,
                )
            manifest = json.loads(
                (root / plan.run_id / "manifest.json").read_text(encoding="utf-8")
            )
            self.assertTrue((root / "active.json").exists())

        self.assertEqual("release-failed", manifest["status"])
        self.assertEqual(["qfit07"], manifest["cleanup"]["unresolved_resources"])
        self.assertIn(("qfit", "off", "qfit07"), failing.remote_commands)
        self.assertIn(("rhubarbe", "pdu", "off", "n300"), failing.remote_commands)

    def test_qfit_gateway_command_maps_resource_to_physical_host(self) -> None:
        slice_name = "oulu_user"
        command = qfit_gateway_command(
            slice_name,
            "qfit07",
            "mbimcli",
            "--query-radio-state",
        )
        rendered = command[-1]
        self.assertIn("root@fit07", rendered)
        self.assertNotIn("root@qfit07", rendered)
        self.assertIn(
            f"UserKnownHostsFile=/home/{slice_name}/.ssh/known_hosts",
            rendered,
        )
        self.assertIn("GlobalKnownHostsFile=/dev/null", rendered)

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
