from __future__ import annotations

import shlex
import unittest

from synthran.live_preflight import CommandResult
from synthran.r2lab.readiness import (
    R2LabQfitReadinessError,
    execute_qfit_readiness,
    qfit_readiness_commands,
)


class ScriptedRunner:
    def __init__(self, script: list[object]) -> None:
        self.script = list(script)
        self.commands: list[tuple[str, ...]] = []

    def __call__(self, command, timeout_seconds: int) -> CommandResult:
        self.commands.append(tuple(command))
        outcome = self.script.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        assert isinstance(outcome, CommandResult)
        return outcome


class R2LabQfitReadinessTests(unittest.TestCase):
    def test_command_set_is_strict_read_only_and_bound_to_fit_node(self) -> None:
        commands = qfit_readiness_commands(
            qfit="qfit07",
            remote_known_hosts="/home/oulu_user/.synthran/run/fit07_known_hosts",
        )
        rendered = "\n".join(shlex.join(command) for command in commands)
        self.assertIn("http://reboot07/usrpstatus", rendered)
        self.assertIn("root@fit07", rendered)
        self.assertIn("StrictHostKeyChecking=yes", rendered)
        self.assertIn("UserKnownHostsFile=/home/oulu_user/.synthran/run/fit07_known_hosts", rendered)
        self.assertIn("/dev/ttyUSB2", rendered)
        self.assertIn("/dev/cdc-wdm0", rendered)
        self.assertIn("wwan0", rendered)
        self.assertNotIn("StrictHostKeyChecking=no", rendered)
        self.assertNotIn("accept-new", rendered)
        self.assertNotIn("usrpon ", rendered)
        self.assertNotIn("qfit on", rendered)
        self.assertNotIn("qfit off", rendered)
        self.assertNotIn("AT+CIMI", rendered)
        self.assertNotIn("--attach-packet-service", rendered)
        self.assertNotIn("--connect=", rendered)

    def test_all_required_observations_produce_ready_evidence(self) -> None:
        runner = ScriptedRunner(
            [
                CommandResult(0, "usrpon\n", ""),
                CommandResult(0, "", ""),
                CommandResult(0, "", ""),
                CommandResult(0, "", ""),
                CommandResult(0, "9: wwan0: <BROADCAST> mtu 1500\n", ""),
            ]
        )
        evidence = execute_qfit_readiness(
            qfit="qfit07",
            remote_known_hosts="/home/oulu_user/.synthran/run/fit07_known_hosts",
            runner=runner,
        )
        self.assertTrue(evidence.ready)
        self.assertFalse(evidence.transport_error)
        self.assertEqual("qfit07", evidence.qfit)
        self.assertNotIn("fit07_known_hosts", str(evidence.to_dict()))

    def test_usb_off_fails_readiness_without_mutation(self) -> None:
        runner = ScriptedRunner(
            [
                CommandResult(0, "usrpoff\n", ""),
                CommandResult(0, "", ""),
                CommandResult(1, "", ""),
                CommandResult(1, "", ""),
                CommandResult(1, "", ""),
            ]
        )
        evidence = execute_qfit_readiness(
            qfit="qfit07",
            remote_known_hosts="/home/oulu_user/.synthran/run/fit07_known_hosts",
            runner=runner,
        )
        self.assertFalse(evidence.ready)
        self.assertFalse(evidence.usb_power_on)
        rendered = "\n".join(shlex.join(command) for command in runner.commands)
        self.assertNotIn("usrpon ", rendered)
        self.assertNotIn("qfit on", rendered)

    def test_transport_error_is_explicitly_not_ready(self) -> None:
        runner = ScriptedRunner(
            [
                CommandResult(0, "usrpon\n", ""),
                RuntimeError("ssh failed"),
                CommandResult(0, "", ""),
                CommandResult(0, "", ""),
                CommandResult(0, "", ""),
            ]
        )
        evidence = execute_qfit_readiness(
            qfit="qfit07",
            remote_known_hosts="/home/oulu_user/.synthran/run/fit07_known_hosts",
            runner=runner,
        )
        self.assertFalse(evidence.ready)
        self.assertTrue(evidence.transport_error)

    def test_unreviewed_qfit_or_unsafe_known_hosts_path_is_rejected(self) -> None:
        runner = ScriptedRunner([])
        with self.assertRaises(R2LabQfitReadinessError):
            execute_qfit_readiness(
                qfit="qfit99",
                remote_known_hosts="/tmp/known_hosts",
                runner=runner,
            )
        with self.assertRaises(R2LabQfitReadinessError):
            execute_qfit_readiness(
                qfit="qfit07",
                remote_known_hosts="../known_hosts",
                runner=runner,
            )
        self.assertEqual([], runner.commands)


if __name__ == "__main__":
    unittest.main()
