from __future__ import annotations

from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from synthran.live_preflight import CommandResult
from synthran.r2lab.controller import (
    QFIT_INITIALIZER,
    QFIT_MBIM_DEVICE,
    R2LabSelection,
    build_plan,
    execute_prepare,
    gateway_command,
)


class QfitPrepareRunner:
    """Deterministic Faraday/qfit double for the post-power-on startup contract."""

    def __init__(self) -> None:
        self.commands: list[tuple[str, ...]] = []
        self.power: dict[str, str] = {}

    @staticmethod
    def remote(command: tuple[str, ...]) -> tuple[str, ...]:
        split = command.index("--")
        return command[split + 2 :]

    @staticmethod
    def qfit_node(qfit: str) -> int:
        return int(qfit.removeprefix("qfit"))

    def __call__(self, command, timeout_seconds: int) -> CommandResult:
        value = tuple(command)
        self.commands.append(value)
        remote = self.remote(value)

        if remote in {("rhubarbe", "leases", "--check"), ("true",)}:
            return CommandResult(0, "", "")

        if remote[:3] == ("rhubarbe", "pdu", "on") and len(remote) == 4:
            resource = remote[3]
            self.power[resource] = "on"
            return CommandResult(
                0,
                f"pdu2 chain-0@outlet-1 ({resource}): ON (28W)\n",
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

        if remote[:2] == ("qfit", "off") and len(remote) == 3:
            qfit = remote[2]
            self.power[qfit] = "off"
            return CommandResult(0, f"reboot{self.qfit_node(qfit):02d}:ok\n", "")

        if remote[:2] == ("qfit", "on") and len(remote) == 3:
            qfit = remote[2]
            self.power[qfit] = "on"
            return CommandResult(0, f"reboot{self.qfit_node(qfit):02d}:ok\n", "")

        if remote[:2] == ("rhubarbe", "status") and len(remote) == 3:
            node = int(remote[2])
            qfit = f"qfit{node:02d}"
            state = self.power.get(qfit)
            if state in {"on", "off"}:
                return CommandResult(0, f"reboot{node:02d}:{state}\n", "")
            return CommandResult(0, "", "")

        if remote[:1] == ("ping",):
            return CommandResult(0, "", "")

        # Nested strict SSH to the selected physical FIT host.
        if remote[:1] == ("ssh",):
            return CommandResult(0, "", "")

        raise AssertionError(f"unexpected command: {remote}")

    @property
    def remote_commands(self) -> list[tuple[str, ...]]:
        return [self.remote(command) for command in self.commands]


class R2LabQfitPrepareTests(unittest.TestCase):
    def test_prepare_initializes_qfit_once_after_power_on_then_enables_radio(self) -> None:
        selection = R2LabSelection.build(
            slice_name="oulu_user",
            radio="n300",
            ue="qfit07",
        )
        plan = build_plan(run_id="r2lab-qfit-prepare", selection=selection)
        runner = QfitPrepareRunner()

        with tempfile.TemporaryDirectory() as directory, patch.dict(
            "os.environ",
            {"SYNTHRAN_R2LAB_IDENTITY": "/tmp/synthran-test-identity"},
            clear=False,
        ):
            result = execute_prepare(
                plan=plan,
                run_root=Path(directory) / "r2lab",
                runner=runner,
                sleeper=lambda _: None,
                power_settle_seconds=0,
                reachability_attempts=1,
                reachability_delay_seconds=0,
            )

        self.assertEqual("ready", result.status)
        commands = runner.remote_commands

        init_matches = [
            (index, command)
            for index, command in enumerate(commands)
            if command[:1] == ("ssh",) and QFIT_INITIALIZER in command
        ]
        radio_matches = [
            (index, command)
            for index, command in enumerate(commands)
            if command[:1] == ("ssh",)
            and "mbimcli" in command
            and QFIT_MBIM_DEVICE in command
            and "--set-radio-state=on" in command
        ]

        self.assertEqual(1, len(init_matches))
        self.assertEqual(1, len(radio_matches))
        self.assertLess(init_matches[0][0], radio_matches[0][0])

        init_index = init_matches[0][0]
        radio_index = radio_matches[0][0]
        self.assertIn(("rhubarbe", "leases", "--check"), commands[:init_index])
        self.assertIn(("rhubarbe", "leases", "--check"), commands[init_index + 1 : radio_index])

        self.assertIn("root@fit07", init_matches[0][1])
        self.assertIn("root@fit07", radio_matches[0][1])
        self.assertNotIn("root@qfit07", init_matches[0][1])
        self.assertNotIn("root@qfit07", radio_matches[0][1])

    def test_gateway_maps_joined_qfit_destination_for_ue_and_workload_paths(self) -> None:
        with patch.dict(
            "os.environ",
            {"SYNTHRAN_R2LAB_IDENTITY": "/tmp/synthran-test-identity"},
            clear=False,
        ):
            command = gateway_command(
                "oulu_user",
                "ssh -o BatchMode=yes -- root@qfit07 mbimcli --query-radio-state",
            )
        self.assertIn("root@fit07", command[-1])
        self.assertNotIn("root@qfit07", command[-1])


if __name__ == "__main__":
    unittest.main()
