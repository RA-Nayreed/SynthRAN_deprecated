from __future__ import annotations

import unittest

from synthran.live_preflight import CommandResult
from synthran.r2lab.radio import (
    CellAcquisitionState,
    Ipv4State,
    PacketServiceState,
    QfitRuntimeEvidence,
    RegistrationState,
)
from synthran.r2lab.ue import QfitActivationRequest, execute_qfit_activation


RUN_ID = "r2lab-provider-order-test"


class ProviderOrderedQfit:
    """Registration becomes observable only after packet-service attach."""

    def __init__(self) -> None:
        self.radio_on = False
        self.attached = False
        self.connected = False
        self.ipv4 = False
        self.commands: list[tuple[str, ...]] = []

    def __call__(self, command, timeout_seconds: int) -> CommandResult:
        value = tuple(command)
        self.commands.append(value)
        if "--query-radio-state" in value:
            state = "on" if self.radio_on else "off"
            return CommandResult(0, f"Software radio state: '{state}'\n", "")
        if "--set-radio-state=on" in value:
            self.radio_on = True
            return CommandResult(0, "", "")
        if "--attach-packet-service" in value:
            self.attached = True
            return CommandResult(0, "", "")
        if any(item.startswith("--connect=") for item in value):
            if self.attached:
                self.connected = True
            return CommandResult(0, "", "")
        if value and value[0] == "mbim-set-ip.sh":
            if self.connected:
                self.ipv4 = True
            return CommandResult(0, "", "")
        if "--set-radio-state=off" in value:
            self.radio_on = False
            self.attached = False
            self.connected = False
            self.ipv4 = False
            return CommandResult(0, "", "")
        if value[:4] == ("ip", "link", "set", "dev"):
            return CommandResult(0, "", "")
        raise AssertionError(f"unexpected command: {value}")

    def observe(self) -> QfitRuntimeEvidence:
        # This is the live-provider behavior that the previous implementation
        # could never pass: radio-on alone does not make registration visible.
        if not self.attached:
            return QfitRuntimeEvidence(
                cell=CellAcquisitionState.NO_SERVICE,
                registration=RegistrationState.NOT_REGISTERED,
                packet_service=PacketServiceState.DETACHED,
                ipv4=Ipv4State.ABSENT,
            )
        return QfitRuntimeEvidence(
            cell=CellAcquisitionState.ACQUIRED_NR_SA,
            registration=RegistrationState.REGISTERED,
            packet_service=PacketServiceState.ATTACHED,
            ipv4=Ipv4State.PRESENT if self.ipv4 else Ipv4State.ABSENT,
        )


class ProviderAlignedActivationTests(unittest.TestCase):
    def test_attach_precedes_registration_observation_gate(self) -> None:
        fake = ProviderOrderedQfit()
        result = execute_qfit_activation(
            request=QfitActivationRequest(run_id=RUN_ID, qfit="qfit07"),
            runner=fake,
            observer=fake.observe,
            sleeper=lambda _: None,
            registration_attempts=1,
            packet_attempts=1,
            pdu_attempts=1,
            rollback_attempts=1,
            poll_interval_seconds=0,
        )

        self.assertTrue(result.accepted)
        self.assertEqual("pdu-established", result.status)
        names = [step.name for step in result.steps]
        self.assertEqual(
            [
                "link-up",
                "radio-on",
                "attach-packet-service",
                "connect-session",
                "configure-ip",
            ],
            names,
        )
        rendered = [" ".join(command) for command in fake.commands]
        attach_index = next(
            index for index, command in enumerate(rendered)
            if "--attach-packet-service" in command
        )
        connect_index = next(
            index for index, command in enumerate(rendered)
            if "--connect=" in command
        )
        self.assertLess(attach_index, connect_index)


if __name__ == "__main__":
    unittest.main()
