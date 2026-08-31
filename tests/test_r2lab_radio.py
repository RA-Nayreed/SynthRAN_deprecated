from __future__ import annotations

import unittest

from synthran.live_preflight import CommandResult
from synthran.r2lab.radio import (
    CellAcquisitionState,
    Ipv4State,
    PacketServiceState,
    QfitRuntimeEvidence,
    RegistrationState,
    UserPlaneProbeError,
    build_user_plane_ping_command,
    classify_qfit_runtime,
    execute_user_plane_probe,
    parse_c5greg,
    parse_ipv4_state,
    parse_packet_service,
    parse_qnwinfo,
)


class R2LabQfitRuntimeTests(unittest.TestCase):
    def test_no_service_state_does_not_advance_acceptance(self) -> None:
        evidence = QfitRuntimeEvidence(
            cell=parse_qnwinfo('+QNWINFO: "No Service"\nOK\n'),
            registration=parse_c5greg('+C5GREG: 0,0\nOK\n'),
            packet_service=parse_packet_service("Packet service state: 'detached'\n"),
            ipv4=parse_ipv4_state(""),
        )
        self.assertEqual(CellAcquisitionState.NO_SERVICE, evidence.cell)
        self.assertEqual(RegistrationState.NOT_REGISTERED, evidence.registration)
        self.assertEqual(PacketServiceState.DETACHED, evidence.packet_service)
        self.assertEqual(Ipv4State.ABSENT, evidence.ipv4)
        self.assertFalse(evidence.cell_acquired)
        self.assertFalse(evidence.registered)
        self.assertFalse(evidence.pdu_session_established)

    def test_nr_sa_registration_and_packet_state_are_separate_gates(self) -> None:
        acquired = QfitRuntimeEvidence(
            cell=parse_qnwinfo('+QNWINFO: "NR5G-SA","00101","NR5G BAND 78",640000\n'),
            registration=parse_c5greg('+C5GREG: 0,2\n'),
            packet_service=parse_packet_service("Packet service state: 'detached'\n"),
            ipv4=parse_ipv4_state(""),
        )
        self.assertTrue(acquired.cell_acquired)
        self.assertFalse(acquired.registered)
        self.assertFalse(acquired.pdu_session_established)

        registered = QfitRuntimeEvidence(
            cell=acquired.cell,
            registration=parse_c5greg('+C5GREG: 0,1\n'),
            packet_service=acquired.packet_service,
            ipv4=acquired.ipv4,
        )
        self.assertTrue(registered.registered)
        self.assertFalse(registered.pdu_session_established)

    def test_attached_plus_ipv4_is_pdu_evidence_but_not_user_plane(self) -> None:
        evidence = QfitRuntimeEvidence(
            cell=parse_qnwinfo('+QNWINFO: "NR5G-SA","00101","NR5G BAND 78",640000\n'),
            registration=parse_c5greg('+C5GREG: 0,1\n'),
            packet_service=parse_packet_service("Packet service state: 'attached'\n"),
            ipv4=parse_ipv4_state(
                "9: wwan0    inet 198.51.100.2/24 scope global wwan0\n"
            ),
        )
        self.assertTrue(evidence.pdu_session_established)
        self.assertEqual("requires-separate-traffic-probe", evidence.to_dict()["user_plane"])

    def test_conflicting_registration_or_packet_state_stays_unknown(self) -> None:
        self.assertEqual(
            RegistrationState.UNKNOWN,
            parse_c5greg('+C5GREG: 0,2\n+C5GREG: 0,1\n'),
        )
        self.assertEqual(
            PacketServiceState.UNKNOWN,
            parse_packet_service(
                "Packet service state: 'detached'\nPacket service state: 'attached'\n"
            ),
        )

    def test_missing_interface_is_unknown_not_clean(self) -> None:
        self.assertEqual(Ipv4State.UNKNOWN, parse_ipv4_state("", interface_present=False))

    def test_classifier_discards_raw_probe_text_and_returns_only_sanitized_states(self) -> None:
        evidence = classify_qfit_runtime(
            qnwinfo_output='+QNWINFO: "NR5G-SA","00101","NR5G BAND 78",640000\n',
            c5greg_output='+C5GREG: 0,1\n',
            packet_service_output="Packet service state: 'attached'\n",
            ipv4_output="9: wwan0    inet 198.51.100.2/24 scope global wwan0\n",
        )
        payload = evidence.to_dict()
        self.assertEqual("acquired-nr-sa", payload["cell"])
        self.assertEqual("registered", payload["registration"])
        self.assertEqual("attached", payload["packet_service"])
        self.assertEqual("present", payload["ipv4"])
        self.assertTrue(payload["pdu_session_established"])
        self.assertNotIn("00101", str(payload))
        self.assertNotIn("198.51.100.2", str(payload))


class R2LabUserPlaneProbeTests(unittest.TestCase):
    def test_probe_is_argv_only_and_explicitly_bound_to_wwan0(self) -> None:
        command = build_user_plane_ping_command("198.51.100.10")
        self.assertEqual("ping", command[0])
        self.assertEqual("wwan0", command[command.index("-I") + 1])
        self.assertEqual("4", command[command.index("-c") + 1])
        self.assertEqual("198.51.100.10", command[-1])

    def test_probe_rejects_hostname_or_nonphysical_interface(self) -> None:
        with self.assertRaisesRegex(UserPlaneProbeError, "literal IP"):
            build_user_plane_ping_command("example.invalid")
        with self.assertRaisesRegex(UserPlaneProbeError, "wwan0"):
            build_user_plane_ping_command("198.51.100.10", interface="eth0")

    def test_success_persists_counts_and_peer_fingerprint_not_raw_peer(self) -> None:
        commands: list[tuple[str, ...]] = []

        def runner(command, timeout_seconds: int) -> CommandResult:
            commands.append(tuple(command))
            return CommandResult(
                0,
                "4 packets transmitted, 4 received, 0% packet loss, time 3ms\n",
                "",
            )

        evidence = execute_user_plane_probe(
            peer="198.51.100.10",
            runner=runner,
        )
        payload = evidence.to_dict()
        self.assertTrue(payload["proven"])
        self.assertEqual(4, payload["transmitted_packets"])
        self.assertEqual(4, payload["received_packets"])
        self.assertEqual("wwan0", payload["interface"])
        self.assertEqual(64, len(payload["peer_sha256"]))
        self.assertNotIn("198.51.100.10", str(payload))
        self.assertEqual("wwan0", commands[0][commands[0].index("-I") + 1])

    def test_loss_or_transport_error_is_not_user_plane_proof(self) -> None:
        loss = execute_user_plane_probe(
            peer="198.51.100.10",
            runner=lambda command, timeout_seconds: CommandResult(
                1,
                "4 packets transmitted, 3 received, 25% packet loss\n",
                "",
            ),
        )
        self.assertFalse(loss.proven)
        self.assertTrue(loss.summary_observed)
        self.assertEqual(3, loss.received_packets)

        def failing(command, timeout_seconds: int) -> CommandResult:
            raise RuntimeError("network unavailable")

        unavailable = execute_user_plane_probe(
            peer="198.51.100.10",
            runner=failing,
        )
        self.assertFalse(unavailable.proven)
        self.assertTrue(unavailable.transport_error)
        self.assertNotIn("network unavailable", str(unavailable.to_dict()))


if __name__ == "__main__":
    unittest.main()
