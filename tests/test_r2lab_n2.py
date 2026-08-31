from __future__ import annotations

import unittest

from synthran.r2lab.n2 import (
    N2State,
    R2LabN2EvidenceError,
    build_amf_n2_evidence,
    parse_amf_n2_acceptance,
    parse_n2_log_state,
)


class R2LabGnbN2EvidenceTests(unittest.TestCase):
    def test_affirmative_gnb_log_is_established(self) -> None:
        self.assertIs(
            parse_n2_log_state("NGAP connection established with AMF"),
            N2State.ESTABLISHED,
        )
        self.assertIs(
            parse_n2_log_state("AMF association connected successfully"),
            N2State.ESTABLISHED,
        )

    def test_error_or_empty_gnb_log_is_not_proof(self) -> None:
        self.assertIs(parse_n2_log_state(""), N2State.NOT_OBSERVED)
        self.assertIs(
            parse_n2_log_state("NGAP connection failed with timeout"),
            N2State.NOT_OBSERVED,
        )


class R2LabAmfN2EvidenceTests(unittest.TestCase):
    def test_exact_open5gs_peer_acceptance_is_recognized(self) -> None:
        text = "\n".join(
            (
                "08/22 18:16:15.965 [amf] INFO: gNB-N2 accepted[10.10.3.234]:58612",
                "08/22 18:16:15.968 [amf] INFO: [Added] Number of gNBs is now 1",
            )
        )
        self.assertTrue(
            parse_amf_n2_acceptance(text, expected_peer="10.10.3.234")
        )

    def test_different_peer_does_not_satisfy_expected_gnb(self) -> None:
        text = "08/22 [amf] INFO: gNB-N2 accepted[10.10.3.235]:58612\n"
        self.assertFalse(
            parse_amf_n2_acceptance(text, expected_peer="10.10.3.234")
        )

    def test_failure_text_is_not_affirmative_evidence(self) -> None:
        text = "[amf] ERROR: gNB-N2 accepted[10.10.3.234] then failed\n"
        self.assertFalse(
            parse_amf_n2_acceptance(text, expected_peer="10.10.3.234")
        )

    def test_serialized_evidence_binds_peer_without_persisting_private_ip(self) -> None:
        text = "[amf] INFO: gNB-N2 accepted[10.10.3.234]\n"
        evidence = build_amf_n2_evidence(
            text=text,
            expected_peer="10.10.3.234",
        )
        payload = evidence.to_dict()
        self.assertTrue(evidence.proven)
        self.assertEqual(64, len(evidence.peer_fingerprint))
        self.assertNotIn("10.10.3.234", str(payload))
        self.assertEqual("sanitized-amf-exact-gnb-peer", payload["source"])

    def test_transport_error_fails_closed_even_if_text_contains_accept(self) -> None:
        evidence = build_amf_n2_evidence(
            text="[amf] INFO: gNB-N2 accepted[10.10.3.234]\n",
            expected_peer="10.10.3.234",
            transport_error=True,
        )
        self.assertFalse(evidence.proven)
        self.assertFalse(evidence.accepted)
        self.assertTrue(evidence.transport_error)

    def test_invalid_expected_peer_is_rejected(self) -> None:
        with self.assertRaises(R2LabN2EvidenceError):
            parse_amf_n2_acceptance(
                "gNB-N2 accepted[10.10.3.234]",
                expected_peer="gnb.example.test",
            )


if __name__ == "__main__":
    unittest.main()
