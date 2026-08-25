from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from synthran.r2lab.acceptance import (
    PhysicalAcceptanceStage,
    PhysicalRunEvidence,
    STAGE_ORDER,
)
from synthran.r2lab.radio import RegistrationState
from synthran.r2lab.ue import R2LabPhysicalUeError
from synthran.r2lab.ue_activation import (
    _AT_LOCK,
    _AT_SCRIPT,
    _at,
    _command_timeout,
    parse_mbim_registration,
    recover_retryable_transport_failure,
)


RUN_ID = "r2lab-ue-proof-test"


def _failed_cell_evidence() -> PhysicalRunEvidence:
    state = PhysicalRunEvidence(run_id=RUN_ID)
    for stage in STAGE_ORDER[:6]:
        state = state.pass_stage(stage, source=f"test-{stage.value}")
    return state.fail_stage(
        PhysicalAcceptanceStage.CELL_ACQUISITION,
        source="physical-ue-activation:cell-unknown",
    )


class PhysicalAtTransportTests(unittest.TestCase):
    def test_at_probe_is_stdlib_locked_and_allowlisted(self) -> None:
        self.assertNotIn("import serial", _AT_SCRIPT)
        self.assertIn("fcntl.flock", _AT_SCRIPT)
        self.assertIn(_AT_LOCK, _AT_SCRIPT)
        self.assertIn("termios", _AT_SCRIPT)
        self.assertEqual("AT+QNWINFO", _at("AT+QNWINFO")[-1])
        self.assertEqual("AT+C5GREG?", _at("AT+C5GREG?")[-1])
        with self.assertRaises(R2LabPhysicalUeError):
            _at("AT+CIMI")

    def test_command_timeout_is_bounded_by_one_deadline(self) -> None:
        self.assertEqual(5, _command_timeout(deadline=100.0, now=95.0, cap=30))
        self.assertEqual(10, _command_timeout(deadline=100.0, now=50.0, cap=10))
        with self.assertRaises(R2LabPhysicalUeError):
            _command_timeout(deadline=100.0, now=100.0, cap=30)


class MbimRegistrationTests(unittest.TestCase):
    def test_registration_states_are_reduced_without_raw_persistence(self) -> None:
        self.assertIs(
            RegistrationState.REGISTERED,
            parse_mbim_registration("Register state: 'home'"),
        )
        self.assertIs(
            RegistrationState.REGISTERED,
            parse_mbim_registration("Register state: 'roaming'"),
        )
        self.assertIs(
            RegistrationState.SEARCHING,
            parse_mbim_registration("Register state: 'searching'"),
        )
        self.assertIs(
            RegistrationState.NOT_REGISTERED,
            parse_mbim_registration("Register state: 'deregistered'"),
        )
        self.assertIs(
            RegistrationState.UNKNOWN,
            parse_mbim_registration("no registration field"),
        )


class RetryableTransportFailureTests(unittest.TestCase):
    def test_transport_failed_cell_can_resume_without_erasing_passed_stages(self) -> None:
        evidence = _failed_cell_evidence()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "physical-ue-activation.json"
            path.write_text(
                json.dumps(
                    {
                        "run_id": RUN_ID,
                        "runtime": {"transport_error": True},
                    }
                ),
                encoding="utf-8",
            )
            repaired = recover_retryable_transport_failure(
                evidence=evidence,
                activation_evidence_path=path,
            )

        self.assertIsNone(repaired.acceptance.failed_stage)
        self.assertIs(
            PhysicalAcceptanceStage.CELL_ACQUISITION,
            repaired.acceptance.next_stage,
        )
        for stage in STAGE_ORDER[:6]:
            self.assertEqual(
                "passed",
                repaired.acceptance.outcome_for(stage).value,
            )

    def test_real_negative_cell_failure_remains_terminal(self) -> None:
        evidence = _failed_cell_evidence()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "physical-ue-activation.json"
            path.write_text(
                json.dumps(
                    {
                        "run_id": RUN_ID,
                        "runtime": {"transport_error": False},
                    }
                ),
                encoding="utf-8",
            )
            unchanged = recover_retryable_transport_failure(
                evidence=evidence,
                activation_evidence_path=path,
            )

        self.assertIs(
            PhysicalAcceptanceStage.CELL_ACQUISITION,
            unchanged.acceptance.failed_stage,
        )
        self.assertIsNone(unchanged.acceptance.next_stage)

    def test_failure_without_matching_activation_evidence_remains_terminal(self) -> None:
        evidence = _failed_cell_evidence()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "physical-ue-activation.json"
            path.write_text(
                json.dumps(
                    {
                        "run_id": "different-run",
                        "runtime": {"transport_error": True},
                    }
                ),
                encoding="utf-8",
            )
            unchanged = recover_retryable_transport_failure(
                evidence=evidence,
                activation_evidence_path=path,
            )

        self.assertIs(
            PhysicalAcceptanceStage.CELL_ACQUISITION,
            unchanged.acceptance.failed_stage,
        )


if __name__ == "__main__":
    unittest.main()
