from __future__ import annotations

import unittest

from synthran.live_preflight import CommandResult
from synthran.r2lab.provider import (
    CleanupEvidence,
    CleanupState,
    PowerState,
    R2LabPowerStateError,
    R2LabQfitStateError,
    evaluate_pdu_transition,
    execute_verified_pdu_transition,
    execute_verified_qfit_transition,
    parse_pdu_status,
    parse_qfit_status,
    qfit_node_number,
    release_assessment,
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


class R2LabPduStateTests(unittest.TestCase):
    def test_parses_exact_on_state_and_watts(self) -> None:
        observation = parse_pdu_status(
            "pdu2 chain-0@outlet-1 (n300): ON (28W)\n",
            resource="n300",
        )
        self.assertEqual(PowerState.ON, observation.state)
        self.assertEqual(28, observation.watts)

    def test_parses_exact_off_state_without_watts(self) -> None:
        observation = parse_pdu_status(
            "pdu2 chain-0@outlet-1 (n300): OFF\n",
            resource="n300",
        )
        self.assertEqual(PowerState.OFF, observation.state)
        self.assertIsNone(observation.watts)

    def test_ignores_other_resources(self) -> None:
        observation = parse_pdu_status(
            "pdu2 chain-0@outlet-1 (n320): OFF\n",
            resource="n300",
        )
        self.assertEqual(PowerState.UNKNOWN, observation.state)

    def test_conflicting_state_fails_closed(self) -> None:
        with self.assertRaises(R2LabPowerStateError):
            parse_pdu_status(
                "\n".join(
                    (
                        "pdu2 chain-0@outlet-1 (n300): ON (28W)",
                        "pdu2 chain-0@outlet-1 (n300): OFF",
                    )
                ),
                resource="n300",
            )

    def test_successful_off_does_not_require_zero_mutation_returncode(self) -> None:
        evidence = evaluate_pdu_transition(
            resource="n300",
            requested_state=PowerState.OFF,
            mutation_returncode=1,
            status_returncode=0,
            status_stdout="pdu2 chain-0@outlet-1 (n300): OFF\n",
        )
        self.assertTrue(evidence.confirmed)
        self.assertEqual(1, evidence.mutation_returncode)
        self.assertEqual(PowerState.OFF, evidence.observed_state)

    def test_timeout_returncode_can_still_be_resolved_by_exact_status(self) -> None:
        evidence = evaluate_pdu_transition(
            resource="n300",
            requested_state=PowerState.OFF,
            mutation_returncode=None,
            status_returncode=0,
            status_stdout="pdu2 chain-0@outlet-1 (n300): OFF\n",
        )
        self.assertTrue(evidence.confirmed)
        self.assertIsNone(evidence.mutation_returncode)

    def test_timeout_without_state_evidence_remains_unknown(self) -> None:
        evidence = evaluate_pdu_transition(
            resource="n300",
            requested_state=PowerState.OFF,
            mutation_returncode=None,
            status_returncode=None,
        )
        self.assertFalse(evidence.confirmed)
        self.assertEqual(PowerState.UNKNOWN, evidence.observed_state)

    def test_textual_state_not_mutation_returncode_decides_transition(self) -> None:
        evidence = evaluate_pdu_transition(
            resource="n300",
            requested_state=PowerState.OFF,
            mutation_returncode=0,
            status_returncode=0,
            status_stdout="pdu2 chain-0@outlet-1 (n300): ON (28W)\n",
        )
        self.assertFalse(evidence.confirmed)
        self.assertEqual(PowerState.ON, evidence.observed_state)

    def test_status_text_can_be_read_from_stderr(self) -> None:
        evidence = evaluate_pdu_transition(
            resource="n300",
            requested_state=PowerState.OFF,
            mutation_returncode=1,
            status_returncode=1,
            status_stderr="pdu2 chain-0@outlet-1 (n300): OFF\n",
        )
        self.assertTrue(evidence.confirmed)

    def test_missing_status_text_remains_unknown(self) -> None:
        evidence = evaluate_pdu_transition(
            resource="n300",
            requested_state=PowerState.OFF,
            mutation_returncode=0,
            status_returncode=0,
        )
        self.assertFalse(evidence.confirmed)
        self.assertEqual(PowerState.UNKNOWN, evidence.observed_state)

    def test_unknown_is_not_a_valid_requested_state(self) -> None:
        with self.assertRaises(R2LabPowerStateError):
            evaluate_pdu_transition(
                resource="n300",
                requested_state=PowerState.UNKNOWN,
                mutation_returncode=0,
                status_returncode=0,
                status_stdout="pdu2 chain-0@outlet-1 (n300): OFF\n",
            )


class R2LabVerifiedPduOperationTests(unittest.TestCase):
    def test_successful_off_accepts_live_rc1_semantics(self) -> None:
        runner = ScriptedRunner(
            [
                CommandResult(
                    1,
                    "Doing a soft TURN OFF on device n300\n"
                    "pdu2 chain-0@outlet-1 (n300): OFF\n",
                    "",
                ),
                CommandResult(0, "pdu2 chain-0@outlet-1 (n300): OFF\n", ""),
            ]
        )
        result = execute_verified_pdu_transition(
            resource="n300",
            requested_state=PowerState.OFF,
            runner=runner,
            timeout_seconds=30,
        )
        self.assertTrue(result.confirmed)
        self.assertEqual(1, result.evidence.mutation_returncode)
        self.assertEqual(PowerState.OFF, result.evidence.observed_state)

    def test_mutation_timeout_still_checks_exact_provider_state(self) -> None:
        runner = ScriptedRunner(
            [
                RuntimeError("timed out"),
                CommandResult(0, "pdu2 chain-0@outlet-1 (n300): OFF\n", ""),
            ]
        )
        result = execute_verified_pdu_transition(
            resource="n300",
            requested_state=PowerState.OFF,
            runner=runner,
            timeout_seconds=30,
        )
        self.assertTrue(result.confirmed)
        self.assertTrue(result.mutation_transport_error)
        self.assertIsNone(result.evidence.mutation_returncode)

    def test_status_timeout_keeps_transition_unresolved(self) -> None:
        runner = ScriptedRunner(
            [CommandResult(0, "", ""), RuntimeError("status timed out")]
        )
        result = execute_verified_pdu_transition(
            resource="n300",
            requested_state=PowerState.OFF,
            runner=runner,
            timeout_seconds=30,
        )
        self.assertFalse(result.confirmed)
        self.assertTrue(result.status_transport_error)
        self.assertEqual(PowerState.UNKNOWN, result.evidence.observed_state)

    def test_wrong_observed_state_is_not_confirmed(self) -> None:
        runner = ScriptedRunner(
            [
                CommandResult(0, "", ""),
                CommandResult(0, "pdu2 chain-0@outlet-1 (n300): ON (28W)\n", ""),
            ]
        )
        result = execute_verified_pdu_transition(
            resource="n300",
            requested_state=PowerState.OFF,
            runner=runner,
            timeout_seconds=30,
        )
        self.assertFalse(result.confirmed)
        self.assertEqual(PowerState.ON, result.evidence.observed_state)

    def test_status_returncode_is_diagnostic_when_text_is_exact(self) -> None:
        runner = ScriptedRunner(
            [
                CommandResult(0, "", ""),
                CommandResult(1, "", "pdu2 chain-0@outlet-1 (n300): OFF\n"),
            ]
        )
        result = execute_verified_pdu_transition(
            resource="n300",
            requested_state=PowerState.OFF,
            runner=runner,
            timeout_seconds=30,
        )
        self.assertTrue(result.confirmed)
        self.assertEqual(1, result.evidence.status_returncode)

    def test_on_transition_uses_only_exact_selected_resource(self) -> None:
        runner = ScriptedRunner(
            [
                CommandResult(0, "", ""),
                CommandResult(0, "pdu2 chain-0@outlet-1 (n320): ON (31W)\n", ""),
            ]
        )
        result = execute_verified_pdu_transition(
            resource="n320",
            requested_state=PowerState.ON,
            runner=runner,
            timeout_seconds=30,
        )
        self.assertTrue(result.confirmed)
        joined = "\n".join(" ".join(command) for command in runner.commands)
        self.assertNotIn("all-off", joined)
        self.assertNotIn("bye", joined)
        self.assertNotIn("n300", joined)


class R2LabQfitProviderTests(unittest.TestCase):
    def test_qfit_identifier_maps_to_exact_r2lab_node(self) -> None:
        self.assertEqual(7, qfit_node_number("qfit07"))
        self.assertEqual(34, qfit_node_number("qfit34"))

    def test_invalid_qfit_identifier_fails_closed(self) -> None:
        for value in ("fit07", "qfit7", "qfit00", "qfit07;all-off"):
            with self.subTest(value=value):
                with self.assertRaises(R2LabQfitStateError):
                    qfit_node_number(value)

    def test_parses_exact_live_off_observation(self) -> None:
        observation = parse_qfit_status("reboot07:off\n", qfit="qfit07")
        self.assertEqual(7, observation.node)
        self.assertEqual(PowerState.OFF, observation.state)

    def test_other_reboot_node_is_ignored(self) -> None:
        observation = parse_qfit_status("reboot09:off\n", qfit="qfit07")
        self.assertEqual(PowerState.UNKNOWN, observation.state)

    def test_conflicting_qfit_status_fails_closed(self) -> None:
        with self.assertRaises(R2LabQfitStateError):
            parse_qfit_status("reboot07:on\nreboot07:off\n", qfit="qfit07")

    def test_verified_off_uses_qfit_then_exact_status_node(self) -> None:
        runner = ScriptedRunner(
            [
                CommandResult(0, "reboot07:ok\n", ""),
                CommandResult(0, "reboot07:off\n", ""),
            ]
        )
        result = execute_verified_qfit_transition(
            qfit="qfit07",
            requested_state=PowerState.OFF,
            runner=runner,
            timeout_seconds=30,
        )
        self.assertTrue(result.confirmed)
        self.assertEqual(
            [("qfit", "off", "qfit07"), ("rhubarbe", "status", "7")],
            runner.commands,
        )

    def test_qfit_mutation_timeout_still_queries_exact_status(self) -> None:
        runner = ScriptedRunner(
            [RuntimeError("qfit timed out"), CommandResult(0, "reboot07:off\n", "")]
        )
        result = execute_verified_qfit_transition(
            qfit="qfit07",
            requested_state=PowerState.OFF,
            runner=runner,
            timeout_seconds=30,
        )
        self.assertTrue(result.confirmed)
        self.assertTrue(result.mutation_transport_error)

    def test_qfit_status_timeout_is_unresolved(self) -> None:
        runner = ScriptedRunner(
            [CommandResult(0, "", ""), RuntimeError("status timed out")]
        )
        result = execute_verified_qfit_transition(
            qfit="qfit07",
            requested_state=PowerState.OFF,
            runner=runner,
            timeout_seconds=30,
        )
        self.assertFalse(result.confirmed)
        self.assertEqual(PowerState.UNKNOWN, result.observed_state)
        self.assertTrue(result.status_transport_error)

    def test_qfit_on_requires_provider_to_report_on(self) -> None:
        runner = ScriptedRunner(
            [CommandResult(0, "", ""), CommandResult(0, "reboot07:off\n", "")]
        )
        result = execute_verified_qfit_transition(
            qfit="qfit07",
            requested_state=PowerState.ON,
            runner=runner,
            timeout_seconds=30,
        )
        self.assertFalse(result.confirmed)
        self.assertEqual(PowerState.OFF, result.observed_state)


class R2LabCleanupAssessmentTests(unittest.TestCase):
    def test_claim_releases_only_when_both_exact_resources_are_proven_off(self) -> None:
        assessment = release_assessment(
            ue=CleanupEvidence(
                resource="qfit07",
                stage="ue-power-off-release",
                state=CleanupState.PROVEN_OFF,
                source="provider-status",
            ),
            radio=CleanupEvidence(
                resource="n300",
                stage="radio-power-off-release",
                state=CleanupState.PROVEN_OFF,
                source="pdu-status",
            ),
        )
        self.assertTrue(assessment.claim_releasable)
        self.assertEqual((), assessment.unresolved_resources)

    def test_unknown_ue_keeps_claim_even_when_radio_is_clean(self) -> None:
        assessment = release_assessment(
            ue=CleanupEvidence(
                resource="qfit07",
                stage="ue-power-off-release",
                state=CleanupState.UNKNOWN,
                source="timeout",
            ),
            radio=CleanupEvidence(
                resource="n300",
                stage="radio-power-off-release",
                state=CleanupState.PROVEN_OFF,
                source="pdu-status",
            ),
        )
        self.assertFalse(assessment.claim_releasable)
        self.assertEqual(("qfit07",), assessment.unresolved_resources)

    def test_unknown_radio_keeps_claim_even_when_ue_is_clean(self) -> None:
        assessment = release_assessment(
            ue=CleanupEvidence(
                resource="qfit07",
                stage="ue-power-off-release",
                state=CleanupState.PROVEN_OFF,
                source="provider-status",
            ),
            radio=CleanupEvidence(
                resource="n300",
                stage="radio-power-off-release",
                state=CleanupState.UNKNOWN,
                source="missing-status",
            ),
        )
        self.assertFalse(assessment.claim_releasable)
        self.assertEqual(("n300",), assessment.unresolved_resources)

    def test_proven_on_is_not_clean(self) -> None:
        assessment = release_assessment(
            ue=CleanupEvidence(
                resource="qfit07",
                stage="ue-power-off-release",
                state=CleanupState.PROVEN_ON,
                source="provider-status",
            ),
            radio=CleanupEvidence(
                resource="n300",
                stage="radio-power-off-release",
                state=CleanupState.PROVEN_OFF,
                source="pdu-status",
            ),
        )
        self.assertFalse(assessment.claim_releasable)
        self.assertEqual(("qfit07",), assessment.unresolved_resources)

    def test_serialized_assessment_contains_only_sanitized_state(self) -> None:
        assessment = release_assessment(
            ue=CleanupEvidence(
                resource="qfit07",
                stage="ue-power-off-release",
                state=CleanupState.UNKNOWN,
                source="timeout",
            ),
            radio=CleanupEvidence(
                resource="n300",
                stage="radio-power-off-release",
                state=CleanupState.PROVEN_OFF,
                source="pdu-status",
            ),
        )
        payload = assessment.to_dict()
        self.assertFalse(payload["claim_releasable"])
        self.assertEqual(["qfit07"], payload["unresolved_resources"])
        self.assertEqual(2, len(payload["evidence"]))


if __name__ == "__main__":
    unittest.main()
