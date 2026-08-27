from __future__ import annotations

from pathlib import Path
import unittest

from synthran.r2lab.acceptance import (
    PhysicalAcceptance,
    PhysicalAcceptanceStage,
    PhysicalRunEvidence,
    StagedPhysicalEvidence,
    StartedGnbEvidence,
)
from synthran.r2lab.reconciliation import LiveResumeResult, _synthetic_gnb_boundary


RUN_ID = "r2lab-run-001"
PACKAGE = "1" * 64
VALUES = "2" * 64
RENDER = "3" * 64
STAGING = "4" * 64
CLAIM = "5" * 64
START = "6" * 64


def _historical_path() -> PhysicalRunEvidence:
    staged = StagedPhysicalEvidence(
        run_id=RUN_ID,
        package_sha256=PACKAGE,
        values_sha256=VALUES,
        render_sha256=RENDER,
        staging_sha256=STAGING,
    )
    started = StartedGnbEvidence(
        run_id=RUN_ID,
        package_sha256=PACKAGE,
        values_sha256=VALUES,
        render_sha256=RENDER,
        claim_sha256=CLAIM,
        start_sha256=START,
    )
    acceptance = PhysicalAcceptance()
    for stage in (
        PhysicalAcceptanceStage.RESOURCE_AUTHORITY,
        PhysicalAcceptanceStage.SLICES_FOUNDATION,
        PhysicalAcceptanceStage.KUBERNETES,
        PhysicalAcceptanceStage.OPEN5GS,
        PhysicalAcceptanceStage.GNB_N2,
        PhysicalAcceptanceStage.UE_MANAGEMENT,
        PhysicalAcceptanceStage.CELL_ACQUISITION,
        PhysicalAcceptanceStage.REGISTRATION,
        PhysicalAcceptanceStage.PDU_SESSION,
        PhysicalAcceptanceStage.USER_PLANE,
    ):
        acceptance = acceptance.pass_stage(stage, source=f"historical-{stage.value}")
    return PhysicalRunEvidence(
        run_id=RUN_ID,
        staged=staged,
        gnb_start=started,
        acceptance=acceptance,
    )


class LiveResumeEvidenceTests(unittest.TestCase):
    def test_live_ue_reproof_uses_synthetic_boundary_without_rewriting_history(self) -> None:
        historical = _historical_path()
        synthetic = _synthetic_gnb_boundary(historical)

        self.assertEqual(
            PhysicalAcceptanceStage.UE_MANAGEMENT,
            synthetic.acceptance.next_stage,
        )
        self.assertEqual(historical.staged, synthetic.staged)
        self.assertEqual(historical.gnb_start, synthetic.gnb_start)
        self.assertEqual(
            PhysicalAcceptanceStage.WORKLOAD,
            historical.acceptance.next_stage,
        )
        self.assertEqual(5, len(synthetic.acceptance.evidence))
        self.assertEqual(10, len(historical.acceptance.evidence))

    def test_resume_result_serializes_separate_live_evidence(self) -> None:
        result = LiveResumeResult(
            run_id=RUN_ID,
            allocation_id="allocation-001",
            foundation_reconciled=True,
            gnb_restarted=True,
            ue_status="connected",
            user_plane_proven=True,
            evidence_path=Path("live-resume.json"),
        )
        payload = result.to_dict()
        self.assertEqual("synthran/r2lab-live-resume/v1alpha1", payload["schema"])
        self.assertTrue(payload["user_plane_proven"])
        self.assertEqual("live-resume.json", payload["evidence_path"])

    def test_unified_run_places_live_reproof_before_historical_shortcuts(self) -> None:
        source = Path("synthran/backends/run.py").read_text(encoding="utf-8")
        live = source.index("reconcile_live_resume(")
        foundation_shortcut = source.index(
            "accepted foundation evidence present; current state re-proven"
        )
        workload = source.index("run deterministic ten-sensor experiment and collect data")
        self.assertLess(live, foundation_shortcut)
        self.assertLess(live, workload)


if __name__ == "__main__":
    unittest.main()
