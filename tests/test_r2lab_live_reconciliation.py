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
from synthran.r2lab.reconciliation import (
    R2LabLiveReconciliationError,
    _require_exact_deployment,
    _synthetic_gnb_boundary,
)


RUN_ID = "r2lab-run-001"
PACKAGE = "1" * 64
VALUES = "2" * 64
RENDER = "3" * 64
STAGING = "4" * 64
CLAIM = "5" * 64
START = "6" * 64


def _artifact():
    class Artifact:
        package_sha256 = PACKAGE
        values_sha256 = VALUES
        render_sha256 = RENDER

    return Artifact()


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


class LiveResumeBindingTests(unittest.TestCase):
    def test_exact_deployment_accepts_only_bound_zero_or_singleton(self) -> None:
        artifact = _artifact()
        payload = {
            "metadata": {
                "labels": {"synthran.run/id": RUN_ID},
                "annotations": {
                    "synthran.io/run-id": RUN_ID,
                    "synthran.io/package-sha256": PACKAGE,
                    "synthran.io/values-sha256": VALUES,
                    "synthran.io/render-sha256": RENDER,
                },
            },
            "spec": {"replicas": 0},
        }
        self.assertEqual(
            0,
            _require_exact_deployment(payload=payload, run_id=RUN_ID, artifact=artifact),
        )
        payload["spec"]["replicas"] = 1
        self.assertEqual(
            1,
            _require_exact_deployment(payload=payload, run_id=RUN_ID, artifact=artifact),
        )

    def test_exact_deployment_rejects_foreign_owner(self) -> None:
        payload = {
            "metadata": {
                "labels": {"synthran.run/id": "other-run-001"},
                "annotations": {
                    "synthran.io/run-id": RUN_ID,
                    "synthran.io/package-sha256": PACKAGE,
                    "synthran.io/values-sha256": VALUES,
                    "synthran.io/render-sha256": RENDER,
                },
            },
            "spec": {"replicas": 0},
        }
        with self.assertRaisesRegex(R2LabLiveReconciliationError, "not owned"):
            _require_exact_deployment(payload=payload, run_id=RUN_ID, artifact=_artifact())

    def test_exact_deployment_rejects_changed_artifact_binding(self) -> None:
        payload = {
            "metadata": {
                "labels": {"synthran.run/id": RUN_ID},
                "annotations": {
                    "synthran.io/run-id": RUN_ID,
                    "synthran.io/package-sha256": "9" * 64,
                    "synthran.io/values-sha256": VALUES,
                    "synthran.io/render-sha256": RENDER,
                },
            },
            "spec": {"replicas": 0},
        }
        with self.assertRaisesRegex(R2LabLiveReconciliationError, "immutable artifact"):
            _require_exact_deployment(payload=payload, run_id=RUN_ID, artifact=_artifact())

    def test_exact_deployment_rejects_invalid_replica_state(self) -> None:
        payload = {
            "metadata": {
                "labels": {"synthran.run/id": RUN_ID},
                "annotations": {
                    "synthran.io/run-id": RUN_ID,
                    "synthran.io/package-sha256": PACKAGE,
                    "synthran.io/values-sha256": VALUES,
                    "synthran.io/render-sha256": RENDER,
                },
            },
            "spec": {"replicas": 2},
        }
        with self.assertRaisesRegex(R2LabLiveReconciliationError, "replica state"):
            _require_exact_deployment(payload=payload, run_id=RUN_ID, artifact=_artifact())


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

    def test_unified_run_places_live_reproof_before_historical_shortcuts(self) -> None:
        source = Path("synthran/backends/run.py").read_text(encoding="utf-8")
        live = source.index("reconcile_live_resume(")
        foundation_shortcut = source.index("accepted foundation evidence present; current state re-proven")
        workload = source.index("run deterministic ten-sensor experiment and collect data")
        self.assertLess(live, foundation_shortcut)
        self.assertLess(live, workload)


if __name__ == "__main__":
    unittest.main()
