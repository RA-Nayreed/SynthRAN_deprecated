from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from synthran.r2lab.acceptance import (
    PhysicalAcceptance,
    PhysicalAcceptanceStage,
    PhysicalRunEvidence,
    STAGE_ORDER,
)
from synthran.r2lab.workload_retry import (
    R2LabWorkloadRetryError,
    next_workload_attempt_id,
    recover_failed_workload,
)


RUN_ID = "r2lab-run-001"


def failed_workload_evidence() -> PhysicalRunEvidence:
    acceptance = PhysicalAcceptance()
    for stage in STAGE_ORDER[:-1]:
        acceptance = acceptance.pass_stage(stage, source=f"passed-{stage.value}")
    acceptance = acceptance.fail_stage(
        PhysicalAcceptanceStage.WORKLOAD,
        source=f"physical-iot:{RUN_ID}:not-proven",
    )
    return PhysicalRunEvidence(run_id=RUN_ID, acceptance=acceptance)


def write_result(root: Path, *, cleanup_proven: bool) -> Path:
    path = root / RUN_ID / "physical" / "physical-workload-result.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "run_id": RUN_ID,
                "workload_id": RUN_ID,
                "backend": "r2lab",
                "interface": "wwan0",
                "evidence_sha256": "a" * 64,
                "accepted": False,
                "cleanup_proven": cleanup_proven,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    return path


class PhysicalWorkloadRetryTests(unittest.TestCase):
    def test_cleanup_proven_failed_workload_reopens_only_terminal_stage(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "r2lab"
            experiments = Path(directory) / "experiments"
            write_result(root, cleanup_proven=True)
            (experiments / RUN_ID).mkdir(parents=True)

            original = failed_workload_evidence()
            recovered, changed = recover_failed_workload(
                evidence=original,
                run_root=root,
            )

            self.assertTrue(changed)
            self.assertIsNone(recovered.acceptance.failed_stage)
            self.assertIs(
                PhysicalAcceptanceStage.WORKLOAD,
                recovered.acceptance.next_stage,
            )
            self.assertEqual(
                original.acceptance.evidence[:-1],
                recovered.acceptance.evidence,
            )

            retry_path = root / RUN_ID / "physical" / "workload-retries" / "retry-001.json"
            retry = json.loads(retry_path.read_text(encoding="utf-8"))
            self.assertEqual(RUN_ID, retry["physical_run_id"])
            self.assertEqual(RUN_ID, retry["previous_workload_id"])
            self.assertTrue(retry["previous_cleanup_proven"])

            persisted = PhysicalRunEvidence.read_json(root / RUN_ID / "physical-run.json")
            self.assertIs(PhysicalAcceptanceStage.WORKLOAD, persisted.acceptance.next_stage)
            self.assertEqual(
                f"{RUN_ID}-w2",
                next_workload_attempt_id(
                    RUN_ID,
                    physical_run_id=RUN_ID,
                    physical_run_root=root,
                    experiment_root=experiments,
                ),
            )

    def test_cleanup_unproven_failure_remains_blocked(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "r2lab"
            write_result(root, cleanup_proven=False)

            with self.assertRaisesRegex(R2LabWorkloadRetryError, "cleanup was not proven"):
                recover_failed_workload(
                    evidence=failed_workload_evidence(),
                    run_root=root,
                )

            self.assertFalse(
                (root / RUN_ID / "physical" / "workload-retries" / "retry-001.json").exists()
            )

    def test_existing_workload_directory_requires_retry_authorization(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "r2lab"
            experiments = Path(directory) / "experiments"
            (experiments / RUN_ID).mkdir(parents=True)

            with self.assertRaisesRegex(R2LabWorkloadRetryError, "without cleanup-proven"):
                next_workload_attempt_id(
                    RUN_ID,
                    physical_run_id=RUN_ID,
                    physical_run_root=root,
                    experiment_root=experiments,
                )


if __name__ == "__main__":
    unittest.main()
