from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from synthran.live_preflight import CommandResult
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


def write_topology(root: Path) -> None:
    path = root / RUN_ID / "topology.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "schema": "synthran/r2lab-topology/v1alpha1",
                "core_node": "sopnode-f2",
                "ran_node": "sopnode-f3",
                "radio": "n300",
                "ue": "qfit07",
                "dnn": "internet",
            }
        )
        + "\n",
        encoding="utf-8",
    )


def write_attempt(experiments: Path, *, later_log: bool = False) -> Path:
    directory = experiments / RUN_ID
    logs = directory / "logs"
    logs.mkdir(parents=True, exist_ok=True)
    (directory / "manifest.json").write_text(
        json.dumps(
            {
                "run_id": RUN_ID,
                "physical_run_id": RUN_ID,
                "backend": "r2lab",
                "status": "failed",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    if later_log:
        (logs / "cooja.log").write_text("started\n", encoding="utf-8")
    return directory


class CleanupRunner:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def __call__(self, command, timeout: int) -> CommandResult:
        rendered = " ".join(command)
        self.calls.append(rendered)
        if "kubectl get deployment,configmap" in rendered:
            return CommandResult(0, "", "")
        if "python3 -c" in rendered:
            return CommandResult(
                0,
                json.dumps({"tun_exists": False, "busy_ports": []}) + "\n",
                "",
            )
        return CommandResult(0, "", "")


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
            self.assertFalse(retry["cleanup_recovered_live"])

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

    def test_cleanup_unproven_early_failure_is_reproved_live_before_retry(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "r2lab"
            experiments = Path(directory) / "experiments"
            result_path = write_result(root, cleanup_proven=False)
            write_topology(root)
            write_attempt(experiments)
            runner = CleanupRunner()

            recovered, changed = recover_failed_workload(
                evidence=failed_workload_evidence(),
                run_root=root,
                experiment_root=experiments,
                cluster_runner=runner,
            )

            self.assertTrue(changed)
            self.assertIs(PhysicalAcceptanceStage.WORKLOAD, recovered.acceptance.next_stage)
            historical = json.loads(result_path.read_text(encoding="utf-8"))
            self.assertFalse(historical["cleanup_proven"])

            retry_path = root / RUN_ID / "physical" / "workload-retries" / "retry-001.json"
            retry = json.loads(retry_path.read_text(encoding="utf-8"))
            self.assertFalse(retry["previous_cleanup_proven"])
            self.assertTrue(retry["cleanup_recovered_live"])
            recovery_path = Path(retry["cleanup_recovery_path"])
            self.assertTrue(recovery_path.is_file())
            recovery = json.loads(recovery_path.read_text(encoding="utf-8"))
            self.assertTrue(recovery["cleanup_proven"])
            self.assertEqual("central-broker-stage-only", recovery["scope"])
            self.assertTrue(any("kubectl delete deployment,configmap" in call for call in runner.calls))
            self.assertTrue(any("ssh root@sopnode-f2" in call for call in runner.calls))
            self.assertFalse(any("-F /dev/null" in call for call in runner.calls))
            self.assertEqual(
                f"{RUN_ID}-w2",
                next_workload_attempt_id(
                    RUN_ID,
                    physical_run_id=RUN_ID,
                    physical_run_root=root,
                    experiment_root=experiments,
                ),
            )

    def test_cleanup_unproven_later_stage_failure_remains_blocked(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "r2lab"
            experiments = Path(directory) / "experiments"
            write_result(root, cleanup_proven=False)
            write_topology(root)
            write_attempt(experiments, later_log=True)
            runner = CleanupRunner()

            with self.assertRaisesRegex(R2LabWorkloadRetryError, "later runtime stage"):
                recover_failed_workload(
                    evidence=failed_workload_evidence(),
                    run_root=root,
                    experiment_root=experiments,
                    cluster_runner=runner,
                )

            self.assertEqual([], runner.calls)
            self.assertFalse(
                (root / RUN_ID / "physical" / "workload-retries" / "retry-001.json").exists()
            )

    def test_cleanup_unproven_failure_without_attempt_evidence_remains_blocked(self) -> None:
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
