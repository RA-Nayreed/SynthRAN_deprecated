from __future__ import annotations

from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from synthran.r2lab.acceptance import (
    AcceptanceOutcome,
    PhysicalAcceptanceStage,
)
from synthran.r2lab.lifecycle import (
    continue_physical_path,
    run_physical_workload,
)


class PhysicalPathCompositionTests(unittest.TestCase):
    @patch("synthran.r2lab.lifecycle._write_json")
    @patch("synthran.r2lab.lifecycle.prove_user_plane")
    @patch("synthran.r2lab.lifecycle.activate_physical_ue")
    @patch("synthran.r2lab.lifecycle.PhysicalRunEvidence.read_json")
    def test_path_composes_activation_then_user_plane_without_digest_authority(
        self,
        read_evidence,
        activate,
        user_plane,
        write_json,
    ) -> None:
        initial = Mock()
        initial.acceptance.next_stage = PhysicalAcceptanceStage.UE_MANAGEMENT

        after_activation = Mock()
        after_activation.acceptance.next_stage = PhysicalAcceptanceStage.USER_PLANE
        after_activation.acceptance.failed_stage = None
        after_activation.acceptance.outcome_for.return_value = AcceptanceOutcome.NOT_REACHED
        activate.return_value = (
            after_activation,
            SimpleNamespace(status="activated"),
        )

        after_user_plane = Mock()
        after_user_plane.acceptance.next_stage = PhysicalAcceptanceStage.WORKLOAD
        after_user_plane.acceptance.failed_stage = None
        probe = SimpleNamespace(proven=True, to_dict=lambda: {"proven": True})
        user_plane.return_value = SimpleNamespace(
            evidence=after_user_plane,
            probe=probe,
        )
        read_evidence.return_value = initial

        run_root = Path(".synthran/r2lab")
        r2lab_runner = Mock()
        cluster_runner = Mock()
        summary = continue_physical_path(
            run_id="r2lab-run-001",
            slice_name="oulu_user",
            owner="rnayreed",
            allocation_id=None,
            known_hosts=Path("known_hosts"),
            peer="198.51.100.10",
            run_root=run_root,
            r2lab_runner=r2lab_runner,
            cluster_runner=cluster_runner,
        )

        self.assertTrue(summary.ready_for_workload)
        self.assertTrue(summary.user_plane_proven)
        self.assertEqual("activated", summary.activation_status)
        self.assertEqual("workload", summary.next_stage)
        self.assertNotIn("sha256", str(summary.to_dict()).lower())
        activate.assert_called_once()
        user_plane.assert_called_once()
        user_plane_kwargs = user_plane.call_args.kwargs
        self.assertIs(after_activation, user_plane_kwargs["evidence"])
        self.assertEqual("oulu_user", user_plane_kwargs["slice_name"])
        self.assertEqual("198.51.100.10", user_plane_kwargs["peer"])
        self.assertEqual(run_root, user_plane_kwargs["run_root"])
        self.assertIs(r2lab_runner, user_plane_kwargs["r2lab_runner"])
        self.assertIs(cluster_runner, user_plane_kwargs["cluster_runner"])
        after_user_plane.write_json.assert_called_once_with(
            run_root.resolve() / "r2lab-run-001" / "physical-run.json"
        )
        write_json.assert_called_once()

    @patch("synthran.r2lab.lifecycle.activate_physical_ue")
    @patch("synthran.r2lab.lifecycle.PhysicalRunEvidence.read_json")
    def test_failed_physical_stage_returns_not_ready_instead_of_skipping(
        self,
        read_evidence,
        activate,
    ) -> None:
        initial = Mock()
        initial.acceptance.next_stage = PhysicalAcceptanceStage.CELL_ACQUISITION
        failed = Mock()
        failed.acceptance.next_stage = None
        failed.acceptance.failed_stage = PhysicalAcceptanceStage.CELL_ACQUISITION
        failed.acceptance.outcome_for.return_value = AcceptanceOutcome.NOT_REACHED
        activate.return_value = (
            failed,
            SimpleNamespace(status="not-proven"),
        )
        read_evidence.return_value = initial

        summary = continue_physical_path(
            run_id="r2lab-run-001",
            slice_name="oulu_user",
            owner="rnayreed",
            allocation_id=None,
            known_hosts=Path("known_hosts"),
            peer="198.51.100.10",
            r2lab_runner=Mock(),
            cluster_runner=Mock(),
        )

        self.assertFalse(summary.ready_for_workload)
        self.assertEqual("cell-acquisition", summary.failed_stage)


class PhysicalWorkloadCompositionTests(unittest.TestCase):
    @patch("synthran.r2lab.lifecycle.execute_physical_workload_handoff")
    @patch("synthran.r2lab.lifecycle.build_physical_workload_executor")
    @patch("synthran.r2lab.lifecycle.repository_root", return_value=Path("."))
    @patch("synthran.r2lab.lifecycle.load_lock", return_value=object())
    @patch("synthran.r2lab.lifecycle.load_physical_inventory", return_value=object())
    @patch("synthran.r2lab.lifecycle.load_topology")
    @patch("synthran.r2lab.lifecycle.PhysicalRunEvidence.read_json")
    def test_workload_reuses_physical_executor_and_common_result_semantics(
        self,
        read_evidence,
        load_topology,
        load_physical_inventory,
        load_lock,
        repo_root,
        build_executor,
        handoff,
    ) -> None:
        evidence = Mock()
        evidence.acceptance.next_stage = PhysicalAcceptanceStage.WORKLOAD
        read_evidence.return_value = evidence
        topology = Mock()
        topology.validate.return_value = topology
        load_topology.return_value = topology
        executor = Mock()
        build_executor.return_value = executor

        completed = Mock()
        completed.acceptance.accepted = True
        handoff.return_value = (
            completed,
            SimpleNamespace(accepted=True, cleanup_proven=True),
        )

        with tempfile.TemporaryDirectory() as directory:
            known_hosts = Path(directory) / "known_hosts"
            known_hosts.write_text("test-host-key\n", encoding="utf-8")
            summary = run_physical_workload(
                run_id="r2lab-run-001",
                workload_id="physical-iot-001",
                slice_name="oulu_user",
                owner="rnayreed",
                allocation_id=None,
                known_hosts=known_hosts,
                inventory_path=Path("hosts.ini"),
                r2lab_runner=Mock(),
                cluster_runner=Mock(),
            )

        self.assertTrue(summary.accepted)
        self.assertTrue(summary.cleanup_proven)
        payload = summary.to_dict()
        self.assertEqual(
            {
                "workload": True,
                "data": True,
                "acceptance": True,
                "workload_cleanup": True,
            },
            payload["stages"],
        )
        self.assertNotIn("sha256", str(payload).lower())
        load_topology.assert_called_once_with(
            run_root=Path(".synthran/r2lab"), run_id="r2lab-run-001"
        )
        load_physical_inventory.assert_called_once_with(Path("hosts.ini"), topology=topology)
        build_executor.assert_called_once()
        handoff.assert_called_once()


if __name__ == "__main__":
    unittest.main()
