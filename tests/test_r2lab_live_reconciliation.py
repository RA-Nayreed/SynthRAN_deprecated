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

        self.assertEqual(PhysicalAcceptanceStage.UE_MANAGEMENT, synthetic.acceptance.next_stage)
        self.assertEqual(historical.staged, synthetic.staged)
        self.assertEqual(historical.gnb_start, synthetic.gnb_start)
        self.assertEqual(PhysicalAcceptanceStage.WORKLOAD, historical.acceptance.next_stage)
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

    def test_resume_converges_ephemeral_layers_before_historical_shortcuts(self) -> None:
        source = Path("synthran/r2lab/reconciliation.py").read_text(encoding="utf-8")
        lifecycle_source = Path("synthran/lifecycle.py").read_text(encoding="utf-8")

        self.assertIn("converge_kubernetes_foundation(", source)
        self.assertIn("converge_physical_gnb(", source)
        self.assertIn("KUBERNETES_OBSERVATION_ATTEMPTS = 3", source)
        self.assertIn("_observe_ready_nodes(", source)
        self.assertNotIn("_replay_gnb", source)
        self.assertNotIn("physical-render.yaml", source)
        live = lifecycle_source.index("reconcile_live_resume(")
        workload = lifecycle_source.index(
            'progress.start("workload", "deterministic AMBER source and PDU-bound transport")'
        )
        self.assertLess(live, workload)

    def test_physical_gnb_uses_upstream_srsran_config_and_deploy_role(self) -> None:
        playbook = Path("deploy/ansible/r2lab-srsran-gnb.yml").read_text(encoding="utf-8")
        self.assertIn("name: 5g/srsRAN/config", playbook)
        self.assertIn("name: 5g/srsRAN/deploy", playbook)
        self.assertIn("tasks_from: deploy_gnb.yml", playbook)
        self.assertIn("existing physical gNB Deployment is not owned by this run", playbook)
        self.assertIn("synthran.io/deployment-authority", playbook)

    def test_foundation_is_dedicated_and_stops_before_5g_roles(self) -> None:
        playbook = Path("deploy/ansible/r2lab-foundation.yml").read_text(encoding="utf-8")
        source = Path("synthran/r2lab/foundation_convergence.py").read_text(encoding="utf-8")

        self.assertIn('str(overlay_directory / "r2lab-foundation.yml")', source)
        self.assertNotIn('str(worktree / "playbooks" / "deploy.yml")', source)
        self.assertIn("role: setup/k8s/cluster_create", playbook)
        self.assertIn("role: setup/k8s/cluster_join", playbook)
        self.assertIn("role: setup/cni", playbook)
        self.assertNotIn("5g/open5gs", playbook)
        self.assertNotIn("5g/srsRAN", playbook)
        self.assertNotIn("ueransim", playbook.lower())

    def test_pos_is_only_used_for_nodes_upstream_ansible_cannot_reach(self) -> None:
        source = Path("synthran/r2lab/foundation_convergence.py").read_text(encoding="utf-8")

        reachability = source.index("_ansible_reachable(")
        unreachable = source.index("unreachable = [")
        pos_loop = source.index("for node in unreachable:")
        cluster = source.index('(\"foundation-cluster\", cluster_command)')
        self.assertLess(reachability, unreachable)
        self.assertLess(unreachable, pos_loop)
        self.assertLess(pos_loop, cluster)
        self.assertIn("foundation-pos: skipped; selected sopnodes already reachable through upstream Ansible", source)
        self.assertIn("ansible.builtin.ping", source)

    def test_active_live_cluster_uses_normal_openssh_without_synthran_ssh_policy(self) -> None:
        source = Path("synthran/r2lab/live_cluster.py").read_text(encoding="utf-8")
        reconciliation = Path("synthran/r2lab/reconciliation.py").read_text(encoding="utf-8")

        self.assertIn('return ("ssh", f"root@{topology.core_node}", shlex.join(remote))', source)
        for forbidden in (
            "strict_ssh_command",
            "ansible_ssh_common_args",
            "isolated_config",
            "ssh-keyscan",
            "ANSIBLE_SSH_ARGS",
            "ANSIBLE_HOST_KEY_CHECKING",
            "-F /dev/null",
            "UserKnownHostsFile",
            "StrictHostKeyChecking",
        ):
            self.assertNotIn(forbidden, source)
        self.assertNotIn("foundation_topology", reconciliation)
        self.assertNotIn("verify_current_n3xx_n2", reconciliation)
        self.assertNotIn("prove_physical_user_plane", reconciliation)


if __name__ == "__main__":
    unittest.main()
