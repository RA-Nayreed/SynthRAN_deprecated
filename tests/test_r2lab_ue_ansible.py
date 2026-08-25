from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from synthran.live_preflight import CommandResult
from synthran.r2lab.acceptance import PhysicalAcceptanceStage, PhysicalRunEvidence, STAGE_ORDER
from synthran.r2lab.hardware import PhysicalTopology
from synthran.r2lab.radio import (
    CellAcquisitionState,
    Ipv4State,
    PacketServiceState,
    RegistrationState,
)
from synthran.r2lab.ue import PhysicalUeRuntimeEvidence
from synthran.r2lab.ue_activation import _pass_functional_path, recover_retryable_transport_failure
from synthran.r2lab.ue_ansible import (
    CONNECT_ROLE,
    OPEN5GS_UPF_ADDRESS,
    SETUP_ROLE,
    STOP_ROLE,
    R2LabUeAnsibleError,
    _inventory,
    _playbook,
    _profile,
    execute_selected_ue_role,
)


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
RUN_ID = "r2lab-ue-role-test"
QFIT = PhysicalTopology(
    core_node="sopnode-f2",
    ran_node="sopnode-f3",
    radio="n300",
    ue="qfit07",
).validate()
QHAT = PhysicalTopology(
    core_node="sopnode-f2",
    ran_node="sopnode-f3",
    radio="n300",
    ue="qhat01",
).validate()


class SelectedUeRoleRenderingTests(unittest.TestCase):
    def test_qfit_connect_maps_to_real_runtime_host_and_literal_faraday(self) -> None:
        inventory = _inventory(slice_name="oulu_user", topology=QFIT, action="connect")
        self.assertIn("faraday.inria.fr ansible_user=oulu_user", inventory)
        self.assertNotIn("faraday ansible_host=faraday.inria.fr", inventory)
        self.assertIn("fit07 mode=mbim", inventory)
        self.assertNotIn("qfit09", inventory)
        self.assertIn("StrictHostKeyChecking=yes", inventory)

    def test_qhat_setup_uses_pinned_setup_group_contract(self) -> None:
        inventory = _inventory(slice_name="oulu_user", topology=QHAT, action="setup")
        playbook = _playbook(action="setup", topology=QHAT)
        self.assertIn("[qhats]", inventory)
        self.assertIn("qhat01 mode=mbim", inventory)
        self.assertIn("role: r2lab/ue/setup", playbook)
        self.assertIn("fiveg_profile: synthran", playbook)
        self.assertIn("rru: n300", playbook)

    def test_qfit_setup_fails_closed_instead_of_misrouting_resource_name(self) -> None:
        with self.assertRaises(R2LabUeAnsibleError):
            _inventory(slice_name="oulu_user", topology=QFIT, action="setup")

    def test_generated_profile_is_minimal_and_contains_no_subscriber_secret(self) -> None:
        profile = _profile(QFIT, "connect")
        self.assertIn("dnn: internet", profile)
        self.assertIn('ip_prefix: "12.1.1"', profile)
        self.assertIn("fit07:", profile)
        for forbidden in ("imsi", "opc", "full_key", "security"):
            self.assertNotIn(forbidden, profile.lower())

    def test_connect_stop_playbooks_contain_no_modem_implementation(self) -> None:
        rendered = _playbook(action="connect", topology=QFIT) + _playbook(
            action="stop", topology=QFIT
        )
        self.assertIn("role: r2lab/ue/connect", rendered)
        self.assertIn("role: r2lab/ue/stop", rendered)
        self.assertNotIn("mbimcli", rendered)
        self.assertNotIn("quectel-CM", rendered)
        self.assertNotIn("ci_ctl_qtel.py", rendered)

    def test_synthran_has_no_custom_ue_actuator(self) -> None:
        source = "\n".join(
            (REPOSITORY_ROOT / path).read_text(encoding="utf-8")
            for path in ("synthran/r2lab/ue.py", "synthran/r2lab/resources.py")
        )
        for forbidden in (
            "AT+QNWINFO",
            "AT+C5GREG?",
            "--set-radio-state=on",
            "--attach-packet-service",
            "--connect=session-id=0",
            "nohup quectel-CM",
            "ci_ctl_qtel.py",
            '"config-ue"',
            'remote=("init.sh",)',
            "QFIT_INITIALIZER",
        ):
            self.assertNotIn(forbidden, source)

    def test_open5gs_functional_peer_is_the_pinned_default_upf(self) -> None:
        self.assertEqual("12.1.1.1", OPEN5GS_UPF_ADDRESS)


class SelectedUeRoleExecutionTests(unittest.TestCase):
    def test_executes_locked_connect_role_and_preserves_only_hashes(self) -> None:
        captured: dict[str, object] = {}

        def runner(command, cwd, environment, timeout_seconds):
            captured["command"] = tuple(command)
            captured["roles"] = environment["ANSIBLE_ROLES_PATH"]
            captured["timeout"] = timeout_seconds
            inventory_path = Path(command[2])
            playbook_path = Path(command[3])
            captured["inventory"] = inventory_path.read_text(encoding="utf-8")
            captured["playbook"] = playbook_path.read_text(encoding="utf-8")
            captured["profile"] = (
                playbook_path.parent.parent
                / "group_vars"
                / "all"
                / "5g_profile_synthran.yaml"
            ).read_text(encoding="utf-8")
            return CommandResult(0, "ok", "")

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            checkout = root / "locked" / "5g_ansible"
            profile_path = checkout / "group_vars" / "all" / "5g_profile_default.yaml"
            profile_path.parent.mkdir(parents=True)
            profile_path.write_text(
                "slices:\n"
                "  - name: slice1\n"
                "    dnn: internet\n"
                "    ip_prefix: \"12.1.1\"\n"
                "ues:\n"
                "  qfit07:\n"
                "    slice: slice1\n",
                encoding="utf-8",
            )
            for relative in (SETUP_ROLE, CONNECT_ROLE, STOP_ROLE):
                path = checkout / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("---\n", encoding="utf-8")

            with patch("synthran.r2lab.ue_ansible._configured_identity", return_value=None):
                result = execute_selected_ue_role(
                    run_id=RUN_ID,
                    slice_name="oulu_user",
                    topology=QFIT,
                    action="connect",
                    lock_path=REPOSITORY_ROOT / "dependencies.lock.yml",
                    deps_root=root / "ignored",
                    run_root=root / "runs",
                    timeout_seconds=30,
                    runner=runner,
                    checkout_validator=lambda lock, deps: checkout,
                )
            payload = json.loads(result.evidence_path.read_text(encoding="utf-8"))

        self.assertEqual("completed", result.status)
        self.assertEqual("ansible-playbook", captured["command"][0])
        self.assertEqual(str(checkout / "roles"), captured["roles"])
        self.assertGreaterEqual(captured["timeout"], 300)
        self.assertIn("faraday.inria.fr ansible_user=oulu_user", captured["inventory"])
        self.assertIn("fit07 mode=mbim", captured["inventory"])
        self.assertIn("role: r2lab/ue/connect", captured["playbook"])
        self.assertNotIn("imsi", captured["profile"].lower())
        self.assertEqual("completed", payload["status"])
        self.assertIn("inventory_sha256", payload)
        self.assertNotIn("oulu_user", json.dumps(payload))
        self.assertNotIn(str(checkout), json.dumps(payload))


class FunctionalAcceptanceTests(unittest.TestCase):
    @staticmethod
    def _state_at_ue() -> PhysicalRunEvidence:
        state = PhysicalRunEvidence(run_id=RUN_ID)
        for stage in STAGE_ORDER[:6]:
            state = state.pass_stage(stage, source=f"test-{stage.value}")
        return state

    def test_upf_proven_runtime_passes_cell_registration_and_pdu_in_order(self) -> None:
        runtime = PhysicalUeRuntimeEvidence(
            ue="qfit07",
            mode="mbim",
            interface="wwan0",
            cell=CellAcquisitionState.ACQUIRED_NR_SA,
            registration=RegistrationState.REGISTERED,
            packet_service=PacketServiceState.ATTACHED,
            ipv4=Ipv4State.PRESENT,
            manager_running=True,
            transport_error=False,
        )
        advanced = _pass_functional_path(self._state_at_ue(), runtime)
        self.assertIs(PhysicalAcceptanceStage.USER_PLANE, advanced.acceptance.next_stage)
        for stage in (
            PhysicalAcceptanceStage.CELL_ACQUISITION,
            PhysicalAcceptanceStage.REGISTRATION,
            PhysicalAcceptanceStage.PDU_SESSION,
        ):
            self.assertEqual("passed", advanced.acceptance.outcome_for(stage).value)

    def test_unproven_runtime_remains_retryable(self) -> None:
        runtime = PhysicalUeRuntimeEvidence(
            ue="qfit07",
            mode="mbim",
            interface="wwan0",
            cell=CellAcquisitionState.UNKNOWN,
            registration=RegistrationState.UNKNOWN,
            packet_service=PacketServiceState.ATTACHED,
            ipv4=Ipv4State.PRESENT,
            manager_running=True,
            transport_error=False,
        )
        unchanged = _pass_functional_path(self._state_at_ue(), runtime)
        self.assertIsNone(unchanged.acceptance.failed_stage)
        self.assertIs(PhysicalAcceptanceStage.CELL_ACQUISITION, unchanged.acceptance.next_stage)

    def test_old_transport_failure_can_be_migrated_for_same_run(self) -> None:
        failed = self._state_at_ue().fail_stage(
            PhysicalAcceptanceStage.CELL_ACQUISITION,
            source="old-transport-cell-unknown",
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "physical-ue-activation.json"
            path.write_text(
                json.dumps({"run_id": RUN_ID, "runtime": {"transport_error": True}}),
                encoding="utf-8",
            )
            repaired = recover_retryable_transport_failure(
                evidence=failed, activation_evidence_path=path
            )
        self.assertIsNone(repaired.acceptance.failed_stage)
        self.assertIs(PhysicalAcceptanceStage.CELL_ACQUISITION, repaired.acceptance.next_stage)


if __name__ == "__main__":
    unittest.main()
