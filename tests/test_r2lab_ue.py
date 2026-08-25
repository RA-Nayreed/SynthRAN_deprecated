from __future__ import annotations

import unittest

from synthran.r2lab.acceptance import (
    PhysicalAcceptanceStage,
    PhysicalRunEvidence,
    STAGE_ORDER,
)
from synthran.r2lab.radio import (
    CellAcquisitionState,
    Ipv4State,
    PacketServiceState,
    RegistrationState,
)
from synthran.r2lab.ue import (
    PhysicalUeRuntimeEvidence,
    PhysicalWorkloadContext,
    PhysicalWorkloadResult,
    R2LabPhysicalUeError,
    _qmi_pid_path,
    _record_runtime,
    _serial,
)


RUN_ID = "r2lab-ue-test"


def evidence_at_cell() -> PhysicalRunEvidence:
    evidence = PhysicalRunEvidence(run_id=RUN_ID)
    for stage in STAGE_ORDER[:6]:
        evidence = evidence.pass_stage(stage, source=f"test-{stage.value}")
    return evidence


class PhysicalUeRuntimeTests(unittest.TestCase):
    def test_mbim_runtime_requires_cell_registration_packet_and_ipv4(self) -> None:
        runtime = PhysicalUeRuntimeEvidence(
            ue="qhat10",
            mode="mbim",
            interface="wwan0",
            cell=CellAcquisitionState.ACQUIRED_NR_SA,
            registration=RegistrationState.REGISTERED,
            packet_service=PacketServiceState.ATTACHED,
            ipv4=Ipv4State.PRESENT,
            manager_running=True,
            transport_error=False,
        )
        self.assertTrue(runtime.cell_acquired)
        self.assertTrue(runtime.registered)
        self.assertTrue(runtime.pdu_session_established)
        self.assertEqual("qhat10", runtime.to_dict()["ue"])

    def test_qmi_runtime_uses_the_same_pdu_postcondition(self) -> None:
        runtime = PhysicalUeRuntimeEvidence(
            ue="qhat23",
            mode="qmi",
            interface="wwan0",
            cell=CellAcquisitionState.ACQUIRED_NR_SA,
            registration=RegistrationState.REGISTERED,
            packet_service=PacketServiceState.ATTACHED,
            ipv4=Ipv4State.PRESENT,
            manager_running=True,
            transport_error=False,
        )
        self.assertTrue(runtime.pdu_session_established)
        payload = runtime.to_dict()
        self.assertEqual("qmi", payload["mode"])
        self.assertEqual("wwan0", payload["interface"])

    def test_ordered_runtime_records_cell_registration_and_pdu(self) -> None:
        runtime = PhysicalUeRuntimeEvidence(
            ue="qhat23",
            mode="qmi",
            interface="wwan0",
            cell=CellAcquisitionState.ACQUIRED_NR_SA,
            registration=RegistrationState.REGISTERED,
            packet_service=PacketServiceState.ATTACHED,
            ipv4=Ipv4State.PRESENT,
            manager_running=True,
            transport_error=False,
        )
        completed = _record_runtime(
            evidence_at_cell(),
            runtime,
            source="current-qhat23",
        )
        self.assertEqual(
            PhysicalAcceptanceStage.USER_PLANE,
            completed.acceptance.next_stage,
        )

    def test_failed_cell_blocks_later_physical_stages(self) -> None:
        runtime = PhysicalUeRuntimeEvidence(
            ue="qfit07",
            mode="mbim",
            interface="wwan0",
            cell=CellAcquisitionState.NO_SERVICE,
            registration=RegistrationState.NOT_REGISTERED,
            packet_service=PacketServiceState.DETACHED,
            ipv4=Ipv4State.ABSENT,
            manager_running=True,
            transport_error=False,
        )
        failed = _record_runtime(
            evidence_at_cell(),
            runtime,
            source="current-qfit07",
        )
        self.assertEqual(
            PhysicalAcceptanceStage.CELL_ACQUISITION,
            failed.acceptance.failed_stage,
        )
        self.assertIsNone(failed.acceptance.next_stage)


class PhysicalQmiOwnershipTests(unittest.TestCase):
    def test_qmi_process_ownership_is_bound_to_one_run_id(self) -> None:
        self.assertEqual(
            "/run/synthran/r2lab-ue-test.qmi.pid",
            _qmi_pid_path(RUN_ID),
        )
        with self.assertRaises(R2LabPhysicalUeError):
            _qmi_pid_path("../other-run")

    def test_at_probe_allow_list_contains_only_runtime_state_queries(self) -> None:
        self.assertEqual("AT+QNWINFO", _serial("AT+QNWINFO")[-1])
        self.assertEqual("AT+C5GREG?", _serial("AT+C5GREG?")[-1])
        with self.assertRaises(R2LabPhysicalUeError):
            _serial("AT+CIMI")


class PhysicalWorkloadContextTests(unittest.TestCase):
    def test_context_is_generic_across_qfit_and_qhat_and_has_no_hash_authority(self) -> None:
        for ue in ("qfit07", "qhat10", "qhat23"):
            context = PhysicalWorkloadContext(
                run_id=RUN_ID,
                ue=ue,
                interface="wwan0",
            )
            payload = context.to_dict()
            self.assertEqual(ue, payload["ue"])
            self.assertNotIn("sha", str(payload).lower())

    def test_workload_result_keeps_only_evidence_digest_as_provenance(self) -> None:
        result = PhysicalWorkloadResult(
            run_id=RUN_ID,
            workload_id="physical-iot-001",
            backend="r2lab",
            interface="wwan0",
            evidence_sha256="a" * 64,
            accepted=True,
            cleanup_proven=True,
        )
        self.assertTrue(result.accepted)
        self.assertEqual("a" * 64, result.to_dict()["evidence_sha256"])

    def test_workload_context_rejects_virtual_interface(self) -> None:
        with self.assertRaises(R2LabPhysicalUeError):
            PhysicalWorkloadContext(
                run_id=RUN_ID,
                ue="qhat23",
                interface="tun_srsue1",
            )


if __name__ == "__main__":
    unittest.main()
