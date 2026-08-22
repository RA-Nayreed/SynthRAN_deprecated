from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from synthran.live_preflight import CommandResult
from synthran.r2lab.acceptance import (
    AcceptanceOutcome,
    PhysicalAcceptanceStage,
    PhysicalRunEvidence,
    STAGE_ORDER,
)
from synthran.r2lab.deployment import PhysicalStartAuthority
from synthran.r2lab.radio import (
    CellAcquisitionState,
    Ipv4State,
    PacketServiceState,
    QfitRuntimeEvidence,
    RegistrationState,
    UserPlaneProbeEvidence,
)
from synthran.r2lab.runtime import GnbN2Evidence, N2State
from synthran.r2lab.ue import (
    PhysicalWorkloadResult,
    QfitActivationRequest,
    QfitActivationResult,
    R2LabQfitActivationError,
    SoftwareRadioState,
    execute_authorized_qfit_activation,
    execute_authorized_qfit_user_plane,
    execute_physical_workload_handoff,
    execute_qfit_activation,
    parse_mbim_radio_state,
    qfit_activation_commands,
)


RUN_ID = "r2lab-ue-test"
CLAIM = "d" * 64
PACKAGE = "a" * 64
VALUES = "b" * 64
RENDER = "c" * 64


def runtime_state(
    *,
    cell: CellAcquisitionState = CellAcquisitionState.ACQUIRED_NR_SA,
    registration: RegistrationState = RegistrationState.REGISTERED,
    packet: PacketServiceState = PacketServiceState.DETACHED,
    ipv4: Ipv4State = Ipv4State.ABSENT,
) -> QfitRuntimeEvidence:
    return QfitRuntimeEvidence(
        cell=cell,
        registration=registration,
        packet_service=packet,
        ipv4=ipv4,
    )


def staging_payload() -> dict[str, object]:
    return {
        "run_id": RUN_ID,
        "package_sha256": PACKAGE,
        "values_sha256": VALUES,
        "render_sha256": RENDER,
        "namespace_owned": True,
        "desired_replicas": 0,
        "gnb_pod_count": 0,
        "deployment_bound": True,
        "status": "staged-stopped",
        "hardware_mutation": False,
    }


def start_payload() -> dict[str, object]:
    return {
        "run_id": RUN_ID,
        "package_sha256": PACKAGE,
        "values_sha256": VALUES,
        "render_sha256": RENDER,
        "claim_sha256": CLAIM,
        "maximum_observed_pods": 1,
        "started_exactly_one": True,
        "status": "gnb-started",
        "hardware_mutation": True,
    }


def raw_started_evidence() -> PhysicalRunEvidence:
    return (
        PhysicalRunEvidence(run_id=RUN_ID)
        .bind_staging(staging_payload())
        .bind_gnb_start(start_payload())
    )


def base_evidence() -> PhysicalRunEvidence:
    """Activation begins only after lower foundation/core acceptance exists."""

    evidence = raw_started_evidence()
    for stage in STAGE_ORDER[:4]:
        evidence = evidence.pass_stage(stage, source=f"test-{stage.value}")
    return evidence


def pdu_evidence() -> PhysicalRunEvidence:
    evidence = base_evidence()
    for stage in STAGE_ORDER[4:9]:
        evidence = evidence.pass_stage(stage, source=f"test-{stage.value}")
    return evidence


def user_plane_evidence() -> PhysicalRunEvidence:
    return pdu_evidence().pass_stage(
        PhysicalAcceptanceStage.USER_PLANE,
        source="test-user-plane",
    )


def authority() -> PhysicalStartAuthority:
    return PhysicalStartAuthority(
        run_id=RUN_ID,
        radio="n300",
        ue="qfit07",
        ue_kind="qfit",
        claim_sha256=CLAIM,
        lease_verified=True,
        radio_state="on",
    )


def proven_gnb() -> GnbN2Evidence:
    return GnbN2Evidence(
        namespace_owned=True,
        deployment_bound=True,
        desired_replicas=1,
        pod_count=1,
        ready_running_count=1,
        n2_state=N2State.ESTABLISHED,
        log_observed=True,
        transport_error=False,
    )


class DirectQfitRunner:
    def __init__(
        self,
        *,
        attach_effective: bool = True,
        rollback_effective: bool = True,
        mutation_returncode: int = 0,
    ) -> None:
        self.radio_on = False
        self.attached = False
        self.ipv4 = False
        self.attach_effective = attach_effective
        self.rollback_effective = rollback_effective
        self.mutation_returncode = mutation_returncode
        self.commands: list[tuple[str, ...]] = []

    def __call__(self, command, timeout_seconds: int) -> CommandResult:
        value = tuple(command)
        self.commands.append(value)
        if "--query-radio-state" in value:
            state = "on" if self.radio_on else "off"
            return CommandResult(0, f"Software radio state: '{state}'\n", "")
        if "--set-radio-state=on" in value:
            self.radio_on = True
            return CommandResult(self.mutation_returncode, "", "")
        if "--attach-packet-service" in value:
            if self.attach_effective:
                self.attached = True
            return CommandResult(self.mutation_returncode, "", "")
        if any(item.startswith("--connect=") for item in value):
            return CommandResult(self.mutation_returncode, "", "")
        if value and value[0] == "mbim-set-ip.sh":
            if self.attached:
                self.ipv4 = True
            return CommandResult(self.mutation_returncode, "", "")
        if "--set-radio-state=off" in value:
            if self.rollback_effective:
                self.radio_on = False
                self.attached = False
                self.ipv4 = False
            return CommandResult(self.mutation_returncode, "", "")
        if value[:4] == ("ip", "link", "set", "dev"):
            return CommandResult(self.mutation_returncode, "", "")
        raise AssertionError(f"unexpected direct qfit command: {value}")

    def observe(self) -> QfitRuntimeEvidence:
        if not self.radio_on:
            return runtime_state(
                cell=CellAcquisitionState.NO_SERVICE,
                registration=RegistrationState.NOT_REGISTERED,
                packet=PacketServiceState.DETACHED,
                ipv4=Ipv4State.ABSENT,
            )
        return runtime_state(
            packet=(
                PacketServiceState.ATTACHED
                if self.attached
                else PacketServiceState.DETACHED
            ),
            ipv4=Ipv4State.PRESENT if self.ipv4 else Ipv4State.ABSENT,
        )


class R2LabQfitActivationCommandTests(unittest.TestCase):
    def test_radio_parser_is_fail_closed_on_missing_or_conflicting_state(self) -> None:
        self.assertEqual(
            SoftwareRadioState.ON,
            parse_mbim_radio_state("Software radio state: 'on'\n"),
        )
        self.assertEqual(
            SoftwareRadioState.OFF,
            parse_mbim_radio_state("Software radio state: 'off'\n"),
        )
        self.assertEqual(SoftwareRadioState.UNKNOWN, parse_mbim_radio_state(""))
        self.assertEqual(
            SoftwareRadioState.UNKNOWN,
            parse_mbim_radio_state(
                "Software radio state: 'on'\nSoftware radio state: 'off'\n"
            ),
        )

    def test_activation_command_set_is_exact_and_excludes_broad_helpers(self) -> None:
        commands = qfit_activation_commands(
            QfitActivationRequest(run_id=RUN_ID, qfit="qfit07")
        )
        rendered = "\n".join(" ".join(command) for command in commands.values())
        self.assertIn("apn=internet", rendered)
        self.assertIn("/dev/cdc-wdm0", rendered)
        self.assertIn("wwan0", rendered)
        for forbidden in (
            "start.sh",
            "stop.sh",
            "prepare-ue",
            "config-ue",
            "check-ue",
            "AT+CIMI",
            "disconnect",
        ):
            self.assertNotIn(forbidden, rendered)

    def test_activation_refuses_unreviewed_dnn_or_interface(self) -> None:
        with self.assertRaisesRegex(R2LabQfitActivationError, "internet DNN"):
            QfitActivationRequest(
                run_id=RUN_ID,
                qfit="qfit07",
                dnn="oai.ipv4",
            ).validate()
        with self.assertRaisesRegex(R2LabQfitActivationError, "wwan0"):
            QfitActivationRequest(
                run_id=RUN_ID,
                qfit="qfit07",
                interface="eth0",
            ).validate()


class R2LabQfitActivationExecutionTests(unittest.TestCase):
    def execute(self, runner: DirectQfitRunner) -> QfitActivationResult:
        return execute_qfit_activation(
            request=QfitActivationRequest(run_id=RUN_ID, qfit="qfit07"),
            runner=runner,
            observer=runner.observe,
            sleeper=lambda seconds: None,
            registration_attempts=1,
            packet_attempts=1,
            pdu_attempts=1,
            rollback_attempts=1,
            poll_interval_seconds=0,
        )

    def test_postcondition_truth_can_accept_nonzero_mutation_returncodes(self) -> None:
        result = self.execute(DirectQfitRunner(mutation_returncode=1))
        self.assertTrue(result.accepted)
        self.assertEqual("pdu-established", result.status)
        self.assertTrue(result.final_runtime.pdu_session_established)
        self.assertTrue(any(step.returncode == 1 for step in result.steps))
        self.assertFalse(result.to_dict()["raw_modem_output_persisted"])
        self.assertFalse(result.to_dict()["subscriber_identity_queried"])

    def test_existing_pdu_is_idempotent_and_performs_no_mutation(self) -> None:
        runner = DirectQfitRunner()
        runner.radio_on = True
        runner.attached = True
        runner.ipv4 = True
        result = self.execute(runner)
        self.assertTrue(result.accepted)
        self.assertEqual("already-established", result.status)
        self.assertEqual((), result.steps)
        self.assertTrue(all("--query-radio-state" in command for command in runner.commands))

    def test_attach_failure_requests_exact_rollback(self) -> None:
        runner = DirectQfitRunner(attach_effective=False)
        result = self.execute(runner)
        self.assertFalse(result.accepted)
        self.assertEqual("failed-clean", result.status)
        self.assertTrue(result.rollback_proven)
        names = [step.name for step in result.steps]
        self.assertIn("rollback-radio-off", names)
        self.assertIn("rollback-link-down", names)
        rendered = "\n".join(" ".join(command) for command in runner.commands)
        self.assertNotIn("--disconnect", rendered)

    def test_unproven_rollback_remains_explicitly_unresolved(self) -> None:
        result = self.execute(
            DirectQfitRunner(attach_effective=False, rollback_effective=False)
        )
        self.assertFalse(result.accepted)
        self.assertEqual("failed-unresolved", result.status)
        self.assertFalse(result.rollback_proven)

    def test_activation_evidence_serialization_contains_no_raw_network_values(self) -> None:
        result = self.execute(DirectQfitRunner())
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "activation.json"
            result.write_json(path)
            payload = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual("pdu-established", payload["status"])
        self.assertNotIn("00101", str(payload))
        self.assertNotIn("198.51.100", str(payload))


class R2LabAuthorizedQfitFlowTests(unittest.TestCase):
    @staticmethod
    def successful_activation() -> QfitActivationResult:
        return QfitActivationResult(
            run_id=RUN_ID,
            qfit="qfit07",
            dnn="internet",
            interface="wwan0",
            device="/dev/cdc-wdm0",
            session_id=0,
            status="pdu-established",
            initial_runtime=runtime_state(),
            final_runtime=runtime_state(
                packet=PacketServiceState.ATTACHED,
                ipv4=Ipv4State.PRESENT,
            ),
            final_radio_state=SoftwareRadioState.ON,
            rollback_proven=False,
            steps=(),
        )

    @patch("synthran.r2lab.ue.execute_qfit_activation")
    @patch("synthran.r2lab.controller.authorize_physical_start")
    def test_lower_acceptance_evidence_is_required_before_activation(
        self,
        authorize,
        activate,
    ) -> None:
        authorize.return_value = authority()
        with self.assertRaisesRegex(R2LabQfitActivationError, "current next stage"):
            execute_authorized_qfit_activation(
                evidence=raw_started_evidence(),
                slice_name="oulu_user",
                run_root=Path("/tmp/r2lab-tests"),
                known_hosts=Path(__file__),
                r2lab_runner=lambda command, timeout: CommandResult(0, "", ""),
                cluster_runner=lambda command, timeout: CommandResult(0, "", ""),
                sleeper=lambda seconds: None,
            )
        activate.assert_not_called()

    @patch("synthran.r2lab.ue.execute_qfit_activation")
    @patch("synthran.r2lab.runtime.execute_qfit_runtime_probe")
    @patch("synthran.r2lab.runtime.execute_qfit_management_probe")
    @patch("synthran.r2lab.runtime.verify_gnb_n2")
    @patch("synthran.r2lab.controller.authorize_physical_start")
    def test_authorized_activation_advances_only_through_pdu(
        self,
        authorize,
        verify_gnb,
        management,
        runtime_probe,
        activate,
    ) -> None:
        authorize.return_value = authority()
        verify_gnb.return_value = proven_gnb()
        management.return_value = True
        runtime_probe.return_value = runtime_state()
        activate.return_value = self.successful_activation()

        outcome = execute_authorized_qfit_activation(
            evidence=base_evidence(),
            slice_name="oulu_user",
            run_root=Path("/tmp/r2lab-tests"),
            known_hosts=Path(__file__),
            r2lab_runner=lambda command, timeout: CommandResult(0, "", ""),
            cluster_runner=lambda command, timeout: CommandResult(0, "", ""),
            sleeper=lambda seconds: None,
        )
        self.assertIsNotNone(outcome.activation)
        for stage in (
            PhysicalAcceptanceStage.GNB_N2,
            PhysicalAcceptanceStage.REGISTRATION,
            PhysicalAcceptanceStage.PDU_SESSION,
        ):
            self.assertEqual(
                AcceptanceOutcome.PASSED,
                outcome.evidence.acceptance.outcome_for(stage),
            )
        self.assertEqual(
            PhysicalAcceptanceStage.USER_PLANE,
            outcome.evidence.acceptance.next_stage,
        )

    @patch("synthran.r2lab.ue.execute_qfit_activation")
    @patch("synthran.r2lab.runtime.execute_qfit_runtime_probe")
    @patch("synthran.r2lab.runtime.execute_qfit_management_probe")
    @patch("synthran.r2lab.runtime.verify_gnb_n2")
    @patch("synthran.r2lab.controller.authorize_physical_start")
    def test_cell_failure_stops_before_any_activation_mutation(
        self,
        authorize,
        verify_gnb,
        management,
        runtime_probe,
        activate,
    ) -> None:
        authorize.return_value = authority()
        verify_gnb.return_value = proven_gnb()
        management.return_value = True
        runtime_probe.return_value = runtime_state(
            cell=CellAcquisitionState.NO_SERVICE,
            registration=RegistrationState.NOT_REGISTERED,
        )
        outcome = execute_authorized_qfit_activation(
            evidence=base_evidence(),
            slice_name="oulu_user",
            run_root=Path("/tmp/r2lab-tests"),
            known_hosts=Path(__file__),
            r2lab_runner=lambda command, timeout: CommandResult(0, "", ""),
            cluster_runner=lambda command, timeout: CommandResult(0, "", ""),
            sleeper=lambda seconds: None,
        )
        self.assertIsNone(outcome.activation)
        self.assertEqual(
            PhysicalAcceptanceStage.CELL_ACQUISITION,
            outcome.evidence.acceptance.failed_stage,
        )
        activate.assert_not_called()

    @patch("synthran.r2lab.ue.execute_user_plane_probe")
    @patch("synthran.r2lab.runtime.execute_qfit_runtime_probe")
    @patch("synthran.r2lab.runtime.execute_qfit_management_probe")
    @patch("synthran.r2lab.runtime.verify_gnb_n2")
    @patch("synthran.r2lab.controller.authorize_physical_start")
    def test_authorized_user_plane_requires_current_pdu_reproof(
        self,
        authorize,
        verify_gnb,
        management,
        runtime_probe,
        ping_probe,
    ) -> None:
        authorize.return_value = authority()
        verify_gnb.return_value = proven_gnb()
        management.return_value = True
        runtime_probe.return_value = runtime_state(
            packet=PacketServiceState.ATTACHED,
            ipv4=Ipv4State.PRESENT,
        )
        ping_probe.return_value = UserPlaneProbeEvidence(
            interface="wwan0",
            peer_sha256="e" * 64,
            requested_packets=4,
            transmitted_packets=4,
            received_packets=4,
            summary_observed=True,
            returncode=0,
            transport_error=False,
        )
        outcome = execute_authorized_qfit_user_plane(
            evidence=pdu_evidence(),
            slice_name="oulu_user",
            run_root=Path("/tmp/r2lab-tests"),
            known_hosts=Path(__file__),
            peer="198.51.100.10",
            r2lab_runner=lambda command, timeout: CommandResult(0, "", ""),
            cluster_runner=lambda command, timeout: CommandResult(0, "", ""),
        )
        self.assertTrue(outcome.probe.proven)
        self.assertEqual(
            PhysicalAcceptanceStage.WORKLOAD,
            outcome.evidence.acceptance.next_stage,
        )


class R2LabPhysicalWorkloadHandoffTests(unittest.TestCase):
    def patch_current_path(self):
        return (
            patch(
                "synthran.r2lab.controller.authorize_physical_start",
                return_value=authority(),
            ),
            patch(
                "synthran.r2lab.runtime.verify_gnb_n2",
                return_value=proven_gnb(),
            ),
            patch(
                "synthran.r2lab.runtime.execute_qfit_management_probe",
                return_value=True,
            ),
            patch(
                "synthran.r2lab.runtime.execute_qfit_runtime_probe",
                return_value=runtime_state(
                    packet=PacketServiceState.ATTACHED,
                    ipv4=Ipv4State.PRESENT,
                ),
            ),
        )

    def test_explicit_physical_executor_can_complete_final_acceptance_stage(self) -> None:
        patches = self.patch_current_path()
        with patches[0], patches[1], patches[2], patches[3]:
            outcome = execute_physical_workload_handoff(
                evidence=user_plane_evidence(),
                slice_name="oulu_user",
                run_root=Path("/tmp/r2lab-tests"),
                known_hosts=Path(__file__),
                r2lab_runner=lambda command, timeout: CommandResult(0, "", ""),
                cluster_runner=lambda command, timeout: CommandResult(0, "", ""),
                executor=lambda context: PhysicalWorkloadResult(
                    run_id=RUN_ID,
                    workload_id="physical-baseline-001",
                    backend="r2lab",
                    interface="wwan0",
                    evidence_sha256="f" * 64,
                    accepted=True,
                    cleanup_proven=True,
                ),
            )
        self.assertTrue(outcome.evidence.acceptance.accepted)

    def test_virtual_result_cannot_satisfy_physical_workload_stage(self) -> None:
        patches = self.patch_current_path()
        with patches[0], patches[1], patches[2], patches[3]:
            with self.assertRaisesRegex(R2LabQfitActivationError, "non-R2Lab"):
                execute_physical_workload_handoff(
                    evidence=user_plane_evidence(),
                    slice_name="oulu_user",
                    run_root=Path("/tmp/r2lab-tests"),
                    known_hosts=Path(__file__),
                    r2lab_runner=lambda command, timeout: CommandResult(0, "", ""),
                    cluster_runner=lambda command, timeout: CommandResult(0, "", ""),
                    executor=lambda context: PhysicalWorkloadResult(
                        run_id=RUN_ID,
                        workload_id="virtual-baseline",
                        backend="rfsim",
                        interface="wwan0",
                        evidence_sha256="f" * 64,
                        accepted=True,
                        cleanup_proven=True,
                    ),
                )

    def test_executor_exception_is_sanitized_and_records_workload_failure(self) -> None:
        patches = self.patch_current_path()

        def failing_executor(context):
            raise RuntimeError("private remote detail")

        with patches[0], patches[1], patches[2], patches[3]:
            outcome = execute_physical_workload_handoff(
                evidence=user_plane_evidence(),
                slice_name="oulu_user",
                run_root=Path("/tmp/r2lab-tests"),
                known_hosts=Path(__file__),
                r2lab_runner=lambda command, timeout: CommandResult(0, "", ""),
                cluster_runner=lambda command, timeout: CommandResult(0, "", ""),
                executor=failing_executor,
            )
        self.assertIsNone(outcome.result)
        self.assertEqual(
            PhysicalAcceptanceStage.WORKLOAD,
            outcome.evidence.acceptance.failed_stage,
        )
        self.assertNotIn("private remote detail", str(outcome.evidence.to_dict()))


if __name__ == "__main__":
    unittest.main()
