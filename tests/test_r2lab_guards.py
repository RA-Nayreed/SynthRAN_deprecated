from __future__ import annotations

from pathlib import Path
import unittest
from unittest.mock import patch

from synthran.live_preflight import CommandResult
from synthran.r2lab.acceptance import PhysicalRunEvidence
from synthran.r2lab.deployment import PhysicalStartAuthority
from synthran.r2lab.guards import (
    R2LabMutationGuardError,
    prove_qfit_mutation_guard,
)
from synthran.r2lab.readiness import QfitReadinessEvidence
from synthran.r2lab.runtime import GnbN2Evidence, N2State


RUN_ID = "r2lab-guard-test"
CLAIM = "d" * 64
PACKAGE = "a" * 64
VALUES = "b" * 64
RENDER = "c" * 64
PEER = "192.0.2.234"
REMOTE_KNOWN_HOSTS = "/var/lib/synthran/r2lab/fit07_known_hosts"


def evidence() -> PhysicalRunEvidence:
    staged = {
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
    started = {
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
    return PhysicalRunEvidence(run_id=RUN_ID).bind_staging(staged).bind_gnb_start(started)


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


def gnb(*, proven: bool = True) -> GnbN2Evidence:
    return GnbN2Evidence(
        namespace_owned=True,
        deployment_bound=True,
        desired_replicas=1,
        pod_count=1,
        ready_running_count=1,
        n2_state=N2State.ESTABLISHED if proven else N2State.NOT_OBSERVED,
        log_observed=True,
        transport_error=False,
        n2_source="amf-exact-peer" if proven else "not-observed",
        peer_fingerprint="e" * 64 if proven else None,
    )


def readiness(*, ready: bool = True) -> QfitReadinessEvidence:
    return QfitReadinessEvidence(
        qfit="qfit07",
        usb_power_on=ready,
        strict_ssh=ready,
        serial_device_present=ready,
        mbim_device_present=ready,
        wwan_interface_present=ready,
        transport_error=False,
    )


class R2LabQfitMutationGuardTests(unittest.TestCase):
    @patch("synthran.r2lab.guards.execute_qfit_readiness")
    @patch("synthran.r2lab.guards.verify_gnb_n2")
    @patch("synthran.r2lab.controller.authorize_physical_start")
    def test_guard_combines_exact_authority_amf_capable_n2_and_readiness(
        self,
        authorize,
        verify_n2,
        verify_readiness,
    ) -> None:
        authorize.return_value = authority()
        verify_n2.return_value = gnb()
        verify_readiness.return_value = readiness()
        commands: list[tuple[str, ...]] = []

        def r2lab_runner(command, timeout_seconds: int) -> CommandResult:
            commands.append(tuple(command))
            return CommandResult(0, "", "")

        result = prove_qfit_mutation_guard(
            evidence=evidence(),
            slice_name="oulu_user",
            run_root=Path("/tmp/r2lab-tests"),
            known_hosts=Path(__file__),
            qfit_known_hosts_remote=REMOTE_KNOWN_HOSTS,
            r2lab_runner=r2lab_runner,
            cluster_runner=lambda command, timeout: CommandResult(0, "", ""),
            expected_gnb_n2_peer=PEER,
        )

        self.assertTrue(result.proven)
        verify_n2.assert_called_once()
        self.assertEqual(
            PEER,
            verify_n2.call_args.kwargs["expected_gnb_n2_peer"],
        )
        verify_readiness.assert_called_once()
        self.assertEqual(
            REMOTE_KNOWN_HOSTS,
            verify_readiness.call_args.kwargs["remote_known_hosts"],
        )
        self.assertNotIn(PEER, str(result.to_dict()))
        self.assertEqual([], commands)

    @patch("synthran.r2lab.guards.execute_qfit_readiness")
    @patch("synthran.r2lab.guards.verify_gnb_n2")
    @patch("synthran.r2lab.controller.authorize_physical_start")
    def test_n2_failure_stops_before_qfit_readiness(
        self,
        authorize,
        verify_n2,
        verify_readiness,
    ) -> None:
        authorize.return_value = authority()
        verify_n2.return_value = gnb(proven=False)

        with self.assertRaisesRegex(R2LabMutationGuardError, "gNB/N2"):
            prove_qfit_mutation_guard(
                evidence=evidence(),
                slice_name="oulu_user",
                run_root=Path("/tmp/r2lab-tests"),
                known_hosts=Path(__file__),
                qfit_known_hosts_remote=REMOTE_KNOWN_HOSTS,
                r2lab_runner=lambda command, timeout: CommandResult(0, "", ""),
                cluster_runner=lambda command, timeout: CommandResult(0, "", ""),
                expected_gnb_n2_peer=PEER,
            )

        verify_readiness.assert_not_called()

    @patch("synthran.r2lab.guards.execute_qfit_readiness")
    @patch("synthran.r2lab.guards.verify_gnb_n2")
    @patch("synthran.r2lab.controller.authorize_physical_start")
    def test_not_ready_qfit_fails_closed(
        self,
        authorize,
        verify_n2,
        verify_readiness,
    ) -> None:
        authorize.return_value = authority()
        verify_n2.return_value = gnb()
        verify_readiness.return_value = readiness(ready=False)

        with self.assertRaisesRegex(R2LabMutationGuardError, "not ready"):
            prove_qfit_mutation_guard(
                evidence=evidence(),
                slice_name="oulu_user",
                run_root=Path("/tmp/r2lab-tests"),
                known_hosts=Path(__file__),
                qfit_known_hosts_remote=REMOTE_KNOWN_HOSTS,
                r2lab_runner=lambda command, timeout: CommandResult(0, "", ""),
                cluster_runner=lambda command, timeout: CommandResult(0, "", ""),
                expected_gnb_n2_peer=PEER,
            )

    def test_missing_strict_remote_known_hosts_is_rejected_before_live_calls(self) -> None:
        with self.assertRaisesRegex(R2LabMutationGuardError, "known-hosts"):
            prove_qfit_mutation_guard(
                evidence=evidence(),
                slice_name="oulu_user",
                run_root=Path("/tmp/r2lab-tests"),
                known_hosts=Path(__file__),
                qfit_known_hosts_remote="",
                r2lab_runner=lambda command, timeout: CommandResult(0, "", ""),
                cluster_runner=lambda command, timeout: CommandResult(0, "", ""),
                expected_gnb_n2_peer=PEER,
            )


if __name__ == "__main__":
    unittest.main()
