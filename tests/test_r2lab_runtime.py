from __future__ import annotations

import hashlib
import json
from pathlib import Path
import shlex
import tempfile
import unittest

from synthran.live_preflight import CommandResult
from synthran.r2lab.acceptance import (
    AcceptanceOutcome,
    PhysicalAcceptanceStage,
    PhysicalRunEvidence,
    STAGE_ORDER,
)
from synthran.r2lab.deployment import (
    DEPLOYMENT_PACKAGE_ANNOTATION,
    DEPLOYMENT_RENDER_ANNOTATION,
    DEPLOYMENT_RUN_ANNOTATION,
    DEPLOYMENT_RUN_LABEL,
    DEPLOYMENT_VALUES_ANNOTATION,
    POD_RUNTIME_STATE_KEY,
)
from synthran.r2lab.runtime import (
    N2State,
    R2LabRuntimeVerificationError,
    capture_gnb_failure_evidence,
    execute_physical_gnb_n2_verification,
    execute_physical_runtime_verification,
    execute_qfit_runtime_probe,
    parse_n2_log_state,
    qfit_runtime_probe_commands,
    verify_gnb_n2,
)


RUN_ID = "r2lab-runtime-test"
SLICE = "oulu_user"
PACKAGE = "a" * 64
VALUES = "b" * 64
RENDER = "c" * 64
REMOTE_QFIT_KNOWN_HOSTS = "/var/lib/synthran/r2lab/fit07_known_hosts"
TEST_GNB_PEER = "192.0.2.234"


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


def start_payload(claim_sha256: str) -> dict[str, object]:
    return {
        "run_id": RUN_ID,
        "package_sha256": PACKAGE,
        "values_sha256": VALUES,
        "render_sha256": RENDER,
        "claim_sha256": claim_sha256,
        "maximum_observed_pods": 1,
        "started_exactly_one": True,
        "status": "gnb-started",
        "hardware_mutation": True,
    }


def canonical_sha256(payload: dict[str, object]) -> str:
    return hashlib.sha256(
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()


class RuntimeR2LabRunner:
    def __init__(self, *, readiness_ready: bool = True) -> None:
        self.readiness_ready = readiness_ready
        self.commands: list[tuple[str, ...]] = []

    @staticmethod
    def remote(command: tuple[str, ...]) -> tuple[str, ...]:
        split = command.index("--")
        return command[split + 2 :]

    @staticmethod
    def inner_qfit(remote: tuple[str, ...]) -> tuple[str, ...] | None:
        if len(remote) != 1:
            return None
        outer = tuple(shlex.split(remote[0]))
        if not outer or outer[0] != "ssh":
            return None
        return tuple(shlex.split(outer[-1]))

    @staticmethod
    def fit_command(remote: tuple[str, ...]) -> tuple[str, ...] | None:
        if not remote or remote[0] != "ssh" or "root@fit07" not in remote:
            return None
        index = remote.index("root@fit07")
        return remote[index + 1 :]

    def __call__(self, command, timeout_seconds: int) -> CommandResult:
        value = tuple(command)
        self.commands.append(value)
        remote = self.remote(value)
        if remote == ("rhubarbe", "leases", "--check"):
            return CommandResult(0, "", "")
        if remote == ("rhubarbe", "pdu", "status", "n300"):
            return CommandResult(0, "pdu2 chain-0@outlet-1 (n300): ON (28W)\n", "")
        if remote == ("ping", "-c", "1", "-W", "1", "fit07"):
            return CommandResult(0, "", "")
        if remote == ("curl", "-fsS", "http://reboot07/usrpstatus"):
            return CommandResult(0, "usrpon\n", "")

        fit = self.fit_command(remote)
        if fit is not None:
            if fit == ("true",):
                return CommandResult(0, "", "")
            if fit == ("test", "-c", "/dev/ttyUSB2"):
                return CommandResult(0, "", "")
            if fit == ("test", "-c", "/dev/cdc-wdm0"):
                return CommandResult(0, "", "")
            if fit == ("ip", "link", "show", "dev", "wwan0"):
                return CommandResult(
                    0 if self.readiness_ready else 1,
                    "9: wwan0: <UP> mtu 1500\n" if self.readiness_ready else "",
                    "",
                )
            raise AssertionError(f"unexpected FIT command: {fit}")

        qfit = self.inner_qfit(remote)
        if qfit is None:
            raise AssertionError(f"unexpected R2Lab command: {value}")
        if qfit[:2] == ("python3", "-c") and qfit[-1] == "AT+QNWINFO":
            return CommandResult(
                0,
                '+QNWINFO: "NR5G-SA","00101","NR5G BAND 78",640000\n',
                "",
            )
        if qfit[:2] == ("python3", "-c") and qfit[-1] == "AT+C5GREG?":
            return CommandResult(0, "+C5GREG: 0,1\n", "")
        if qfit and qfit[0] == "mbimcli":
            return CommandResult(0, "Packet service state: 'attached'\n", "")
        if qfit[:5] == ("ip", "-o", "link", "show", "dev"):
            return CommandResult(0, "9: wwan0: <UP> mtu 1500\n", "")
        if qfit[:5] == ("ip", "-o", "-4", "addr", "show"):
            return CommandResult(
                0,
                "9: wwan0    inet 198.51.100.2/24 scope global wwan0\n",
                "",
            )
        if qfit and qfit[0] == "ping":
            return CommandResult(
                0,
                "4 packets transmitted, 4 received, 0% packet loss, time 3ms\n",
                "",
            )
        raise AssertionError(f"unexpected qfit command: {qfit}")


class RuntimeClusterRunner:
    def __init__(
        self,
        *,
        run_id: str = RUN_ID,
        render: str = RENDER,
        gnb_log: str = "NGAP: AMF connection established\n",
        amf_log: str = f"[amf] INFO: gNB-N2 accepted[{TEST_GNB_PEER}]:58612\n",
        gnb_ready: bool = True,
        gnb_restart_count: int = 0,
        gnb_waiting_reason: str | None = None,
        gnb_last_termination_reason: str | None = None,
        gnb_last_exit_code: int | None = None,
        previous_gnb_log: str = "",
    ) -> None:
        self.run_id = run_id
        self.render = render
        self.gnb_log = gnb_log
        self.amf_log = amf_log
        self.gnb_ready = gnb_ready
        self.gnb_restart_count = gnb_restart_count
        self.gnb_waiting_reason = gnb_waiting_reason
        self.gnb_last_termination_reason = gnb_last_termination_reason
        self.gnb_last_exit_code = gnb_last_exit_code
        self.previous_gnb_log = previous_gnb_log
        self.commands: list[tuple[str, ...]] = []

    @staticmethod
    def remote(command: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(shlex.split(command[-1]))

    def __call__(self, command, timeout_seconds: int) -> CommandResult:
        value = tuple(command)
        self.commands.append(value)
        remote = self.remote(value)
        if remote[:3] == ("kubectl", "get", "namespace"):
            return CommandResult(0, self.run_id, "")
        if remote[:3] == ("kubectl", "get", "deployment/srsran-gnb"):
            return CommandResult(
                0,
                json.dumps(
                    {
                        "metadata": {
                            "labels": {DEPLOYMENT_RUN_LABEL: self.run_id},
                            "annotations": {
                                DEPLOYMENT_RUN_ANNOTATION: self.run_id,
                                DEPLOYMENT_PACKAGE_ANNOTATION: PACKAGE,
                                DEPLOYMENT_VALUES_ANNOTATION: VALUES,
                                DEPLOYMENT_RENDER_ANNOTATION: self.render,
                            },
                        },
                        "spec": {"replicas": 1},
                    }
                ),
                "",
            )
        if remote[:3] == ("kubectl", "get", "pods"):
            if "open5gs" in remote and "nf=amf" in remote:
                return CommandResult(
                    0,
                    json.dumps(
                        {
                            "items": [
                                {
                                    "metadata": {"name": "open5gs-amf-current"},
                                    "status": {
                                        POD_RUNTIME_STATE_KEY: "Running",
                                        "containerStatuses": [
                                            {"name": "amf", "ready": True}
                                        ],
                                    },
                                }
                            ]
                        }
                    ),
                    "",
                )
            return CommandResult(
                0,
                json.dumps(
                    {
                        "items": [
                            {
                                "metadata": {
                                    "name": "srsran-gnb-current",
                                    "creationTimestamp": "2026-08-24T13:01:11Z",
                                },
                                "status": {
                                    POD_RUNTIME_STATE_KEY: "Running",
                                    "containerStatuses": [
                                        {
                                            "name": "gnb",
                                            "ready": self.gnb_ready,
                                            "restartCount": self.gnb_restart_count,
                                            "state": (
                                                {
                                                    "waiting": {
                                                        "reason": self.gnb_waiting_reason
                                                    }
                                                }
                                                if self.gnb_waiting_reason is not None
                                                else {"running": {}}
                                            ),
                                            "lastState": (
                                                {
                                                    "terminated": {
                                                        "reason": self.gnb_last_termination_reason,
                                                        "exitCode": self.gnb_last_exit_code,
                                                        "signal": 0,
                                                    }
                                                }
                                                if self.gnb_last_termination_reason
                                                is not None
                                                else {}
                                            ),
                                        }
                                    ],
                                },
                            }
                        ]
                    }
                ),
                "",
            )
        if remote[:2] == ("kubectl", "logs"):
            if "pod/open5gs-amf-current" in remote:
                return CommandResult(0, self.amf_log, "")
            if "--previous" in remote:
                return CommandResult(0, self.previous_gnb_log, "")
            return CommandResult(0, self.gnb_log, "")
        raise AssertionError(f"unexpected cluster command: {remote}")


class R2LabQfitLiveProbeTests(unittest.TestCase):
    def test_probe_set_is_read_only_and_never_uses_subscriber_or_attach_helpers(self) -> None:
        rendered = "\n".join(shlex.join(command) for command in qfit_runtime_probe_commands())
        self.assertIn("AT+QNWINFO", rendered)
        self.assertIn("AT+C5GREG?", rendered)
        self.assertIn("--query-packet-service-state", rendered)
        self.assertNotIn("AT+CIMI", rendered)
        self.assertNotIn("check-ue", rendered)
        self.assertNotIn("start.sh", rendered)
        self.assertNotIn("--attach-packet-service", rendered)
        self.assertNotIn("--connect=", rendered)

    def test_live_probe_immediately_reduces_raw_modem_output_to_sanitized_state(self) -> None:
        runner = RuntimeR2LabRunner()
        evidence = execute_qfit_runtime_probe(slice_name=SLICE, qfit="qfit07", runner=runner)
        payload = evidence.to_dict()
        self.assertTrue(evidence.cell_acquired)
        self.assertTrue(evidence.registered)
        self.assertTrue(evidence.pdu_session_established)
        self.assertNotIn("00101", str(payload))
        self.assertNotIn("198.51.100.2", str(payload))
        nested = [
            RuntimeR2LabRunner.inner_qfit(RuntimeR2LabRunner.remote(command))
            for command in runner.commands
        ]
        flattened = "\n".join(shlex.join(command) for command in nested if command is not None)
        self.assertIn("StrictHostKeyChecking=yes", "\n".join(" ".join(c) for c in runner.commands))
        self.assertNotIn("StrictHostKeyChecking=no", flattened)
        self.assertNotIn("AT+CIMI", flattened)

    def test_unreviewed_qfit_is_rejected_before_transport(self) -> None:
        runner = RuntimeR2LabRunner()
        with self.assertRaisesRegex(R2LabRuntimeVerificationError, "reviewed qfit"):
            execute_qfit_runtime_probe(slice_name=SLICE, qfit="qfit99", runner=runner)
        self.assertEqual([], runner.commands)


class R2LabGnbN2VerificationTests(unittest.TestCase):
    def base_evidence(self) -> PhysicalRunEvidence:
        return PhysicalRunEvidence(run_id=RUN_ID).bind_staging(staging_payload()).bind_gnb_start(
            start_payload("d" * 64)
        )

    def test_current_bound_singleton_and_current_pod_n2_log_are_required(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            known_hosts = Path(directory) / "known_hosts"
            known_hosts.write_text("fixture\n", encoding="utf-8")
            result = verify_gnb_n2(
                evidence=self.base_evidence(), known_hosts=known_hosts, runner=RuntimeClusterRunner()
            )
        self.assertTrue(result.proven)
        self.assertEqual(N2State.ESTABLISHED, result.n2_state)
        self.assertEqual("gnb-log", result.n2_source)
        self.assertEqual(1, result.pod_count)
        self.assertEqual(1, result.ready_running_count)
        self.assertNotIn("srsran-gnb-current", str(result.to_dict()))

    def test_exact_amf_peer_can_prove_n2_when_current_gnb_log_is_silent(self) -> None:
        cluster = RuntimeClusterRunner(gnb_log="gNB running\n")
        with tempfile.TemporaryDirectory() as directory:
            known_hosts = Path(directory) / "known_hosts"
            known_hosts.write_text("fixture\n", encoding="utf-8")
            result = verify_gnb_n2(
                evidence=self.base_evidence(),
                known_hosts=known_hosts,
                runner=cluster,
                expected_gnb_n2_peer=TEST_GNB_PEER,
            )
        self.assertTrue(result.proven)
        self.assertEqual(N2State.ESTABLISHED, result.n2_state)
        self.assertEqual("amf-exact-peer", result.n2_source)
        self.assertEqual(64, len(result.peer_fingerprint or ""))
        self.assertNotIn(TEST_GNB_PEER, str(result.to_dict()))
        rendered = "\n".join(shlex.join(RuntimeClusterRunner.remote(c)) for c in cluster.commands)
        self.assertIn("-l nf=amf", rendered)
        self.assertIn("pod/open5gs-amf-current", rendered)
        self.assertIn("--since-time=2026-08-24T13:01:11Z", rendered)

    def test_changed_render_binding_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            known_hosts = Path(directory) / "known_hosts"
            known_hosts.write_text("fixture\n", encoding="utf-8")
            result = verify_gnb_n2(
                evidence=self.base_evidence(),
                known_hosts=known_hosts,
                runner=RuntimeClusterRunner(render="e" * 64),
            )
        self.assertFalse(result.proven)
        self.assertFalse(result.deployment_bound)

    def test_not_ready_singleton_reports_the_exact_sanitized_blocker(self) -> None:
        cluster = RuntimeClusterRunner(gnb_ready=False)
        with tempfile.TemporaryDirectory() as directory:
            known_hosts = Path(directory) / "known_hosts"
            known_hosts.write_text("fixture\n", encoding="utf-8")
            result = verify_gnb_n2(
                evidence=self.base_evidence(),
                known_hosts=known_hosts,
                runner=cluster,
                expected_gnb_n2_peer=TEST_GNB_PEER,
            )
        self.assertFalse(result.proven)
        self.assertEqual(N2State.UNKNOWN, result.n2_state)
        self.assertEqual("not-observed", result.n2_source)
        self.assertEqual(
            (
                "ready-running-count",
                "log-observation",
                "n2-state",
                "n2-source",
            ),
            result.unproven_reasons,
        )
        rendered = "\n".join(
            shlex.join(RuntimeClusterRunner.remote(command))
            for command in cluster.commands
        )
        self.assertNotIn("nf=amf", rendered)

    def test_crash_loop_evidence_is_sanitized_before_cleanup(self) -> None:
        cluster = RuntimeClusterRunner(
            gnb_log="starting gNB\n",
            gnb_ready=False,
            gnb_restart_count=3,
            gnb_waiting_reason="CrashLoopBackOff",
            gnb_last_termination_reason="Error",
            gnb_last_exit_code=1,
            previous_gnb_log=(
                "UHD Error: No devices found for addr=192.168.235.103\n"
            ),
        )
        with tempfile.TemporaryDirectory() as directory:
            known_hosts = Path(directory) / "known_hosts"
            known_hosts.write_text("fixture\n", encoding="utf-8")
            result = capture_gnb_failure_evidence(
                known_hosts=known_hosts,
                runner=cluster,
            )

        payload = result.to_dict()
        self.assertEqual("uhd-device", payload["failure_class"])
        self.assertEqual("crash-loop-backoff", payload["waiting_reason"])
        self.assertEqual("process-error", payload["last_termination_reason"])
        self.assertEqual(3, payload["restart_count"])
        self.assertEqual(["uhd-device"], payload["classifications"])
        self.assertEqual(64, len(str(payload["previous_log_sha256"])))
        self.assertNotIn("192.168.235.103", str(payload))
        self.assertNotIn("srsran-gnb-current", str(payload))

    def test_n2_parser_requires_affirmative_nonfailure_evidence(self) -> None:
        self.assertEqual(N2State.ESTABLISHED, parse_n2_log_state("NGAP: AMF connection established\n"))
        self.assertEqual(N2State.NOT_OBSERVED, parse_n2_log_state("AMF connection failed to establish\n"))


class R2LabPhysicalRuntimeOrchestrationTests(unittest.TestCase):
    @staticmethod
    def prepared_evidence(directory: str) -> tuple[Path, Path, PhysicalRunEvidence, Path]:
        root = Path(directory) / "r2lab"
        run_dir = root / RUN_ID
        run_dir.mkdir(parents=True)
        known_hosts = Path(directory) / "known_hosts"
        known_hosts.write_text("fixture\n", encoding="utf-8")
        slice_fingerprint = hashlib.sha256(SLICE.encode("utf-8")).hexdigest()
        claim = {
            "schema": "synthran/r2lab-claim/v1alpha1",
            "run_id": RUN_ID,
            "slice_fingerprint": slice_fingerprint,
            "radio": "n300",
            "ue": "qfit07",
            "created_at_utc": "2026-08-22T15:00:00Z",
        }
        claim_digest = canonical_sha256(claim)
        (root / "active.json").write_text(
            json.dumps(claim, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        (run_dir / "manifest.json").write_text(
            json.dumps(
                {
                    "schema": "synthran/r2lab-resource/v1alpha1",
                    "run_id": RUN_ID,
                    "status": "ready",
                    "resource_claim": "held",
                    "resources": {
                        "slice_fingerprint": slice_fingerprint,
                        "radio": "n300",
                        "ue": "qfit07",
                    },
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        evidence = PhysicalRunEvidence(run_id=RUN_ID).bind_staging(staging_payload()).bind_gnb_start(
            start_payload(claim_digest)
        )
        for stage in STAGE_ORDER[:4]:
            evidence = evidence.pass_stage(stage, source=f"fixture-{stage.value}")
        evidence_path = run_dir / "evidence" / "physical-run.json"
        return root, known_hosts, evidence, evidence_path

    def test_runtime_verification_persists_ordered_sanitized_acceptance(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root, known_hosts, evidence, evidence_path = self.prepared_evidence(directory)
            r2lab = RuntimeR2LabRunner()
            result = execute_physical_runtime_verification(
                evidence=evidence,
                slice_name=SLICE,
                run_root=root,
                known_hosts=known_hosts,
                r2lab_runner=r2lab,
                cluster_runner=RuntimeClusterRunner(),
                qfit_known_hosts_remote=REMOTE_QFIT_KNOWN_HOSTS,
                user_plane_peer="198.51.100.10",
                evidence_path=evidence_path,
            )
            persisted = evidence_path.read_text(encoding="utf-8")

        self.assertEqual(AcceptanceOutcome.PASSED, result.evidence.acceptance.outcome_for(PhysicalAcceptanceStage.GNB_N2))
        self.assertEqual(AcceptanceOutcome.PASSED, result.evidence.acceptance.outcome_for(PhysicalAcceptanceStage.UE_MANAGEMENT))
        self.assertEqual(AcceptanceOutcome.PASSED, result.evidence.acceptance.outcome_for(PhysicalAcceptanceStage.PDU_SESSION))
        self.assertEqual(AcceptanceOutcome.PASSED, result.evidence.acceptance.outcome_for(PhysicalAcceptanceStage.USER_PLANE))
        self.assertEqual(PhysicalAcceptanceStage.WORKLOAD, result.evidence.acceptance.next_stage)
        self.assertIsNotNone(result.qfit_readiness)
        self.assertTrue(result.qfit_readiness.ready if result.qfit_readiness is not None else False)
        remotes = [RuntimeR2LabRunner.remote(command) for command in r2lab.commands]
        self.assertIn(("curl", "-fsS", "http://reboot07/usrpstatus"), remotes)
        self.assertNotIn(("ping", "-c", "1", "-W", "1", "qfit07"), remotes)
        self.assertNotIn("00101", persisted)
        self.assertNotIn("198.51.100.2", persisted)
        self.assertNotIn("198.51.100.10", persisted)
        self.assertNotIn("srsran-gnb-current", persisted)

    def test_gnb_n2_boundary_retries_without_entering_ue_checks(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root, known_hosts, evidence, evidence_path = self.prepared_evidence(directory)
            r2lab = RuntimeR2LabRunner()
            cluster = RuntimeClusterRunner(gnb_log="gNB starting\n")

            def make_n2_visible(_seconds: float) -> None:
                cluster.gnb_log = "NGAP: AMF connection established\n"

            result = execute_physical_gnb_n2_verification(
                evidence=evidence,
                slice_name=SLICE,
                run_root=root,
                known_hosts=known_hosts,
                r2lab_runner=r2lab,
                cluster_runner=cluster,
                evidence_path=evidence_path,
                attempts=2,
                poll_interval_seconds=0,
                sleeper=make_n2_visible,
            )

        self.assertTrue(result.gnb_n2.proven)
        self.assertEqual(2, result.attempts)
        self.assertEqual(
            PhysicalAcceptanceStage.UE_MANAGEMENT,
            result.evidence.acceptance.next_stage,
        )
        rendered = "\n".join(shlex.join(command) for command in r2lab.commands)
        self.assertNotIn("AT+QNWINFO", rendered)
        self.assertNotIn("/dev/cdc-wdm0", rendered)

    def test_gnb_n2_boundary_requires_consecutive_stable_proofs(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root, known_hosts, evidence, evidence_path = self.prepared_evidence(
                directory
            )
            r2lab = RuntimeR2LabRunner()
            cluster = RuntimeClusterRunner()
            readiness = iter((False, True, True))

            def advance_readiness(_seconds: float) -> None:
                cluster.gnb_ready = next(readiness)

            result = execute_physical_gnb_n2_verification(
                evidence=evidence,
                slice_name=SLICE,
                run_root=root,
                known_hosts=known_hosts,
                r2lab_runner=r2lab,
                cluster_runner=cluster,
                evidence_path=evidence_path,
                attempts=4,
                required_consecutive_proofs=2,
                poll_interval_seconds=0,
                sleeper=advance_readiness,
            )

        self.assertTrue(result.proven)
        self.assertEqual(4, result.attempts)
        self.assertEqual(2, result.consecutive_proofs)
        self.assertEqual(2, result.required_consecutive_proofs)

    def test_not_ready_qfit_stops_before_modem_runtime_probes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root, known_hosts, evidence, evidence_path = self.prepared_evidence(directory)
            r2lab = RuntimeR2LabRunner(readiness_ready=False)
            result = execute_physical_runtime_verification(
                evidence=evidence,
                slice_name=SLICE,
                run_root=root,
                known_hosts=known_hosts,
                r2lab_runner=r2lab,
                cluster_runner=RuntimeClusterRunner(),
                qfit_known_hosts_remote=REMOTE_QFIT_KNOWN_HOSTS,
                evidence_path=evidence_path,
            )
        self.assertEqual(
            AcceptanceOutcome.FAILED,
            result.evidence.acceptance.outcome_for(PhysicalAcceptanceStage.UE_MANAGEMENT),
        )
        self.assertIsNone(result.qfit_runtime)
        rendered = "\n".join(shlex.join(command) for command in r2lab.commands)
        self.assertNotIn("AT+QNWINFO", rendered)
        self.assertNotIn("AT+C5GREG?", rendered)


if __name__ == "__main__":
    unittest.main()
