from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from synthran.dependencies import load_lock
from synthran.r2lab.acceptance import (
    PhysicalAcceptanceStage,
    PhysicalRunEvidence,
    STAGE_ORDER,
)
from synthran.r2lab.deployment import (
    PHYSICAL_VALUES_SOURCE,
    PhysicalChartBindings,
    PhysicalGnbStartResult,
    PhysicalGnbStopResult,
    PhysicalHelmRenderEvidence,
    PhysicalStagingResult,
)
from synthran.r2lab.gnb import (
    execute_physical_gnb_n2_acceptance,
    execute_physical_gnb_staging,
)
from synthran.r2lab.runtime import (
    GnbN2Evidence,
    N2State,
    PhysicalGnbN2VerificationResult,
)


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
RUN_ID = "r2lab-gnb-test"
NOW = datetime(2026, 8, 24, 0, 30, tzinfo=timezone.utc)


def foundation_evidence() -> PhysicalRunEvidence:
    evidence = PhysicalRunEvidence(run_id=RUN_ID)
    for stage in STAGE_ORDER[:4]:
        evidence = evidence.pass_stage(stage, source=f"fixture-{stage.value}")
    return evidence


def staging_result() -> PhysicalStagingResult:
    return PhysicalStagingResult(
        run_id=RUN_ID,
        package_sha256="a" * 64,
        values_sha256="b" * 64,
        render_sha256="c" * 64,
        namespace_owned=True,
        desired_replicas=0,
        gnb_pod_count=0,
    )


class R2LabPhysicalGnbCompositionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.bindings = PhysicalChartBindings(
            amf_n2_address="198.51.100.200",
            gnb_n2_address="198.51.100.234",
            n300_address="192.0.2.203",
            ru_pod_address="192.0.2.234",
            ru_subnet="192.0.2.0/24",
        )

    @staticmethod
    def write_foundation(root: Path) -> Path:
        run_directory = root / RUN_ID
        run_directory.mkdir(parents=True)
        evidence_path = run_directory / "physical-run.json"
        foundation_evidence().write_json(evidence_path)
        return evidence_path

    def test_staging_composes_offline_artifacts_and_binds_live_result(self) -> None:
        lock = load_lock(REPOSITORY_ROOT / "dependencies.lock.yml")
        dependency = next(item for item in lock.git if item.name == "srsran_helm")
        render = PhysicalHelmRenderEvidence(
            sha256="c" * 64,
            source_values_sha256="e" * 64,
            replicas=0,
            strategy="Recreate",
            image_reference="example.invalid/gnb:v1@sha256:" + "d" * 64,
            carrier_arfcn=640_000,
            band=78,
            channel_bandwidth_mhz=20,
            common_scs_khz=30,
            sample_rate_mhz=61.44,
            tx_gain_db=35,
            rx_gain_db=60,
            ss0_index=0,
            coreset0_index=12,
            prach_config_index=1,
            device_args_sha256="f" * 64,
        )

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            run_root = root / "runs"
            evidence_path = self.write_foundation(run_root)
            known_hosts = root / "known_hosts"
            known_hosts.write_text("fixture\n", encoding="utf-8")
            checkout = root / "deps" / dependency.checkout
            templates = checkout / "charts" / "srsran-gnb" / "templates"
            templates.mkdir(parents=True)
            (templates.parent / "Chart.yaml").write_text(
                "apiVersion: v2\nname: srsran-gnb\n",
                encoding="utf-8",
            )
            (templates.parent / Path(PHYSICAL_VALUES_SOURCE).name).write_text(
                "pinned R2Lab values\n",
                encoding="utf-8",
            )
            (templates / "deployment.yaml").write_text(
                "apiVersion: apps/v1\n"
                "kind: Deployment\n"
                "spec:\n"
                "  selector:\n"
                "    matchLabels:\n"
                "      app: srsran\n"
                "  replicas: 1\n"
                "  template:\n"
                "    spec:\n"
                "      containers:\n"
                "        - name: gnb\n"
                "          image: \"{{ .Values.image.repository }}:{{ .Values.image.tag }}\"\n",
                encoding="utf-8",
            )

            def staged(**kwargs) -> PhysicalStagingResult:
                artifact = kwargs["artifact"]
                reservation_verifier = kwargs["reservation_verifier"]
                self.assertTrue(callable(reservation_verifier))
                reservation_verifier()
                return PhysicalStagingResult(
                    run_id=RUN_ID,
                    package_sha256=artifact.package_sha256,
                    values_sha256=artifact.values_sha256,
                    render_sha256=render.sha256,
                    namespace_owned=True,
                    desired_replicas=0,
                    gnb_pod_count=0,
                )

            with (
                patch("synthran.r2lab.gnb.authorize_physical_start"),
                patch("synthran.r2lab.gnb._verify_checkout"),
                patch(
                    "synthran.r2lab.gnb.render_physical_chart_offline",
                    return_value=("rendered\n", render),
                ),
                patch(
                    "synthran.r2lab.gnb.execute_stopped_physical_staging",
                    side_effect=staged,
                ) as live_stage,
            ):
                result = execute_physical_gnb_staging(
                    run_id=RUN_ID,
                    slice_name="test_slice",
                    owner="test-owner",
                    reservation_id="reservation-1",
                    allocation_id="allocation-1",
                    known_hosts=known_hosts,
                    now=NOW,
                    bindings=self.bindings,
                    lock_path=REPOSITORY_ROOT / "dependencies.lock.yml",
                    deps_root=root / "deps",
                    run_root=run_root,
                )
                recovered = execute_physical_gnb_staging(
                    run_id=RUN_ID,
                    slice_name="test_slice",
                    owner="test-owner",
                    reservation_id="reservation-1",
                    allocation_id="allocation-1",
                    known_hosts=known_hosts,
                    now=NOW,
                    bindings=self.bindings,
                    lock_path=REPOSITORY_ROOT / "dependencies.lock.yml",
                    deps_root=root / "deps",
                    run_root=run_root,
                )

            persisted = PhysicalRunEvidence.read_json(evidence_path)

        self.assertEqual("staged-stopped", result.to_dict()["status"])
        self.assertEqual(result.staging, recovered.staging)
        self.assertIsNotNone(persisted.staged)
        self.assertEqual(1, live_stage.call_count)

    def test_start_binds_singleton_and_persists_n2_proof(self) -> None:
        stage = staging_result()
        started = PhysicalGnbStartResult(
            run_id=RUN_ID,
            package_sha256=stage.package_sha256,
            values_sha256=stage.values_sha256,
            render_sha256=stage.render_sha256,
            claim_sha256="d" * 64,
            maximum_observed_pods=1,
        )
        n2 = GnbN2Evidence(
            namespace_owned=True,
            deployment_bound=True,
            desired_replicas=1,
            pod_count=1,
            ready_running_count=1,
            n2_state=N2State.ESTABLISHED,
            n2_source="gnb-log",
            log_observed=True,
            transport_error=False,
        )

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            run_root = root / "runs"
            evidence_path = self.write_foundation(run_root)
            evidence = PhysicalRunEvidence.read_json(evidence_path).bind_staging(
                stage.to_dict()
            )
            evidence.write_json(evidence_path)
            physical = run_root / RUN_ID / "physical"
            physical.mkdir()
            (physical / "physical-staging.json").write_text(
                json.dumps(stage.to_dict()),
                encoding="utf-8",
            )
            (physical / "physical-chart.json").write_text(
                json.dumps({"values": {"gnbIp": "198.51.100.234"}}),
                encoding="utf-8",
            )
            known_hosts = root / "known_hosts"
            known_hosts.write_text("fixture\n", encoding="utf-8")

            def verified(**kwargs) -> PhysicalGnbN2VerificationResult:
                self.assertEqual(2, kwargs["required_consecutive_proofs"])
                accepted = kwargs["evidence"].pass_stage(
                    PhysicalAcceptanceStage.GNB_N2,
                    source="fixture-gnb-n2",
                )
                accepted.write_json(kwargs["evidence_path"])
                return PhysicalGnbN2VerificationResult(accepted, n2, 1)

            with (
                patch(
                    "synthran.r2lab.gnb.execute_physical_gnb_start",
                    return_value=started,
                ),
                patch(
                    "synthran.r2lab.gnb.execute_physical_gnb_n2_verification",
                    side_effect=verified,
                ),
            ):
                result = execute_physical_gnb_n2_acceptance(
                    run_id=RUN_ID,
                    slice_name="test_slice",
                    owner="test-owner",
                    reservation_id="reservation-1",
                    allocation_id="allocation-1",
                    known_hosts=known_hosts,
                    now=NOW,
                    run_root=run_root,
                    attempts=2,
                    poll_interval_seconds=0,
                )
            persisted = PhysicalRunEvidence.read_json(evidence_path)

        self.assertTrue(result.proven)
        self.assertIsNotNone(persisted.gnb_start)
        self.assertEqual(
            PhysicalAcceptanceStage.UE_MANAGEMENT,
            persisted.acceptance.next_stage,
        )

    def test_unsuccessful_n2_proof_scales_bound_gnb_to_zero(self) -> None:
        stage = staging_result()
        failed_observation = GnbN2Evidence(
            namespace_owned=True,
            deployment_bound=True,
            desired_replicas=1,
            pod_count=1,
            ready_running_count=1,
            n2_state=N2State.NOT_OBSERVED,
            n2_source="not-observed",
            log_observed=True,
            transport_error=False,
        )
        started = PhysicalGnbStartResult(
            run_id=RUN_ID,
            package_sha256=stage.package_sha256,
            values_sha256=stage.values_sha256,
            render_sha256=stage.render_sha256,
            claim_sha256="d" * 64,
            maximum_observed_pods=1,
        )

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            run_root = root / "runs"
            evidence_path = self.write_foundation(run_root)
            evidence = PhysicalRunEvidence.read_json(evidence_path).bind_staging(
                stage.to_dict()
            )
            evidence.write_json(evidence_path)
            physical = run_root / RUN_ID / "physical"
            physical.mkdir()
            (physical / "physical-staging.json").write_text(
                json.dumps(stage.to_dict()), encoding="utf-8"
            )
            (physical / "physical-chart.json").write_text(
                json.dumps({"values": {"gnbIp": "198.51.100.234"}}),
                encoding="utf-8",
            )
            known_hosts = root / "known_hosts"
            known_hosts.write_text("fixture\n", encoding="utf-8")

            def failed(**kwargs) -> PhysicalGnbN2VerificationResult:
                self.assertEqual(2, kwargs["required_consecutive_proofs"])
                blocked = kwargs["evidence"].fail_stage(
                    PhysicalAcceptanceStage.GNB_N2,
                    source="fixture-no-n2",
                )
                blocked.write_json(kwargs["evidence_path"])
                return PhysicalGnbN2VerificationResult(
                    blocked,
                    failed_observation,
                    2,
                )

            with (
                patch(
                    "synthran.r2lab.gnb.execute_physical_gnb_start",
                    return_value=started,
                ),
                patch(
                    "synthran.r2lab.gnb.execute_physical_gnb_n2_verification",
                    side_effect=failed,
                ),
                patch(
                    "synthran.r2lab.gnb.execute_authorized_physical_gnb_stop",
                    return_value=PhysicalGnbStopResult(RUN_ID, 0, 0),
                ) as stop,
            ):
                result = execute_physical_gnb_n2_acceptance(
                    run_id=RUN_ID,
                    slice_name="test_slice",
                    owner="test-owner",
                    reservation_id="reservation-1",
                    allocation_id="allocation-1",
                    known_hosts=known_hosts,
                    now=NOW,
                    run_root=run_root,
                    attempts=2,
                    poll_interval_seconds=0,
                )
            stop_evidence = json.loads(
                (physical / "physical-gnb-stop.json").read_text(encoding="utf-8")
            )

        self.assertFalse(result.proven)
        self.assertEqual(1, stop.call_count)
        self.assertEqual("gnb-stopped", stop_evidence["status"])


if __name__ == "__main__":
    unittest.main()
