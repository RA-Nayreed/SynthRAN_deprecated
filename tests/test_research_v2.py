from __future__ import annotations

import unittest

from synthran.research import LoadSpec, MeasurementSpec, ResearchError
from synthran.research.v2 import (
    AmberResearchSpec,
    RESEARCH_EXPERIMENT_SCHEMA_V2,
    RESEARCH_SUMMARY_SCHEMA_V2,
    measurement_source_bounds,
    require_consistent_campaign_summaries,
    research_experiment_artifact,
    research_summary_artifact,
    select_measurement_telemetry,
)


class AmberResearchV2Tests(unittest.TestCase):
    def test_spec_uses_iot_seed_and_total_warmup_measurement_duration(self) -> None:
        spec = AmberResearchSpec(
            campaign_id="amber-campaign",
            run_id="amber-run",
            network_run_id="network-run",
            condition="baseline",
            iot_profile="ambient-v1",
            iot_seed=17,
            sensor_period_seconds=12,
            measurement=MeasurementSpec(warmup_seconds=30, duration_seconds=180),
        )
        self.assertEqual(210, spec.total_source_seconds)
        rendered = spec.to_request_dict()
        self.assertEqual("amber", rendered["iot_source"])
        self.assertEqual("ambient-v1", rendered["iot_profile"])
        self.assertEqual(17, rendered["iot_seed"])
        self.assertNotIn("cooja_seed", rendered)

    def test_experiment_artifact_persists_profile_identity(self) -> None:
        spec = AmberResearchSpec(
            campaign_id="amber-campaign",
            run_id="amber-run",
            network_run_id="network-run",
            condition="baseline",
        )
        value = research_experiment_artifact(
            spec,
            profile_digest="a" * 64,
            amber_commit="b" * 40,
            energy_trace_sha256=None,
        )
        self.assertEqual(RESEARCH_EXPERIMENT_SCHEMA_V2, value["schema"])
        self.assertEqual("a" * 64, value["profile_digest"])
        self.assertEqual("b" * 40, value["amber_commit"])

    def test_summary_separates_source_and_transport_loss(self) -> None:
        spec = AmberResearchSpec(
            campaign_id="amber-campaign",
            run_id="amber-run",
            network_run_id="network-run",
            condition="baseline",
            iot_profile="ambient-v1",
        )
        summary = research_summary_artifact(
            spec,
            profile_digest="c" * 64,
            planned_opportunities=100,
            decoded_opportunities=80,
            published_events=80,
            received_events=80,
            source_loss=20,
            transport_loss=0,
            duplicate_count=0,
            measurement_received_events=60,
            infrastructure_valid=True,
            scientific_valid=True,
        )
        self.assertEqual(RESEARCH_SUMMARY_SCHEMA_V2, summary["schema"])
        self.assertEqual(20, summary["source"]["source_loss"])
        self.assertEqual(0, summary["transport"]["transport_loss"])
        self.assertTrue(summary["infrastructure_valid"])

    def test_transport_profile_rejects_source_loss(self) -> None:
        spec = AmberResearchSpec(
            campaign_id="amber-campaign",
            run_id="amber-run",
            network_run_id="network-run",
            condition="baseline",
        )
        with self.assertRaisesRegex(ResearchError, "cannot contain source loss"):
            research_summary_artifact(
                spec,
                profile_digest="c" * 64,
                planned_opportunities=100,
                decoded_opportunities=99,
                published_events=99,
                received_events=99,
                source_loss=1,
                transport_loss=0,
                duplicate_count=0,
                measurement_received_events=99,
                infrastructure_valid=True,
                scientific_valid=True,
            )

    def test_transport_loss_forces_infrastructure_invalid(self) -> None:
        spec = AmberResearchSpec(
            campaign_id="amber-campaign",
            run_id="amber-run",
            network_run_id="network-run",
            condition="baseline",
            iot_profile="ambient-v1",
        )
        summary = research_summary_artifact(
            spec,
            profile_digest="d" * 64,
            planned_opportunities=100,
            decoded_opportunities=80,
            published_events=80,
            received_events=79,
            source_loss=20,
            transport_loss=1,
            duplicate_count=0,
            measurement_received_events=59,
            infrastructure_valid=True,
            scientific_valid=False,
        )
        self.assertFalse(summary["infrastructure_valid"])

    def test_campaign_analysis_rejects_mixed_profile_or_digest(self) -> None:
        spec = AmberResearchSpec(
            campaign_id="amber-campaign",
            run_id="amber-run",
            network_run_id="network-run",
            condition="baseline",
            iot_profile="ambient-v1",
        )
        first = research_summary_artifact(
            spec,
            profile_digest="e" * 64,
            planned_opportunities=10,
            decoded_opportunities=8,
            published_events=8,
            received_events=8,
            source_loss=2,
            transport_loss=0,
            duplicate_count=0,
            measurement_received_events=8,
            infrastructure_valid=True,
            scientific_valid=True,
        )
        second = dict(first)
        second["run_id"] = "amber-run-two"
        self.assertEqual(
            ("amber", "ambient-v1", "e" * 64),
            require_consistent_campaign_summaries([first, second]),
        )
        second["profile_digest"] = "f" * 64
        with self.assertRaisesRegex(ResearchError, "mixed IoT source/profile"):
            require_consistent_campaign_summaries([first, second])

    def test_new_campaign_analysis_does_not_mix_legacy_cooja_summary(self) -> None:
        with self.assertRaisesRegex(ResearchError, "v2 summaries"):
            require_consistent_campaign_summaries(
                [
                    {
                        "schema": "synthran/research-summary/v1alpha1",
                        "run_id": "legacy-run",
                    }
                ]
            )

    def test_warmup_events_remain_source_evidence_but_leave_measurement_metrics(self) -> None:
        spec = AmberResearchSpec(
            campaign_id="amber-campaign",
            run_id="amber-run",
            network_run_id="network-run",
            condition="baseline",
            measurement=MeasurementSpec(warmup_seconds=20, duration_seconds=30),
        )
        self.assertEqual((20000, 50000), measurement_source_bounds(spec))
        records = [
            {"sensor_time_ms": 0, "sensor_id": "sensor-01", "sequence": 1},
            {"sensor_time_ms": 10000, "sensor_id": "sensor-01", "sequence": 2},
            {"sensor_time_ms": 20000, "sensor_id": "sensor-01", "sequence": 3},
            {"sensor_time_ms": 40000, "sensor_id": "sensor-01", "sequence": 5},
            {"sensor_time_ms": 50000, "sensor_id": "sensor-01", "sequence": 6},
        ]
        selected = select_measurement_telemetry(spec, records)
        self.assertEqual([3, 5], [record["sequence"] for record in selected])
        self.assertEqual(5, len(records))

    def test_loaded_condition_still_requires_load(self) -> None:
        with self.assertRaisesRegex(ResearchError, "requires an enabled load"):
            AmberResearchSpec(
                campaign_id="amber-campaign",
                run_id="amber-run",
                network_run_id="network-run",
                condition="load50",
            )
        valid = AmberResearchSpec(
            campaign_id="amber-campaign",
            run_id="amber-run",
            network_run_id="network-run",
            condition="load50",
            load=LoadSpec(enabled=True, target_bps=1_000_000),
        )
        self.assertTrue(valid.load.enabled)


if __name__ == "__main__":
    unittest.main()
