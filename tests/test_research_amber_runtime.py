from __future__ import annotations

from datetime import datetime, timedelta, timezone
import inspect
import json
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from synthran.iot_source import IoTSourceEvent
from synthran.research import MeasurementSpec
from synthran.research.amber_runtime import (
    AmberResearchMeasurementLifecycle,
    execute_amber_research_experiment,
)
from synthran.research.v2 import AmberResearchSpec, RESEARCH_SUMMARY_SCHEMA_V2


class AmberResearchRuntimeTests(unittest.TestCase):
    def test_amber_research_execution_does_not_use_legacy_runtime_overrides(self) -> None:
        source = inspect.getsource(execute_amber_research_experiment)
        self.assertNotIn("_runtime_overrides", source)
        self.assertNotIn("_RUNTIME_OVERRIDE_LOCK", source)

    def test_research_cleanup_is_idempotent(self) -> None:
        spec = AmberResearchSpec(
            campaign_id="amber-campaign",
            run_id="amber-research",
            network_run_id="network-run",
            condition="baseline",
            measurement=MeasurementSpec(warmup_seconds=0, duration_seconds=30),
            probe_target="198.51.100.1",
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            lifecycle = AmberResearchMeasurementLifecycle(
                spec=spec,
                inventory=MagicMock(),
                lock=MagicMock(),
                repository_root=root,
            )
            lifecycle.context = SimpleNamespace(
                run_id=spec.run_id,
                network_run_id=spec.network_run_id,
                ue_pod="srsran-ue-test",
                pdu_address="10.45.0.2",
                run_directory=root,
            )
            with patch(
                "synthran.research.amber_runtime._parse_probe_log"
            ) as parse_probe:
                lifecycle.stop()
                lifecycle.stop()
            parse_probe.assert_called_once()
            self.assertTrue(lifecycle._instrumentation_stopped)

    def test_run_passes_explicit_lifecycle_and_total_source_duration(self) -> None:
        spec = AmberResearchSpec(
            campaign_id="amber-campaign",
            run_id="amber-research",
            network_run_id="network-run",
            condition="baseline",
            iot_profile="ambient-v1",
            iot_seed=77,
            sensor_period_seconds=10,
            measurement=MeasurementSpec(warmup_seconds=20, duration_seconds=30),
            probe_target="198.51.100.1",
        )
        with tempfile.TemporaryDirectory() as directory:
            run_directory = Path(directory) / spec.run_id
            run_directory.mkdir(parents=True)
            (run_directory / "iot-evidence-v2.json").write_text(
                json.dumps(
                    {
                        "schema": "synthran/iot-evidence/v2alpha1",
                        "run_id": spec.run_id,
                        "iot_source": "amber",
                        "iot_profile": "ambient-v1",
                        "iot_seed": 77,
                        "profile_digest": "a" * 64,
                        "amber_commit": "b" * 40,
                        "energy_trace_sha256": "c" * 64,
                        "ready": True,
                        "live_transport": {
                            "reconciliation": {
                                "valid": True,
                                "planned_count": 50,
                                "decoded_count": 40,
                                "source_loss_count": 10,
                                "published_count": 40,
                                "central_received_count": 40,
                                "transport_loss_count": 0,
                                "duplicate_count": 0,
                                "unexpected_central_count": 0,
                            },
                            "measurement": {
                                "source_clock_alignment": "publisher-start-gate",
                                "pre_window_network_ready": True,
                                "pre_window_target_ready": True,
                                "post_window_network_ready": True,
                                "instrumentation_errors": [],
                            },
                        },
                    }
                ),
                encoding="utf-8",
            )

            profile_digest = "a" * 64
            measurement_source: list[IoTSourceEvent] = []
            for period_index, planned_at_ms in enumerate((20_000, 30_000, 40_000), start=3):
                for sensor_index in range(1, 11):
                    decoded = planned_at_ms < 40_000
                    measurement_source.append(
                        IoTSourceEvent(
                            run_id=spec.run_id,
                            source="amber",
                            profile="ambient-v1",
                            profile_digest=profile_digest,
                            planned_at_ms=planned_at_ms,
                            sensor_id=f"sensor-{sensor_index:02d}",
                            sequence=period_index,
                            value_milli=1000 + sensor_index,
                            transmitted=decoded,
                            decoded=decoded,
                            outcome="decoded" if decoded else "energy-below-threshold",
                        )
                    )

            decoded_events = [event for event in measurement_source if event.decoded]
            telemetry = [
                {
                    "schema": "synthran/telemetry/v1alpha1",
                    "run_id": spec.run_id,
                    "sensor_id": event.sensor_id,
                    "sequence": event.sequence,
                    "sensor_time_ms": event.planned_at_ms,
                    "value_milli": event.value_milli,
                }
                for event in decoded_events
            ]
            published_pairs = [event.key for event in decoded_events]

            fake_result = SimpleNamespace(run_directory=run_directory)
            inventory = MagicMock()
            lock = MagicMock()
            window_start = datetime(2026, 8, 30, 10, 0, 20, tzinfo=timezone.utc)
            window_end = window_start + timedelta(seconds=30)
            with patch(
                "synthran.research.amber_runtime.execute_amber_experiment",
                return_value=fake_result,
            ) as execute, patch(
                "synthran.research.amber_runtime._load_window",
                return_value=(window_start, window_end, 20_000, 50_000),
            ), patch(
                "synthran.research.amber_runtime.load_source_events",
                return_value=measurement_source,
            ), patch(
                "synthran.research.amber_runtime.load_telemetry_jsonl",
                return_value=telemetry,
            ), patch(
                "synthran.research.amber_runtime._publisher_pairs",
                return_value=published_pairs,
            ), patch(
                "synthran.research.amber_runtime.write_records_parquet"
            ), patch(
                "synthran.research.amber_runtime._measurement_metrics",
                return_value={"probe_records": 0, "mean_rtt_ms": None},
            ):
                summary_path = execute_amber_research_experiment(
                    spec=spec,
                    inventory=inventory,
                    lock=lock,
                    dependency_root=Path(directory) / "deps",
                    network_manifest=Path(directory) / "manifest.json",
                    network_evidence=Path(directory) / "network-evidence.json",
                    repository_root=Path(directory),
                    run_root=Path(directory),
                )

            kwargs = execute.call_args.kwargs
            self.assertEqual(50, kwargs["collection_seconds"])
            self.assertEqual("ambient-v1", kwargs["iot_profile"])
            self.assertEqual(77, kwargs["iot_seed"])
            self.assertEqual(1.0, kwargs["energy_power_scale"])
            self.assertEqual(0.0, kwargs["energy_node_variation"])
            self.assertIsNotNone(kwargs["measurement_lifecycle"])

            summary = json.loads(summary_path.read_text(encoding="utf-8"))
            self.assertEqual(RESEARCH_SUMMARY_SCHEMA_V2, summary["schema"])
            self.assertEqual(30, summary["source"]["planned_opportunities"])
            self.assertEqual(20, summary["source"]["decoded_opportunities"])
            self.assertEqual(10, summary["source"]["source_loss"])
            self.assertEqual(0, summary["transport"]["transport_loss"])
            self.assertEqual(20, summary["measurement_received_events"])
            self.assertTrue(summary["infrastructure_valid"])
            self.assertTrue(summary["scientific_valid"])

            experiment = json.loads(
                (run_directory / "experiment-spec-v2.json").read_text(encoding="utf-8")
            )
            self.assertEqual(77, experiment["iot_seed"])
            self.assertNotIn("cooja_seed", experiment)


if __name__ == "__main__":
    unittest.main()
