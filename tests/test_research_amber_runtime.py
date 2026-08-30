from __future__ import annotations

import inspect
import json
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import MagicMock, patch

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
                                "planned_count": 50,
                                "decoded_count": 40,
                                "source_loss_count": 10,
                                "published_count": 40,
                                "central_received_count": 40,
                                "transport_loss_count": 0,
                                "duplicate_count": 0,
                            },
                            "measurement": {
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
            telemetry = [
                {
                    "schema": "synthran/telemetry/v1alpha1",
                    "run_id": spec.run_id,
                    "sensor_id": "sensor-01",
                    "sequence": 1,
                    "sensor_time_ms": 0,
                    "value_milli": 1001,
                },
                {
                    "schema": "synthran/telemetry/v1alpha1",
                    "run_id": spec.run_id,
                    "sensor_id": "sensor-01",
                    "sequence": 3,
                    "sensor_time_ms": 20000,
                    "value_milli": 1003,
                },
            ]
            fake_result = SimpleNamespace(run_directory=run_directory)
            inventory = MagicMock()
            lock = MagicMock()
            with patch(
                "synthran.research.amber_runtime.execute_amber_experiment",
                return_value=fake_result,
            ) as execute, patch(
                "synthran.research.amber_runtime.load_telemetry_jsonl",
                return_value=telemetry,
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
            self.assertIsNotNone(kwargs["measurement_lifecycle"])
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
            self.assertEqual(RESEARCH_SUMMARY_SCHEMA_V2, summary["schema"])
            self.assertEqual(10, summary["source"]["source_loss"])
            self.assertEqual(0, summary["transport"]["transport_loss"])
            self.assertEqual(1, summary["measurement_received_events"])
            experiment = json.loads(
                (run_directory / "experiment-spec-v2.json").read_text(encoding="utf-8")
            )
            self.assertEqual(77, experiment["iot_seed"])
            self.assertNotIn("cooja_seed", experiment)


if __name__ == "__main__":
    unittest.main()
