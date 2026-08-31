from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import tempfile
import unittest

from synthran.experiment import (
    ExperimentError,
    ExperimentScenario,
    TelemetryEvent,
    build_scenario,
    deterministic_records,
    load_path_proven_network,
    render_edge_mosquitto_config,
    validate_sequence_integrity,
)


class ExperimentContractTests(unittest.TestCase):
    def _network_evidence(
        self,
        root: Path,
        *,
        status: str = "path-proven",
        ready: bool = True,
    ) -> tuple[Path, Path]:
        manifest = root / "manifest.json"
        evidence = root / "network-evidence.json"
        manifest.write_text(
            json.dumps(
                {
                    "run_id": "network-accepted-01",
                    "status": status,
                    "network_evidence": evidence.name,
                }
            ),
            encoding="utf-8",
        )
        evidence.write_text(
            json.dumps(
                {
                    "schema": "synthran/network-evidence/v1alpha1",
                    "run_id": "network-accepted-01",
                    "ready": ready,
                    "path": {
                        "pdu_address": "12.1.0.1",
                        "pdu_network": "12.1.0.0/16",
                        "ue_interface": "tun_srsue1",
                        "slice": "slice1",
                        "sst": 1,
                        "dnn": "internet",
                    },
                }
            ),
            encoding="utf-8",
        )
        return manifest, evidence

    def test_experiment_requires_path_proven_network(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            manifest, evidence = self._network_evidence(
                Path(temporary), status="deployed-unverified"
            )
            with self.assertRaisesRegex(ExperimentError, "accepted network manifest"):
                load_path_proven_network(manifest, evidence)

    def test_build_scenario_uses_accepted_pdu(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            manifest, evidence = self._network_evidence(Path(temporary))
            scenario = build_scenario(
                run_id="experiment-01",
                network_manifest=manifest,
                network_evidence=evidence,
            )
            self.assertEqual(scenario.sensor_count, 10)
            self.assertEqual(scenario.pdu_address, "12.1.0.1")
            self.assertEqual(
                scenario.sensor_topic,
                "synthran/experiment-01/sensor/+",
            )

    def test_edge_bridge_is_run_scoped_and_pdu_bound(self) -> None:
        scenario = ExperimentScenario(
            "experiment-01",
            "network-accepted-01",
            "12.1.0.1",
        )
        config = render_edge_mosquitto_config(
            scenario,
            central_broker_address="192.0.2.10",
            central_broker_port=18884,
        )
        self.assertIn("listener 1883", config.splitlines())
        self.assertNotIn("listener 1883 ::", config)
        self.assertIn("bridge_bind_address 12.1.0.1", config)
        self.assertIn("address 192.0.2.10:18884", config)
        self.assertIn("topic synthran/experiment-01/# out 1", config)
        self.assertNotIn("topic #", config)

    def test_telemetry_validation(self) -> None:
        payload = json.dumps(
            {
                "schema": "synthran/telemetry/v1alpha1",
                "run_id": "experiment-01",
                "sensor_id": "sensor-10",
                "sequence": 3,
                "sensor_time_ms": 20000,
                "value_milli": 10003,
            }
        )
        event = TelemetryEvent.from_payload(payload, "experiment-01")
        record = event.to_record(
            received_at_utc=datetime(2026, 8, 16, tzinfo=timezone.utc)
        )
        self.assertEqual(record["sensor_id"], "sensor-10")
        self.assertEqual(record["sequence"], 3)

    def test_sequence_integrity_requires_all_ten_sensors(self) -> None:
        records = [
            {"sensor_id": f"sensor-{index:02d}", "sequence": sequence}
            for index in range(1, 11)
            for sequence in (1, 2, 3)
        ]
        self.assertEqual(
            validate_sequence_integrity(records, minimum_per_sensor=3),
            (),
        )
        broken = [
            record
            for record in records
            if not (
                record["sensor_id"] == "sensor-04"
                and record["sequence"] == 2
            )
        ]
        failures = validate_sequence_integrity(broken, minimum_per_sensor=3)
        self.assertTrue(any("sensor-04" in failure for failure in failures))

    def test_canonical_records_are_sensor_then_sequence(self) -> None:
        records = [
            {"sensor_id": "sensor-02", "sequence": 2},
            {"sensor_id": "sensor-01", "sequence": 2},
            {"sensor_id": "sensor-01", "sequence": 1},
        ]
        canonical = deterministic_records(records)
        self.assertEqual(
            [(item["sensor_id"], item["sequence"]) for item in canonical],
            [("sensor-01", 1), ("sensor-01", 2), ("sensor-02", 2)],
        )


if __name__ == "__main__":
    unittest.main()
