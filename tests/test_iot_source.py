from __future__ import annotations

from pathlib import Path, PurePosixPath
import json
import subprocess
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from synthran.ambient_contract import ENERGY_TRACE_SHA256
from synthran.dependencies import GitDependency
from synthran.iot_source import (
    AMBIENT_PROFILE,
    AMBER_SOURCE_ID,
    IOT_EVIDENCE_SCHEMA_V2,
    IOT_EXPERIMENT_SCHEMA_V2,
    IOT_SOURCE_EVENT_SCHEMA_V2,
    TRANSPORT_PROFILE,
    AmberSourceAdapter,
    IoTSourceError,
    IoTSourceEvent,
    IoTSourceSpec,
    MQTTEndpoint,
    profile_descriptor,
    profile_digest,
    reconcile_source_and_transport,
)


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
AMBER_COMMIT = "08dd6bd445e607ad3accf4e9a2dff51a499ebdf9"


class IoTSourceContractTests(unittest.TestCase):
    def test_default_spec_is_backend_neutral_amber_transport(self) -> None:
        spec = IoTSourceSpec(run_id="amber-test", network_run_id="network-test")
        self.assertEqual(AMBER_SOURCE_ID, spec.source)
        self.assertEqual(TRANSPORT_PROFILE, spec.profile)
        self.assertEqual(424242, spec.seed)
        self.assertEqual(10, spec.sensor_count)
        self.assertEqual("synthran/amber-test", spec.topic_root)
        self.assertEqual("sensor-01", spec.sensor_ids[0])
        self.assertEqual("sensor-10", spec.sensor_ids[-1])

    def test_spec_rejects_non_amber_source_and_bad_profile(self) -> None:
        with self.assertRaisesRegex(IoTSourceError, "unsupported IoT source"):
            IoTSourceSpec(
                run_id="amber-test",
                network_run_id="network-test",
                source="cooja",
            )
        with self.assertRaisesRegex(IoTSourceError, "unsupported Amber profile"):
            IoTSourceSpec(
                run_id="amber-test",
                network_run_id="network-test",
                profile="unknown",
            )

    def test_profile_digest_is_seed_independent_but_period_sensitive(self) -> None:
        first = IoTSourceSpec(run_id="amber-a", network_run_id="network-test", seed=1)
        second = IoTSourceSpec(run_id="amber-b", network_run_id="network-test", seed=99)
        changed_period = IoTSourceSpec(
            run_id="amber-c",
            network_run_id="network-test",
            seed=1,
            sensor_period_seconds=20,
        )
        first_digest = profile_digest(
            profile_descriptor(first, amber_commit=AMBER_COMMIT, energy_trace_sha256=None)
        )
        second_digest = profile_digest(
            profile_descriptor(second, amber_commit=AMBER_COMMIT, energy_trace_sha256=None)
        )
        changed_digest = profile_digest(
            profile_descriptor(
                changed_period,
                amber_commit=AMBER_COMMIT,
                energy_trace_sha256=None,
            )
        )
        self.assertEqual(first_digest, second_digest)
        self.assertNotEqual(first_digest, changed_digest)

    def test_ambient_profile_requires_and_hashes_energy_trace(self) -> None:
        spec = IoTSourceSpec(
            run_id="ambient-test",
            network_run_id="network-test",
            profile=AMBIENT_PROFILE,
        )
        with self.assertRaisesRegex(IoTSourceError, "energy trace"):
            profile_descriptor(spec, amber_commit=AMBER_COMMIT, energy_trace_sha256=None)
        descriptor = profile_descriptor(
            spec,
            amber_commit=AMBER_COMMIT,
            energy_trace_sha256=ENERGY_TRACE_SHA256,
        )
        model = descriptor["model"]
        self.assertEqual("hybrid", model["energy"]["mode"])
        self.assertTrue(model["collision"]["sic"])
        self.assertEqual(16, model["access"]["slots"])
        self.assertEqual(ENERGY_TRACE_SHA256, model["energy"]["trace_sha256"])

    def test_source_event_round_trip_preserves_scientific_outcome(self) -> None:
        event = IoTSourceEvent(
            run_id="ambient-test",
            source="amber",
            profile="ambient-v1",
            profile_digest="b" * 64,
            planned_at_ms=10000,
            sensor_id="sensor-03",
            sequence=2,
            value_milli=3002,
            transmitted=True,
            decoded=False,
            outcome="collision",
            slot_index=4,
            details={"collided_packets": 1},
        )
        rendered = event.to_dict()
        self.assertEqual(IOT_SOURCE_EVENT_SCHEMA_V2, rendered["schema"])
        self.assertEqual(event, IoTSourceEvent.from_dict(rendered))

    def test_decoded_event_cannot_be_untransmitted(self) -> None:
        with self.assertRaisesRegex(IoTSourceError, "must have been transmitted"):
            IoTSourceEvent(
                run_id="ambient-test",
                source="amber",
                profile="ambient-v1",
                profile_digest="c" * 64,
                planned_at_ms=0,
                sensor_id="sensor-01",
                sequence=1,
                value_milli=1001,
                transmitted=False,
                decoded=True,
                outcome="decoded",
            )

    def test_reconciliation_separates_source_and_transport_loss(self) -> None:
        events = (
            IoTSourceEvent(
                run_id="ambient-test",
                source="amber",
                profile="ambient-v1",
                profile_digest="d" * 64,
                planned_at_ms=0,
                sensor_id="sensor-01",
                sequence=1,
                value_milli=1001,
                transmitted=True,
                decoded=True,
                outcome="decoded",
            ),
            IoTSourceEvent(
                run_id="ambient-test",
                source="amber",
                profile="ambient-v1",
                profile_digest="d" * 64,
                planned_at_ms=0,
                sensor_id="sensor-02",
                sequence=1,
                value_milli=2001,
                transmitted=True,
                decoded=False,
                outcome="collision",
            ),
            IoTSourceEvent(
                run_id="ambient-test",
                source="amber",
                profile="ambient-v1",
                profile_digest="d" * 64,
                planned_at_ms=0,
                sensor_id="sensor-03",
                sequence=1,
                value_milli=3001,
                transmitted=True,
                decoded=True,
                outcome="decoded",
            ),
        )
        result = reconcile_source_and_transport(
            events,
            published_pairs=(("sensor-01", 1), ("sensor-03", 1)),
            central_pairs=(("sensor-01", 1),),
        )
        self.assertEqual(1, result.source_loss_count)
        self.assertEqual(1, result.transport_loss_count)
        self.assertFalse(result.valid)

    def test_schemas_do_not_reintroduce_cooja_or_network_mechanisms(self) -> None:
        scenario_schema = json.loads(
            (REPOSITORY_ROOT / "contracts" / "iot-experiment-v2alpha1.schema.json").read_text()
        )
        names = set(scenario_schema["properties"])
        self.assertFalse(
            names.intersection(
                {"cooja_seed", "serial_socket_port", "rpl_prefix", "pdu_address", "ue_interface"}
            )
        )


class AmberSourceAdapterTests(unittest.TestCase):
    def test_verify_checkout_rejects_drift(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            checkout = root / "deps" / "amber"
            (checkout / ".git").mkdir(parents=True)
            adapter = AmberSourceAdapter(
                repository_root=REPOSITORY_ROOT,
                dependency_root=root / "deps",
            )
            responses = [
                subprocess.CompletedProcess([], 0, stdout="0" * 40 + "\n", stderr=""),
                subprocess.CompletedProcess([], 0, stdout="", stderr=""),
            ]
            with patch("synthran.iot_source.subprocess.run", side_effect=responses):
                with self.assertRaisesRegex(IoTSourceError, "checkout drifted"):
                    adapter._verify_amber_checkout()

    def test_prepare_writes_deterministic_transport_artifacts(self) -> None:
        dependency = GitDependency(
            name="amber",
            url="https://github.com/RA-Nayreed/Amber.git",
            commit=AMBER_COMMIT,
            checkout=PurePosixPath("amber"),
            sync=True,
            role="Ambient IoT discrete-event source model",
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            checkout = root / "amber"
            checkout.mkdir()
            run_directory = root / "run"
            adapter = AmberSourceAdapter(repository_root=REPOSITORY_ROOT)
            spec = IoTSourceSpec(
                run_id="amber-prepare",
                network_run_id="network-test",
                sensor_period_seconds=10,
            )

            def fake_planner(command, **kwargs):
                del kwargs
                input_path = Path(command[command.index("--input") + 1])
                output_path = Path(command[command.index("--output") + 1])
                config = json.loads(input_path.read_text(encoding="utf-8"))
                events = []
                for planned_at_ms in (0, 10000, 20000):
                    sequence = planned_at_ms // 10000 + 1
                    for index in range(1, 11):
                        events.append(
                            {
                                "schema": IOT_SOURCE_EVENT_SCHEMA_V2,
                                "run_id": config["run_id"],
                                "source": "amber",
                                "profile": "transport-v1",
                                "profile_digest": config["profile_digest"],
                                "planned_at_ms": planned_at_ms,
                                "sensor_id": f"sensor-{index:02d}",
                                "sequence": sequence,
                                "value_milli": index * 1000 + sequence % 1000,
                                "transmitted": True,
                                "decoded": True,
                                "outcome": "decoded",
                                "slot_index": index - 1,
                                "details": {"model": "ideal-unicast"},
                            }
                        )
                output_path.write_text(
                    "".join(json.dumps(event, sort_keys=True) + "\n" for event in events),
                    encoding="utf-8",
                )
                return subprocess.CompletedProcess(command, 0, stdout="planner-ok\n", stderr="")

            with patch.object(
                adapter,
                "_verify_amber_checkout",
                return_value=(dependency, checkout),
            ), patch("synthran.iot_source.subprocess.run", side_effect=fake_planner):
                plan = adapter.prepare(spec, 21, run_directory)

            self.assertEqual(30, plan.planned_count)
            self.assertEqual(30, plan.decoded_count)
            self.assertEqual(0, plan.source_loss_count)
            self.assertTrue(plan.source_jsonl_path.is_file())
            self.assertTrue(plan.source_parquet_path.is_file())
            scenario = json.loads(plan.scenario_path.read_text(encoding="utf-8"))
            evidence = json.loads(plan.evidence_path.read_text(encoding="utf-8"))
            self.assertEqual(IOT_EXPERIMENT_SCHEMA_V2, scenario["schema"])
            self.assertEqual(IOT_EVIDENCE_SCHEMA_V2, evidence["schema"])
            self.assertNotIn("pdu_address", scenario)
            self.assertNotIn("ue_interface", scenario)
            self.assertNotIn("serial_socket_port", scenario)
            self.assertIsNone(evidence["live_transport"])

    def test_live_start_returns_portable_session_wrapper(self) -> None:
        adapter = AmberSourceAdapter(repository_root=REPOSITORY_ROOT)
        plan = MagicMock()
        endpoint = MQTTEndpoint("127.0.0.1", 18886)
        barrier = MagicMock()
        replay = MagicMock()
        replay.start.return_value = replay
        replay.evidence.return_value.to_dict.return_value = {
            "client_count": 10,
            "complete": True,
        }
        with patch("synthran.iot_publisher.AmberReplaySession", return_value=replay) as cls:
            session = adapter.start(plan, endpoint, barrier)
        cls.assert_called_once_with(
            plan=plan,
            endpoint=endpoint,
            collector_barrier=barrier,
        )
        replay.start.assert_called_once_with()
        self.assertEqual(
            {"client_count": 10, "complete": True},
            session.evidence(),
        )
        session.stop()
        replay.stop.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
