from __future__ import annotations

from pathlib import Path
import tempfile
import threading
import unittest

from synthran.iot_publisher import AmberReplaySession
from synthran.iot_source import (
    IoTSourceError,
    IoTSourceEvent,
    IoTSourceSpec,
    MQTTEndpoint,
    PreparedIoTPlan,
)


class FakeClock:
    def __init__(self, *, oversleep: float = 0.0) -> None:
        self.now = 100.0
        self.oversleep = oversleep
        self.lock = threading.Lock()

    def monotonic(self) -> float:
        with self.lock:
            return self.now

    def sleep(self, seconds: float) -> None:
        with self.lock:
            self.now += max(0.0, seconds) + self.oversleep


class FakePublishInfo:
    def __init__(self, *, success: bool = True) -> None:
        self.rc = 0 if success else 1
        self.success = success

    def wait_for_publish(self, timeout: float | None = None) -> None:
        del timeout

    def is_published(self) -> bool:
        return self.success


class FakeClient:
    def __init__(self, client_id: str, records: list[tuple[str, str, int]], *, fail_kind: str = "") -> None:
        self.client_id = client_id
        self.records = records
        self.fail_kind = fail_kind
        self.on_connect = None
        self.on_disconnect = None
        self.disconnected = False
        self.looping = False

    def connect(self, host: str, port: int, keepalive: int = 30) -> None:
        del host, port, keepalive
        assert self.on_connect is not None
        self.on_connect(self, None, None, 0, None)

    def loop_start(self) -> None:
        self.looping = True

    def loop_stop(self) -> None:
        self.looping = False

    def disconnect(self) -> None:
        self.disconnected = True

    def publish(self, topic: str, payload: str, qos: int, retain: bool = False) -> FakePublishInfo:
        del retain
        self.records.append((topic, payload, qos))
        should_fail = self.fail_kind and self.fail_kind in topic
        return FakePublishInfo(success=not should_fail)


class Barrier:
    def __init__(self) -> None:
        self.ready_calls = 0
        self.start_calls = 0
        self.sensor_ids: tuple[str, ...] = ()

    def wait_ready(self) -> None:
        self.ready_calls += 1

    def wait_start_canaries(self, sensor_ids) -> None:
        self.start_calls += 1
        self.sensor_ids = tuple(sensor_ids)


def make_plan(root: Path, *, planned_at_ms: tuple[int, ...] = (0, 1000)) -> PreparedIoTPlan:
    spec = IoTSourceSpec(
        run_id="publisher-test",
        network_run_id="network-test",
        sensor_period_seconds=1,
    )
    digest = "a" * 64
    events = tuple(
        IoTSourceEvent(
            run_id=spec.run_id,
            source="amber",
            profile="transport-v1",
            profile_digest=digest,
            planned_at_ms=planned,
            sensor_id=f"sensor-{index:02d}",
            sequence=sequence,
            value_milli=index * 1000 + sequence,
            transmitted=True,
            decoded=True,
            outcome="decoded",
        )
        for sequence, planned in enumerate(planned_at_ms, start=1)
        for index in range(1, 11)
    )
    evidence = root / "iot-evidence-v2.json"
    evidence.write_text(
        '{"schema":"synthran/iot-evidence/v2alpha1","run_id":"publisher-test"}\n',
        encoding="utf-8",
    )
    return PreparedIoTPlan(
        spec=spec,
        duration_seconds=2,
        amber_commit="0" * 40,
        profile_digest=digest,
        energy_trace_sha256=None,
        events=events,
        scenario_path=root / "iot-scenario-v2.json",
        source_jsonl_path=root / "amber-source-events.jsonl",
        source_parquet_path=root / "amber-source-events.parquet",
        evidence_path=evidence,
    )


class AmberReplayTests(unittest.TestCase):
    def test_replay_uses_ten_clients_canaries_qos_and_sensor_order(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            records: list[tuple[str, str, int]] = []
            clients: list[FakeClient] = []

            def factory(client_id: str) -> FakeClient:
                client = FakeClient(client_id, records)
                clients.append(client)
                return client

            barrier = Barrier()
            session = AmberReplaySession(
                plan=make_plan(root),
                endpoint=MQTTEndpoint("127.0.0.1", 18886),
                collector_barrier=barrier,
                client_factory=factory,
                clock=FakeClock(),
            ).start()
            evidence = session.wait(timeout=2.0)
            session.stop()

            self.assertEqual(1, barrier.ready_calls)
            self.assertEqual(1, barrier.start_calls)
            self.assertEqual(10, len(barrier.sensor_ids))
            self.assertEqual(10, evidence.client_count)
            self.assertEqual(10, evidence.connected_clients)
            self.assertEqual(10, evidence.start_canaries)
            self.assertEqual(10, evidence.end_canaries)
            self.assertEqual(20, evidence.published_events)
            self.assertEqual(20, len(session.published_pairs()))
            self.assertTrue(evidence.timing_valid)
            self.assertTrue(evidence.complete)
            self.assertTrue(all(client.disconnected for client in clients))

            start_records = [record for record in records if "/control/" in record[0]][:10]
            telemetry = [record for record in records if "/sensor/" in record[0]]
            end_records = [record for record in records if "/control/" in record[0]][10:]
            self.assertEqual(10, len(start_records))
            self.assertEqual(20, len(telemetry))
            self.assertEqual(10, len(end_records))
            self.assertTrue(all(record[2] == 1 for record in start_records + end_records))
            self.assertTrue(all(record[2] == 0 for record in telemetry))
            self.assertEqual(
                [f"synthran/publisher-test/sensor/sensor-{index:02d}" for index in range(1, 11)],
                [record[0] for record in telemetry[:10]],
            )
            self.assertTrue((root / "publisher-events.jsonl").is_file())

    def test_pacing_failure_is_instrumentation_failure(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            records: list[tuple[str, str, int]] = []

            session = AmberReplaySession(
                plan=make_plan(root, planned_at_ms=(1000,)),
                endpoint=MQTTEndpoint("127.0.0.1", 18886),
                collector_barrier=Barrier(),
                client_factory=lambda client_id: FakeClient(client_id, records),
                clock=FakeClock(oversleep=0.60),
            ).start()
            try:
                with self.assertRaisesRegex(IoTSourceError, "timing exceeded"):
                    session.wait(timeout=2.0)
                evidence = session.evidence()
                self.assertGreater(evidence.p95_lag_ms, 250.0)
                self.assertFalse(evidence.timing_valid)
            finally:
                session.stop()

    def test_publish_failure_prevents_completion_and_cleans_clients(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            records: list[tuple[str, str, int]] = []
            clients: list[FakeClient] = []

            def factory(client_id: str) -> FakeClient:
                client = FakeClient(client_id, records, fail_kind="sensor-05")
                clients.append(client)
                return client

            session = AmberReplaySession(
                plan=make_plan(root),
                endpoint=MQTTEndpoint("127.0.0.1", 18886),
                collector_barrier=Barrier(),
                client_factory=factory,
                clock=FakeClock(),
            )
            with self.assertRaisesRegex(IoTSourceError, "sensor-05"):
                session.start()
            self.assertTrue(all(client.disconnected for client in clients))

    def test_missing_start_barrier_fails_closed_before_telemetry(self) -> None:
        class ReadyOnly:
            def wait_ready(self) -> None:
                pass

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            records: list[tuple[str, str, int]] = []
            session = AmberReplaySession(
                plan=make_plan(root),
                endpoint=MQTTEndpoint("127.0.0.1", 18886),
                collector_barrier=ReadyOnly(),
                client_factory=lambda client_id: FakeClient(client_id, records),
                clock=FakeClock(),
            )
            with self.assertRaisesRegex(IoTSourceError, "start-canary barrier"):
                session.start()
            self.assertEqual([], [record for record in records if "/sensor/" in record[0]])


if __name__ == "__main__":
    unittest.main()
