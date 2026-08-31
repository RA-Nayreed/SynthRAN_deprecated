from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from synthran.experiment import TELEMETRY_SCHEMA
from synthran.iot_collector import PortableMqttCollectorSession


class FakeMessage:
    def __init__(self, topic: str, payload: bytes) -> None:
        self.topic = topic
        self.payload = payload


class FakeCollectorClient:
    def __init__(self, client_id: str) -> None:
        self.client_id = client_id
        self.on_connect = None
        self.on_subscribe = None
        self.on_message = None
        self.on_disconnect = None
        self.subscriptions: list[tuple[str, int]] = []
        self.stopped = False
        self._mid = 0

    def connect(self, host: str, port: int, keepalive: int = 30) -> None:
        del host, port, keepalive
        assert self.on_connect is not None
        self.on_connect(self, None, None, 0, None)

    def loop_start(self) -> None:
        pass

    def loop_stop(self) -> None:
        self.stopped = True

    def disconnect(self) -> None:
        pass

    def subscribe(self, topic: str, qos: int = 0):
        self.subscriptions.append((topic, qos))
        self._mid += 1
        assert self.on_subscribe is not None
        self.on_subscribe(self, None, self._mid, [0], None)
        return 0, self._mid

    def emit(self, topic: str, value: dict) -> None:
        assert self.on_message is not None
        self.on_message(
            self,
            None,
            FakeMessage(
                topic,
                json.dumps(value, sort_keys=True).encode("utf-8"),
            ),
        )


class PortableCollectorTests(unittest.TestCase):
    def test_readiness_canaries_and_expected_telemetry_are_separate(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            holder: list[FakeCollectorClient] = []

            def factory(client_id: str) -> FakeCollectorClient:
                client = FakeCollectorClient(client_id)
                holder.append(client)
                return client

            session = PortableMqttCollectorSession(
                run_id="collector-test",
                sensor_count=10,
                topic_root="synthran/collector-test",
                host="127.0.0.1",
                port=18885,
                jsonl_path=root / "telemetry.jsonl",
                rejected_path=root / "rejected.jsonl",
                client_factory=factory,
                default_timeout_seconds=1.0,
            ).start()
            session.wait_ready()
            client = holder[0]
            self.assertEqual(
                [
                    ("synthran/collector-test/sensor/+", 0),
                    ("synthran/collector-test/control/+", 1),
                ],
                client.subscriptions,
            )

            for index in range(1, 11):
                sensor_id = f"sensor-{index:02d}"
                client.emit(
                    f"synthran/collector-test/control/{sensor_id}",
                    {
                        "schema": "synthran/iot-control/v2alpha1",
                        "run_id": "collector-test",
                        "sensor_id": sensor_id,
                        "kind": "start-canary",
                    },
                )
            session.wait_start_canaries()

            expected: list[tuple[str, int]] = []
            for index in range(1, 11):
                sensor_id = f"sensor-{index:02d}"
                expected.append((sensor_id, 1))
                client.emit(
                    f"synthran/collector-test/sensor/{sensor_id}",
                    {
                        "schema": TELEMETRY_SCHEMA,
                        "run_id": "collector-test",
                        "sensor_id": sensor_id,
                        "sequence": 1,
                        "sensor_time_ms": 0,
                        "value_milli": index * 1000 + 1,
                    },
                )
                client.emit(
                    f"synthran/collector-test/control/{sensor_id}",
                    {
                        "schema": "synthran/iot-control/v2alpha1",
                        "run_id": "collector-test",
                        "sensor_id": sensor_id,
                        "kind": "end-canary",
                    },
                )

            records = session.wait_expected_pairs(expected)
            session.wait_end_canaries()
            evidence = session.evidence()
            session.stop()

            self.assertEqual(10, len(records))
            self.assertTrue(evidence.connected)
            self.assertTrue(evidence.subscriptions_ready)
            self.assertEqual(10, evidence.start_canaries)
            self.assertEqual(10, evidence.end_canaries)
            self.assertEqual(10, evidence.records)
            self.assertEqual(0, evidence.rejected)
            self.assertTrue(client.stopped)

    def test_invalid_control_topic_is_rejected_without_becoming_ready(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            holder: list[FakeCollectorClient] = []

            def factory(client_id: str) -> FakeCollectorClient:
                client = FakeCollectorClient(client_id)
                holder.append(client)
                return client

            session = PortableMqttCollectorSession(
                run_id="collector-test",
                sensor_count=10,
                topic_root="synthran/collector-test",
                host="127.0.0.1",
                port=18885,
                jsonl_path=root / "telemetry.jsonl",
                rejected_path=root / "rejected.jsonl",
                client_factory=factory,
            ).start()
            session.wait_ready(timeout=1.0)
            holder[0].emit(
                "synthran/collector-test/control/sensor-02",
                {
                    "run_id": "collector-test",
                    "sensor_id": "sensor-01",
                    "kind": "start-canary",
                },
            )
            evidence = session.evidence()
            session.stop()
            self.assertEqual(0, evidence.start_canaries)
            self.assertEqual(1, evidence.rejected)
            self.assertTrue((root / "rejected.jsonl").is_file())


if __name__ == "__main__":
    unittest.main()
