from __future__ import annotations

from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest.mock import patch

from synthran.experiment import ExperimentScenario
from synthran.research.collector import collect_mqtt_window


class _FakeClient:
    def __init__(self, **kwargs):
        self.on_connect = None
        self.on_message = None
        self.subscriptions = []

    def connect(self, host, port, keepalive):
        return None

    def loop_start(self):
        if self.on_connect is not None:
            self.on_connect(self, None, None, 0, None)

    def subscribe(self, topic, qos=0):
        self.subscriptions.append((topic, qos))

    def disconnect(self):
        return None

    def loop_stop(self):
        return None


class FixedWindowCollectorTests(unittest.TestCase):
    def _scenario(self) -> ExperimentScenario:
        return ExperimentScenario(
            run_id="baseline-s01-r01",
            network_run_id="network-accepted",
            pdu_address="12.1.0.8",
        )

    def _paho_modules(self):
        mqtt = types.ModuleType("paho.mqtt.client")
        mqtt.Client = _FakeClient
        mqtt.CallbackAPIVersion = types.SimpleNamespace(VERSION2=object())
        mqtt.MQTTv311 = object()
        mqtt_package = types.ModuleType("paho.mqtt")
        mqtt_package.client = mqtt
        paho = types.ModuleType("paho")
        paho.mqtt = mqtt_package
        return {
            "paho": paho,
            "paho.mqtt": mqtt_package,
            "paho.mqtt.client": mqtt,
        }

    def test_window_start_hook_runs_after_warmup_and_before_measurement(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            events = []
            with (
                patch.dict(sys.modules, self._paho_modules()),
                patch(
                    "synthran.research.collector.time.sleep",
                    side_effect=lambda seconds: events.append(("sleep", seconds)),
                ),
                patch(
                    "synthran.research.collector.time.monotonic",
                    side_effect=[0.0, 0.0, 2.0],
                ),
            ):
                result = collect_mqtt_window(
                    self._scenario(),
                    host="127.0.0.1",
                    port=18885,
                    jsonl_path=root / "telemetry.jsonl",
                    rejected_path=root / "rejected.jsonl",
                    duration_seconds=1,
                    warmup_seconds=3,
                    on_window_start=lambda: events.append(("start", None)),
                )
            self.assertTrue(result.completed)
            self.assertEqual(result.records, 0)
            self.assertEqual(events, [("sleep", 3), ("start", None)])

    def test_invalid_window_values_fail_before_mqtt_import(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with self.assertRaisesRegex(Exception, "duration"):
                collect_mqtt_window(
                    self._scenario(),
                    host="127.0.0.1",
                    port=18885,
                    jsonl_path=root / "telemetry.jsonl",
                    rejected_path=root / "rejected.jsonl",
                    duration_seconds=0,
                )
            with self.assertRaisesRegex(Exception, "warmup"):
                collect_mqtt_window(
                    self._scenario(),
                    host="127.0.0.1",
                    port=18885,
                    jsonl_path=root / "telemetry.jsonl",
                    rejected_path=root / "rejected.jsonl",
                    duration_seconds=1,
                    warmup_seconds=-1,
                )


if __name__ == "__main__":
    unittest.main()
