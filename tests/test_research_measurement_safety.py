from __future__ import annotations

import inspect
import json
from pathlib import Path
from types import SimpleNamespace
import sys
import tempfile
import types
import unittest
from unittest.mock import MagicMock, patch

from synthran.experiment import ExperimentScenario
from synthran.live_preflight import CommandResult
from synthran.research import PROBE_SCHEMA, ResearchError, load_jsonl, probe_metrics
from synthran.research.collector import collect_mqtt_window
from synthran.research.instrumentation import (
    _IPERF_CONNECT_TIMEOUT_MS,
    _parse_probe_log,
    _prove_target_reachability,
    _start_load_client,
    _wait_load_client_connected,
)
from synthran.research.iperf import _listener_ready
from synthran.research.path import require_network_ready
from synthran.research.sampling import _future_deadline


class _FakeClient:
    def __init__(self, **kwargs):
        self.on_connect = None
        self.on_message = None

    def connect(self, host, port, keepalive):
        return None

    def loop_start(self):
        if self.on_connect is not None:
            self.on_connect(self, None, None, 0, None)

    def subscribe(self, topic, qos=0):
        return None

    def disconnect(self):
        return None

    def loop_stop(self):
        return None


def _paho_modules():
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


class MeasurementCollectorTests(unittest.TestCase):
    def _scenario(self) -> ExperimentScenario:
        return ExperimentScenario(
            run_id="research-safe-01",
            network_run_id="network-accepted",
            pdu_address="12.1.0.2",
        )

    def test_zero_delivery_window_persists_empty_telemetry_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "telemetry.jsonl"
            with (
                patch.dict(sys.modules, _paho_modules()),
                patch("synthran.research.collector.time.monotonic", side_effect=[0.0, 0.0, 2.0]),
            ):
                result = collect_mqtt_window(
                    self._scenario(),
                    host="127.0.0.1",
                    port=18885,
                    jsonl_path=path,
                    rejected_path=Path(temporary) / "rejected.jsonl",
                    duration_seconds=1,
                )
            self.assertTrue(result.completed)
            self.assertEqual(result.records, 0)
            self.assertTrue(path.is_file())
            self.assertEqual(path.read_text(encoding="utf-8"), "")

    def test_health_failure_aborts_window(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            with (
                patch.dict(sys.modules, _paho_modules()),
                patch("synthran.research.collector.time.monotonic", side_effect=[0.0, 0.0]),
                self.assertRaisesRegex(ResearchError, "instrument stopped"),
            ):
                collect_mqtt_window(
                    self._scenario(),
                    host="127.0.0.1",
                    port=18885,
                    jsonl_path=Path(temporary) / "telemetry.jsonl",
                    rejected_path=Path(temporary) / "rejected.jsonl",
                    duration_seconds=1,
                    health_check=lambda: (_ for _ in ()).throw(ResearchError("instrument stopped")),
                )


class ProbeEvidenceTests(unittest.TestCase):
    def test_structured_probe_keeps_total_packet_loss_as_attempts(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            log = root / "probe.log"
            destination = root / "probe.jsonl"
            lines = []
            for sequence in range(1, 4):
                lines.append(
                    json.dumps(
                        {
                            "schema": PROBE_SCHEMA,
                            "sequence": sequence,
                            "elapsed_seconds": float(sequence - 1),
                            "observed_at_utc": f"2026-08-17T10:00:0{sequence}Z",
                            "rtt_ms": None,
                            "timeout": True,
                        }
                    )
                )
            log.write_text("\n".join(lines) + "\n", encoding="utf-8")
            _parse_probe_log(log, destination, interval_seconds=1.0)
            records = load_jsonl(destination, schema=PROBE_SCHEMA)
        metrics = probe_metrics(records)
        self.assertEqual(metrics["samples"], 3)
        self.assertEqual(metrics["successful"], 0)
        self.assertEqual(metrics["timeouts"], 3)
        self.assertEqual(metrics["timeout_ratio"], 1.0)
        self.assertIsNone(metrics["rtt_ms"]["mean"])

    def test_pre_window_target_probe_fails_closed(self) -> None:
        with (
            patch("synthran.research.instrumentation._kubectl_exec_command", return_value=("ssh",)),
            patch("synthran.research.instrumentation.base_runtime._run", return_value=CommandResult(1, "", "")),
            self.assertRaisesRegex(ResearchError, "not reachable"),
        ):
            _prove_target_reachability(object(), "ue-pod", target="192.0.2.1")


class LoadReadinessTests(unittest.TestCase):
    def test_load_client_early_exit_fails_without_waiting_for_window(self) -> None:
        managed = MagicMock()
        managed.process.poll.return_value = 1
        with self.assertRaisesRegex(ResearchError, "before its control connection"):
            _wait_load_client_connected(
                inventory=object(),
                ue_pod="ue-pod",
                pdu_address="12.1.0.2",
                target="192.0.2.1",
                port=5201,
                process=managed,
            )

    def test_pre_window_readiness_budget_exceeds_iperf_connect_timeout(self) -> None:
        wait_default = inspect.signature(_wait_load_client_connected).parameters["timeout_seconds"].default
        self.assertGreater(wait_default * 1000, _IPERF_CONNECT_TIMEOUT_MS)
        self.assertEqual(15000, _IPERF_CONNECT_TIMEOUT_MS)

    def test_load_client_uses_the_bounded_connect_timeout(self) -> None:
        managed = MagicMock()
        with (
            patch("synthran.research.instrumentation._kubectl_exec_command", return_value=("ssh", "load")) as command,
            patch("synthran.research.instrumentation.base_runtime._start_process", return_value=managed),
        ):
            result = _start_load_client(
                inventory=object(),
                ue_pod="ue-pod",
                pdu_address="12.1.0.2",
                target="192.0.2.1",
                port=5201,
                target_bps=1_000_000,
                protocol="udp",
                parallel_flows=2,
                duration_seconds=30,
                repository_root=Path("."),
                log_path=Path("load.log"),
            )
        self.assertIs(result, managed)
        args = command.call_args.args
        index = args.index("--connect-timeout")
        self.assertEqual(str(_IPERF_CONNECT_TIMEOUT_MS), args[index + 1])

    def test_owned_listener_probe_is_non_connecting_remote_inspection(self) -> None:
        with patch("synthran.research.iperf.base_runtime._remote", return_value="4321\n") as remote:
            self.assertTrue(
                _listener_ready(
                    object(),
                    pidfile="/tmp/synthran-research/run/iperf3-5201.pid",
                    port=5201,
                )
            )
        command = remote.call_args.args
        self.assertIn("python3", command)
        self.assertNotIn("connect", " ".join(str(item) for item in command[:3]))


class ResearchPathTests(unittest.TestCase):
    def test_network_gate_requires_handed_off_pdu(self) -> None:
        ready = SimpleNamespace(ready=True, pdu_address="12.1.0.2", checks=())
        with (
            patch("synthran.research.path.verify_network_path", return_value=ready),
            patch("synthran.research.path.discover_rfsim_ue_pod", return_value="ue-pod"),
        ):
            report = require_network_ready(
                inventory=object(),
                lock=object(),
                network_run_id="network-accepted",
                ue_pod="ue-pod",
                pdu_address="12.1.0.2",
            )
        self.assertIs(report, ready)

    def test_network_gate_rejects_pdu_drift(self) -> None:
        ready = SimpleNamespace(ready=True, pdu_address="12.1.0.3", checks=())
        with (
            patch("synthran.research.path.verify_network_path", return_value=ready),
            patch("synthran.research.path.discover_rfsim_ue_pod", return_value="ue-pod"),
            self.assertRaisesRegex(ResearchError, "PDU changed"),
        ):
            require_network_ready(
                inventory=object(),
                lock=object(),
                network_run_id="network-accepted",
                ue_pod="ue-pod",
                pdu_address="12.1.0.2",
            )


class SchedulingTests(unittest.TestCase):
    def test_missed_sampling_deadlines_advance_without_catch_up(self) -> None:
        self.assertEqual(_future_deadline(2.0, 1.0, 4.2), 5.0)
        self.assertEqual(_future_deadline(5.0, 1.0, 4.2), 5.0)


if __name__ == "__main__":
    unittest.main()
