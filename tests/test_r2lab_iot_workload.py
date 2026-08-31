from __future__ import annotations

from contextlib import ExitStack
import inspect
import json
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from synthran.iot_source import MQTTEndpoint
from synthran.r2lab.iot_workload import (
    PhysicalIoTConfig,
    execute_physical_iot_workload,
)
from synthran.r2lab.ue import PhysicalWorkloadContext


class R2LabIoTWorkloadTests(unittest.TestCase):
    def _config(self, root: Path) -> PhysicalIoTConfig:
        known_hosts = root / "known_hosts"
        known_hosts.write_text("host key\n", encoding="utf-8")
        inventory = SimpleNamespace(
            core_node=SimpleNamespace(name="sopnode-f2"),
            ran_node=SimpleNamespace(name="sopnode-f3"),
        )
        return PhysicalIoTConfig(
            slice_name="slice-a",
            inventory=inventory,
            lock=MagicMock(),
            dependency_root=root / "deps",
            repository_root=root,
            known_hosts=known_hosts,
            workload_id="workload-001",
            run_root=root / "experiments",
            physical_run_root=root / "physical",
            collection_seconds=30,
            minimum_per_sensor=1,
        )

    def _execute(
        self,
        root: Path,
        *,
        source_loss: int = 0,
        accepted_connections: int = 10,
        tx_after: int = 200,
        delivery_reproof: bool = True,
        transport_cleanup: bool = True,
    ):
        config = self._config(root)
        context = PhysicalWorkloadContext(
            run_id="physical-001",
            ue="qfit07",
            interface="wwan0",
        )
        sensors = [f"sensor-{index:02d}" for index in range(1, 11)]
        events = [SimpleNamespace(key=(sensor, 1), decoded=True) for sensor in sensors]
        records = [
            {
                "schema": "synthran/telemetry/v1alpha1",
                "run_id": "workload-001",
                "sensor_id": sensor,
                "sequence": 1,
                "sensor_time_ms": 0,
                "value_milli": index * 1000 + 1,
            }
            for index, sensor in enumerate(sensors, start=1)
        ]

        def prepare(spec, duration, run_directory):
            evidence_path = run_directory / "iot-evidence-v2.json"
            evidence_path.write_text(
                json.dumps(
                    {
                        "schema": "synthran/iot-evidence/v2alpha1",
                        "run_id": spec.run_id,
                        "iot_source": "amber",
                        "iot_profile": spec.profile,
                        "iot_seed": spec.seed,
                        "profile_digest": "a" * 64,
                        "amber_commit": "b" * 40,
                        "live_transport": None,
                    }
                ),
                encoding="utf-8",
            )
            return SimpleNamespace(
                spec=spec,
                duration_seconds=duration,
                amber_commit="b" * 40,
                profile_digest="a" * 64,
                energy_trace_sha256=None,
                events=tuple(events),
                evidence_path=evidence_path,
                source_loss_count=source_loss,
            )

        source_adapter = MagicMock()
        source_adapter.prepare.side_effect = prepare
        collector = MagicMock()
        collector.start.return_value = collector
        collector.wait_expected_pairs.return_value = records
        collector.evidence.return_value.to_dict.return_value = {"records": 10}
        transport = MagicMock()
        transport.mqtt_endpoint = MQTTEndpoint("127.0.0.1", 18886)
        transport.snapshot.return_value = SimpleNamespace(
            accepted_connections=accepted_connections,
            upstream_bytes=100,
            to_dict=lambda: {
                "accepted_connections": accepted_connections,
                "upstream_bytes": 100,
            },
        )
        transport.evidence.return_value = {"cleanup_valid": transport_cleanup}
        transport_adapter = MagicMock()
        transport_adapter.start.return_value = transport
        publisher = MagicMock()
        publisher.start.return_value = publisher
        publisher.wait.return_value.to_dict.return_value = {"published": 10}
        publisher.published_pairs.return_value = [(sensor, 1) for sensor in sensors]
        reconciliation = SimpleNamespace(
            valid=True,
            to_dict=lambda: {
                "planned_count": 10,
                "decoded_count": 10 - source_loss,
                "source_loss_count": source_loss,
                "published_count": 10,
                "central_received_count": 10,
                "transport_loss_count": 0,
                "duplicate_count": 0,
            },
        )
        topology = MagicMock()
        topology.validate.return_value = SimpleNamespace(
            ue="qfit07",
            core_node="sopnode-f2",
            ran_node="sopnode-f3",
        )
        forward = MagicMock()
        reproof_values = [delivery_reproof, True] if delivery_reproof else [False, True]

        with ExitStack() as stack:
            stack.enter_context(patch.dict("os.environ", {"CONDA_DEFAULT_ENV": "synthran"}))
            stack.enter_context(
                patch("synthran.r2lab.iot_workload.sys.platform", "linux")
            )
            stack.enter_context(
                patch(
                    "synthran.r2lab.iot_workload._validate_ue",
                    return_value=SimpleNamespace(),
                )
            )
            stack.enter_context(
                patch("synthran.r2lab.iot_workload.load_topology", return_value=topology)
            )
            stack.enter_context(
                patch("synthran.r2lab.iot_workload._core_address", return_value="10.0.0.2")
            )
            stack.enter_context(patch("synthran.r2lab.iot_workload._probe_ssh_forwarding"))
            stack.enter_context(patch("synthran.r2lab.iot_workload._prove_ue_route"))
            stack.enter_context(
                patch(
                    "synthran.r2lab.iot_workload._ue_relay_process_count",
                    return_value=0,
                )
            )
            stack.enter_context(
                patch(
                    "synthran.r2lab.iot_workload._remote_port_is_closed",
                    return_value=True,
                )
            )
            stack.enter_context(
                patch(
                    "synthran.r2lab.iot_workload._local_port_is_closed",
                    return_value=True,
                )
            )
            stack.enter_context(
                patch(
                    "synthran.r2lab.iot_workload.AmberSourceAdapter",
                    return_value=source_adapter,
                )
            )
            stack.enter_context(
                patch(
                    "synthran.r2lab.iot_workload.render_physical_central_objects",
                    return_value=(),
                )
            )
            stack.enter_context(patch("synthran.r2lab.iot_workload._central_rollout"))
            stack.enter_context(
                patch(
                    "synthran.r2lab.iot_workload._ue_counter",
                    side_effect=[100, 50, tx_after, 60],
                )
            )
            stack.enter_context(
                patch(
                    "synthran.r2lab.iot_workload._local_forward_command",
                    return_value=("ssh", "forward"),
                )
            )
            stack.enter_context(
                patch("synthran.r2lab.iot_workload._start_process", return_value=forward)
            )
            stack.enter_context(patch("synthran.r2lab.iot_workload._wait_local_tcp"))
            stack.enter_context(
                patch(
                    "synthran.r2lab.iot_workload.PortableMqttCollectorSession",
                    return_value=collector,
                )
            )
            stack.enter_context(
                patch(
                    "synthran.r2lab.iot_workload.R2LabIoTTransportAdapter",
                    return_value=transport_adapter,
                )
            )
            stack.enter_context(
                patch(
                    "synthran.r2lab.iot_workload.AmberReplaySession",
                    return_value=publisher,
                )
            )
            stack.enter_context(
                patch(
                    "synthran.r2lab.iot_workload.reconcile_source_and_transport",
                    return_value=reconciliation,
                )
            )
            reproof = stack.enter_context(
                patch(
                    "synthran.r2lab.iot_workload._network_reproof",
                    side_effect=reproof_values,
                )
            )
            stack.enter_context(patch("synthran.r2lab.iot_workload.write_parquet"))
            stack.enter_context(
                patch("synthran.r2lab.iot_workload._delete_experiment_objects")
            )
            result = execute_physical_iot_workload(context, config=config)
        return result, config.run_root / config.workload_id, reproof

    def test_physical_executor_contains_no_legacy_sensor_network_runtime(self) -> None:
        source = inspect.getsource(execute_physical_iot_workload).lower()
        for marker in ("cooja", "tunslip6", "serialsocket", "6lowpan", "rpl"):
            self.assertNotIn(marker, source)

    def test_transport_delivery_accepts_only_with_cleanup_and_two_network_reproofs(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            result, run_directory, reproof = self._execute(Path(directory))
            self.assertTrue(result.accepted)
            self.assertTrue(result.cleanup_proven)
            self.assertEqual("wwan0", result.interface)
            self.assertEqual(2, reproof.call_count)
            summary = json.loads(
                (run_directory / "physical-workload.json").read_text(encoding="utf-8")
            )
            self.assertEqual("amber", summary["iot_source"])
            self.assertEqual("wwan0", summary["ue_interface"])
            self.assertTrue(summary["delivery_network_reproof_ready"])
            self.assertTrue(summary["cleanup_network_reproof_ready"])

    def test_transport_profile_rejects_source_loss(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            result, _, _ = self._execute(Path(directory), source_loss=1)
            self.assertFalse(result.accepted)

    def test_counted_ingress_requires_ten_connections(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            result, _, _ = self._execute(Path(directory), accepted_connections=9)
            self.assertFalse(result.accepted)

    def test_wwan0_tx_counter_must_increase(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            result, _, _ = self._execute(Path(directory), tx_after=100)
            self.assertFalse(result.accepted)

    def test_delivery_network_reproof_is_mandatory(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            result, _, _ = self._execute(Path(directory), delivery_reproof=False)
            self.assertFalse(result.accepted)

    def test_transport_cleanup_is_mandatory(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            result, _, _ = self._execute(Path(directory), transport_cleanup=False)
            self.assertFalse(result.accepted)
            self.assertFalse(result.cleanup_proven)


if __name__ == "__main__":
    unittest.main()
