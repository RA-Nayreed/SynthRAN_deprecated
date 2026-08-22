from __future__ import annotations

import socket
import sys
import unittest
from unittest.mock import patch

from synthran.r2lab.ue import PhysicalWorkloadContext, PhysicalWorkloadResult
from synthran.experiment.r2lab import (
    ManagedQfitRelay,
    PhysicalExperimentConfig,
    PhysicalExperimentScenario,
    build_physical_workload_executor,
    build_qfit_stdio_relay_command,
    physical_central_name,
    render_physical_central_objects,
    route_uses_wwan0,
)


class FakeLock:
    raw = {
        "containers": {
            "mosquitto": {
                "image": "docker.io/library/eclipse-mosquitto",
                "digest": "sha256:" + "a" * 64,
            }
        }
    }


class R2LabPhysicalScenarioTests(unittest.TestCase):
    def test_physical_scenario_preserves_iot_contract_without_virtual_ue_claims(self) -> None:
        scenario = PhysicalExperimentScenario(
            run_id="physical-iot-001",
            network_run_id="r2lab-run-001",
        )
        payload = scenario.to_dict()
        self.assertEqual("r2lab", payload["backend"])
        self.assertEqual("wwan0", payload["ue_interface"])
        self.assertEqual(10, payload["sensor_count"])
        self.assertEqual(10, len(scenario.exact_sensor_topics))
        self.assertEqual("synthran/physical-iot-001/sensor/+", scenario.sensor_topic)
        self.assertNotIn("pdu_address", payload)
        self.assertNotIn("tun_srsue1", str(payload))
        self.assertFalse(payload["raw_pdu_address_persisted"])

    def test_central_resource_is_run_owned_host_network_and_digest_locked(self) -> None:
        scenario = PhysicalExperimentScenario(
            run_id="physical-iot-001",
            network_run_id="r2lab-run-001",
        )
        config, deployment = render_physical_central_objects(
            scenario,
            lock=FakeLock(),  # type: ignore[arg-type]
            core_node="sopnode-f2",
        )
        expected_name = physical_central_name(scenario.run_id)
        self.assertEqual(expected_name, config["metadata"]["name"])
        self.assertEqual(expected_name, deployment["metadata"]["name"])
        pod = deployment["spec"]["template"]["spec"]
        self.assertTrue(pod["hostNetwork"])
        self.assertEqual("sopnode-f2", pod["nodeSelector"]["kubernetes.io/hostname"])
        image = pod["containers"][0]["image"]
        self.assertIn("@sha256:", image)
        self.assertEqual(18884, pod["containers"][0]["ports"][0]["hostPort"])


class R2LabPhysicalRelayTests(unittest.TestCase):
    def test_qfit_relay_command_is_strict_and_binds_outbound_socket_to_wwan0(self) -> None:
        command = build_qfit_stdio_relay_command(
            slice_name="oulu_user",
            qfit="qfit07",
            run_id="physical-iot-001",
            central_address="198.51.100.10",
        )
        rendered = " ".join(command)
        self.assertIn("StrictHostKeyChecking=yes", rendered)
        self.assertNotIn("StrictHostKeyChecking=no", rendered)
        self.assertNotIn("accept-new", rendered)
        self.assertIn("SO_BINDTODEVICE", rendered)
        self.assertIn("wwan0", rendered)
        self.assertIn("physical-iot-001", rendered)
        self.assertIn("198.51.100.10", rendered)

    def test_route_parser_requires_exact_wwan0_observation(self) -> None:
        destination = "198.51.100.10"
        self.assertTrue(
            route_uses_wwan0(
                '[{"dst":"198.51.100.10","dev":"wwan0","src":"12.1.1.2"}]',
                destination,
            )
        )
        self.assertFalse(
            route_uses_wwan0(
                '[{"dst":"198.51.100.10","dev":"eth0"}]',
                destination,
            )
        )
        self.assertFalse(route_uses_wwan0("not-json", destination))

    def test_local_relay_bridges_binary_stdio_without_qfit_or_network_dependencies(self) -> None:
        relay = ManagedQfitRelay(
            port=0,
            command=(
                sys.executable,
                "-c",
                "import sys; data=sys.stdin.buffer.read(); sys.stdout.buffer.write(data); sys.stdout.buffer.flush()",
            ),
        )
        relay.start()
        try:
            client = socket.create_connection(("127.0.0.1", relay.port), timeout=3)
            with client:
                client.sendall(b"mqtt-test-bytes")
                client.shutdown(socket.SHUT_WR)
                received = b""
                while True:
                    chunk = client.recv(4096)
                    if not chunk:
                        break
                    received += chunk
            self.assertEqual(b"mqtt-test-bytes", received)
        finally:
            relay.stop()


class R2LabPhysicalExecutorFactoryTests(unittest.TestCase):
    @patch("synthran.experiment.r2lab.execute_physical_iot_workload")
    def test_factory_returns_explicit_physical_executor(self, execute) -> None:
        expected = PhysicalWorkloadResult(
            run_id="r2lab-run-001",
            workload_id="physical-iot-001",
            backend="r2lab",
            interface="wwan0",
            evidence_sha256="f" * 64,
            accepted=True,
            cleanup_proven=True,
        )
        execute.return_value = expected
        config = PhysicalExperimentConfig(
            slice_name="oulu_user",
            inventory=object(),  # type: ignore[arg-type]
            lock=FakeLock(),  # type: ignore[arg-type]
            dependency_root=__import__("pathlib").Path("dependencies"),
            repository_root=__import__("pathlib").Path("."),
            workload_id="physical-iot-001",
        )
        executor = build_physical_workload_executor(config)
        context = PhysicalWorkloadContext(
            run_id="r2lab-run-001",
            qfit="qfit07",
            interface="wwan0",
            claim_sha256="d" * 64,
            package_sha256="a" * 64,
            render_sha256="c" * 64,
        )
        self.assertEqual(expected, executor(context))
        execute.assert_called_once_with(context, config=config)


if __name__ == "__main__":
    unittest.main()
