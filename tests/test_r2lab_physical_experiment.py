from __future__ import annotations

import socket
import sys
import unittest
from unittest.mock import patch

from synthran.r2lab.iot_resources import (
    physical_central_name,
    render_physical_central_objects,
)
from synthran.r2lab.iot_ue import (
    ManagedPhysicalUeRelay,
    build_physical_ue_stdio_relay_command,
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


class R2LabPhysicalResourceTests(unittest.TestCase):
    def test_central_resource_is_run_owned_host_network_and_digest_locked(self) -> None:
        run_id = "physical-iot-001"
        config, deployment = render_physical_central_objects(
            run_id,
            lock=FakeLock(),  # type: ignore[arg-type]
            core_node="sopnode-f1",
        )
        expected_name = physical_central_name(run_id)
        self.assertEqual(expected_name, config["metadata"]["name"])
        self.assertEqual(expected_name, deployment["metadata"]["name"])
        pod = deployment["spec"]["template"]["spec"]
        self.assertTrue(pod["hostNetwork"])
        self.assertEqual("sopnode-f1", pod["nodeSelector"]["kubernetes.io/hostname"])
        container = pod["containers"][0]
        self.assertIn("@sha256:", container["image"])
        self.assertEqual(18884, container["ports"][0]["hostPort"])
        readiness = container["readinessProbe"]
        self.assertNotIn("tcpSocket", readiness)
        command = " ".join(readiness["exec"]["command"])
        self.assertIn("/proc/net/tcp", command)
        self.assertIn(":49C4", command)


class R2LabPhysicalRelayTests(unittest.TestCase):
    @patch("synthran.r2lab.controller._configured_identity", return_value=None)
    def test_qfit_relay_is_strict_and_binds_to_wwan0(self, _identity) -> None:
        command = build_physical_ue_stdio_relay_command(
            slice_name="oulu_user",
            ue="qfit07",
            run_id="physical-iot-001",
            central_address="198.51.100.10",
        )
        rendered = " ".join(command)
        self.assertIn("StrictHostKeyChecking=yes", rendered)
        self.assertNotIn("StrictHostKeyChecking=no", rendered)
        self.assertIn("SO_BINDTODEVICE", rendered)
        self.assertIn("wwan0", rendered)

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

    def test_local_relay_bridges_binary_stdio_without_hardware(self) -> None:
        relay = ManagedPhysicalUeRelay(
            port=0,
            command=(
                sys.executable,
                "-c",
                "import sys; data=sys.stdin.buffer.read(); "
                "sys.stdout.buffer.write(data); sys.stdout.buffer.flush()",
            ),
        )
        relay.start()
        try:
            with socket.create_connection(("127.0.0.1", relay.port), timeout=3) as client:
                client.sendall(b"mqtt-test-bytes")
                client.shutdown(socket.SHUT_WR)
                received = b""
                while chunk := client.recv(4096):
                    received += chunk
            self.assertEqual(b"mqtt-test-bytes", received)
        finally:
            relay.stop()


if __name__ == "__main__":
    unittest.main()
