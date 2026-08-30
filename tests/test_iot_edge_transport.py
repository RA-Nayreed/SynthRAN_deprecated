from __future__ import annotations

from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from synthran.fiveg_ansible import InventoryHost, NetworkInventory
from synthran.iot_edge_transport import (
    RfsimEdgeTransportAdapter,
    RfsimEdgeTransportSession,
    _local_forward_command,
    _remote_listener_probe_command,
)
from synthran.iot_source import MQTTEndpoint


def inventory() -> NetworkInventory:
    return NetworkInventory(
        path=Path("inventory.ini"),
        sha256="a" * 64,
        core_node=InventoryHost(
            name="sopnode-f2",
            variables={"ansible_user": "root", "ansible_host": "core.example"},
        ),
        ran_node=InventoryHost(
            name="sopnode-f3",
            variables={"ansible_user": "root", "ansible_host": "ran.example"},
        ),
        all_vars={"core": "open5gs", "ran": "srsran", "rru": "rfsim"},
    )


class FakeProcess:
    def __init__(self, name: str, stopped: list[str]) -> None:
        self.name = name
        self.stopped = stopped
        self.process = self
        self.log_path = Path(f"{name}.log")

    def poll(self):
        return None

    def stop(self) -> None:
        self.stopped.append(self.name)


class RfsimEdgeTransportTests(unittest.TestCase):
    def test_local_forward_is_loopback_only_and_fail_closed(self) -> None:
        with patch(
            "synthran.iot_edge_transport._strict_ssh_base",
            return_value=(["ssh", "-o", "BatchMode=yes"], "root@core.example"),
        ):
            command = _local_forward_command(
                inventory(),
                local_port=18886,
                remote_port=18886,
            )
        rendered = " ".join(command)
        self.assertIn("ExitOnForwardFailure=yes", rendered)
        self.assertIn("127.0.0.1:18886:127.0.0.1:18886", rendered)
        self.assertIn("-N", command)
        self.assertNotIn("0.0.0.0", rendered)

    def test_listener_probe_reads_socket_tables_without_connecting(self) -> None:
        with patch(
            "synthran.iot_edge_transport.ssh_command",
            return_value=("ssh", "root@core.example", "python3"),
        ) as mocked_ssh:
            _remote_listener_probe_command(inventory(), port=18886)
        remote_args = mocked_ssh.call_args.args[1:]
        rendered = " ".join(str(value) for value in remote_args)
        self.assertIn("/proc/net/tcp", rendered)
        self.assertIn("49C6", rendered)
        self.assertNotIn("socket.connect", rendered)
        self.assertNotIn("connect((", rendered)

    def test_adapter_owns_edge_forward_ingress_and_local_forward(self) -> None:
        stopped: list[str] = []
        started: list[tuple[str, tuple[str, ...]]] = []

        def fake_start(name, command, *, cwd, log_path):
            del cwd, log_path
            started.append((name, tuple(command)))
            return FakeProcess(name, stopped)

        def fake_ssh(host, *remote):
            del host
            return ("ssh", "root@core.example", " ".join(remote))

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            adapter = RfsimEdgeTransportAdapter(
                inventory=inventory(),
                repository_root=root,
            )
            with patch(
                "synthran.iot_edge_transport._start_process",
                side_effect=fake_start,
            ), patch(
                "synthran.iot_edge_transport._wait_remote_tcp",
            ), patch(
                "synthran.iot_edge_transport._wait_remote_listener",
            ), patch(
                "synthran.iot_edge_transport._wait_local_tcp",
            ), patch(
                "synthran.iot_edge_transport._remote_port_is_closed",
                return_value=True,
            ), patch(
                "synthran.iot_edge_transport._local_port_is_closed",
                return_value=True,
            ), patch(
                "synthran.iot_edge_transport.ssh_command",
                side_effect=fake_ssh,
            ), patch(
                "synthran.iot_edge_transport._local_forward_command",
                return_value=(
                    "ssh",
                    "-N",
                    "-L",
                    "127.0.0.1:18886:127.0.0.1:18886",
                    "root@core.example",
                ),
            ):
                session = adapter.start(
                    run_id="amber-rfsim-test",
                    ue_pod="srsran-ue-test",
                    remote_workspace="/tmp/synthran/amber-rfsim-test",
                    run_directory=root / "run",
                )

            self.assertEqual(MQTTEndpoint("127.0.0.1", 18886), session.mqtt_endpoint)
            self.assertEqual(3, len(started))
            edge_text = " ".join(started[0][1])
            ingress_text = " ".join(started[1][1])
            local_text = " ".join(started[2][1])
            self.assertIn("--address 127.0.0.1", edge_text)
            self.assertIn("--listen-host 127.0.0.1", ingress_text)
            self.assertIn("--target-host 127.0.0.1", ingress_text)
            self.assertIn("--target-port 18883", ingress_text)
            self.assertIn("127.0.0.1:18886:127.0.0.1:18886", local_text)
            self.assertNotIn("0.0.0.0", edge_text + ingress_text + local_text)

    def test_busy_owned_ports_fail_closed_before_process_start(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            adapter = RfsimEdgeTransportAdapter(
                inventory=inventory(),
                repository_root=Path(directory),
            )
            with patch(
                "synthran.iot_edge_transport._remote_port_is_closed",
                return_value=False,
            ):
                with self.assertRaisesRegex(Exception, "already owned"):
                    adapter.start(
                        run_id="amber-rfsim-test",
                        ue_pod="srsran-ue-test",
                        remote_workspace="/tmp/synthran/amber-rfsim-test",
                        run_directory=Path(directory) / "run",
                    )

    def test_cleanup_stops_owned_processes_in_reverse_order(self) -> None:
        stopped: list[str] = []
        processes = [
            FakeProcess("edge", stopped),
            FakeProcess("ingress", stopped),
            FakeProcess("local", stopped),
        ]
        session = RfsimEdgeTransportSession(
            inventory=inventory(),
            endpoint=MQTTEndpoint("127.0.0.1", 18886),
            remote_ingress_port=18886,
            snapshot_remote_path="/tmp/synthran/test/snapshot.json",
            processes=processes,  # type: ignore[arg-type]
        )
        with patch(
            "synthran.iot_edge_transport._local_port_is_closed",
            return_value=True,
        ), patch(
            "synthran.iot_edge_transport._remote_port_is_closed",
            return_value=True,
        ):
            session.stop()
        self.assertEqual(["local", "ingress", "edge"], stopped)
        evidence = session.evidence()
        self.assertTrue(evidence["stopped"])
        self.assertTrue(evidence["cleanup_valid"])


if __name__ == "__main__":
    unittest.main()
