from __future__ import annotations

from pathlib import Path
import unittest
from unittest.mock import MagicMock, patch

from synthran.experiment.rfsim_transport import (
    CORE_INGRESS_PORT,
    UE_INTERFACE,
    _core_local_forward,
    _install_route,
    _start_counted_ingress,
    _ue_relay_command,
)
from synthran.fiveg_ansible import InventoryHost, NetworkInventory
from synthran.iot_edge_transport import (
    PHYSICAL_UE_INTERFACE,
    _physical_local_forward_command,
    _start_core_ingress,
    prove_physical_ue_route,
    resolve_physical_ue,
)
from synthran.live_preflight import CommandResult


def inventory(*, physical: bool = False) -> NetworkInventory:
    ue_hosts = {}
    all_vars = {"core": "open5gs", "ran": "srsran", "rru": "rfsim"}
    if physical:
        all_vars["rru"] = "n300"
        ue_hosts = {
            "qfit07": InventoryHost(
                name="qfit07",
                variables={
                    "ansible_host": "qfit07",
                    "ansible_user": "root",
                    "ansible_ssh_common_args": (
                        "-o ProxyJump=slice@faraday.inria.fr "
                        "-o StrictHostKeyChecking=yes "
                        "-o UserKnownHostsFile=/tmp/known_hosts"
                    ),
                    "mode": "mbim",
                },
            )
        }
    return NetworkInventory(
        path=Path("inventory.ini"),
        sha256="a" * 64,
        core_node=InventoryHost(
            "sopnode-f2",
            {
                "ansible_user": "root",
                "ansible_host": "core.example",
                "ip": "172.28.2.77",
            },
        ),
        ran_node=InventoryHost(
            "sopnode-f3",
            {
                "ansible_user": "root",
                "ansible_host": "ran.example",
                "ip": "172.28.2.95",
            },
        ),
        all_vars=all_vars,
        ue_hosts=ue_hosts,
    )


class RfsimTransportTests(unittest.TestCase):
    def test_controller_forward_is_loopback_only(self) -> None:
        with patch(
            "synthran.experiment.rfsim_transport.ssh_command",
            return_value=("ssh", "root@core.example"),
        ):
            command = _core_local_forward(
                inventory(),
                local_port=18886,
                remote_port=18883,
            )
        rendered = " ".join(command)
        self.assertIn("ExitOnForwardFailure=yes", rendered)
        self.assertIn("127.0.0.1:18886:127.0.0.1:18883", rendered)
        self.assertNotIn("0.0.0.0", rendered)

    def test_transient_relay_binds_outbound_socket_to_pdu_and_tunnel(self) -> None:
        with patch(
            "synthran.experiment.rfsim_transport.ssh_command",
            return_value=("ssh", "root@core.example", "relay"),
        ) as mocked:
            _ue_relay_command(
                inventory(),
                "ue-pod",
                "12.1.0.8",
                "172.28.2.77",
                CORE_INGRESS_PORT,
                "synthran-rfsim-relay-run-1",
            )
        rendered = " ".join(str(value) for value in mocked.call_args.args)
        self.assertIn("SO_BINDTODEVICE", rendered)
        self.assertIn("tun_srsue1", rendered)
        self.assertIn("12.1.0.8", rendered)
        self.assertIn("172.28.2.77", rendered)
        self.assertEqual("tun_srsue1", UE_INTERFACE)

    def test_counted_ingress_binds_exact_core_address(self) -> None:
        process = MagicMock()
        with patch(
            "synthran.experiment.rfsim_transport.ssh_command",
            return_value=("ssh", "root@core.example", "ingress"),
        ) as ssh, patch(
            "synthran.experiment.rfsim_transport._start_process",
            return_value=process,
        ):
            result = _start_counted_ingress(
                inventory(),
                repository_root=Path("."),
                remote_workspace="/tmp/synthran/run-1",
                listen_host="172.28.2.77",
                listen_port=18886,
                target_port=18884,
                snapshot_path="/tmp/synthran/run-1/snapshot.json",
                log_path=Path("ingress.log"),
            )
        self.assertIs(result, process)
        rendered = " ".join(str(value) for value in ssh.call_args.args)
        self.assertIn("--listen-host 172.28.2.77", rendered)
        self.assertIn("--target-host 127.0.0.1", rendered)
        self.assertNotIn("--listen-host 127.0.0.1", rendered)
        self.assertNotIn("0.0.0.0", rendered)

    def test_route_install_never_replaces_existing_state(self) -> None:
        with patch(
            "synthran.experiment.rfsim_transport._route",
            side_effect=(
                [{"dst": "172.28.2.77", "dev": "eth0"}],
                [{"dst": "172.28.2.77", "dev": "tun_srsue1"}],
            ),
        ), patch(
            "synthran.experiment.rfsim_transport._kubectl_exec_command",
            side_effect=lambda _inventory, _pod, *command: tuple(command),
        ), patch(
            "synthran.experiment.rfsim_transport._run",
            return_value=CommandResult(0, "", ""),
        ) as run:
            self.assertTrue(_install_route(inventory(), "ue-pod", "172.28.2.77"))
        command = run.call_args.args[0]
        self.assertEqual(
            command,
            ("ip", "route", "add", "172.28.2.77/32", "dev", "tun_srsue1"),
        )
        self.assertNotIn("replace", command)


class PhysicalTransportTests(unittest.TestCase):
    def test_physical_ue_is_resolved_only_from_upstream_inventory(self) -> None:
        endpoint = resolve_physical_ue(inventory(physical=True), "qfit07")
        self.assertEqual("qfit07", endpoint.name)
        self.assertEqual("root", endpoint.user)
        self.assertEqual("mbim", endpoint.mode)
        self.assertIn("ProxyJump=slice@faraday.inria.fr", " ".join(endpoint.ssh_common_args))

    def test_unknown_physical_ue_fails_closed(self) -> None:
        with self.assertRaisesRegex(Exception, "not present in the upstream inventory"):
            resolve_physical_ue(inventory(physical=True), "qfit99")

    def test_physical_forward_uses_selected_ue_and_core_address(self) -> None:
        endpoint = resolve_physical_ue(inventory(physical=True), "qfit07")
        command = _physical_local_forward_command(
            endpoint,
            local_port=18886,
            target_host="172.28.2.77",
            target_port=18886,
        )
        rendered = " ".join(command)
        self.assertIn("ProxyJump=slice@faraday.inria.fr", rendered)
        self.assertIn("StrictHostKeyChecking=yes", rendered)
        self.assertIn("127.0.0.1:18886:172.28.2.77:18886", rendered)
        self.assertIn("root@qfit07", rendered)
        self.assertNotIn("0.0.0.0", rendered)

    def test_physical_counted_ingress_binds_exact_core_address(self) -> None:
        process = MagicMock()
        with patch(
            "synthran.iot_edge_transport.ssh_command",
            return_value=("ssh", "root@core.example", "ingress"),
        ) as ssh, patch(
            "synthran.iot_edge_transport._start_process",
            return_value=process,
        ):
            result = _start_core_ingress(
                inventory(physical=True),
                repository_root=Path("."),
                remote_workspace="/tmp/synthran/run-1",
                listen_host="172.28.2.77",
                listen_port=18886,
                target_port=18884,
                snapshot_path="/tmp/synthran/run-1/snapshot.json",
                log_path=Path("physical-ingress.log"),
            )
        self.assertIs(result, process)
        rendered = " ".join(str(value) for value in ssh.call_args.args)
        self.assertIn("--listen-host 172.28.2.77", rendered)
        self.assertIn("--target-host 127.0.0.1", rendered)
        self.assertNotIn("--listen-host 127.0.0.1", rendered)

    def test_physical_route_gate_requires_wwan0(self) -> None:
        endpoint = resolve_physical_ue(inventory(physical=True), "qfit07")
        with patch(
            "synthran.iot_edge_transport._run",
            return_value=CommandResult(0, '[{"dst":"172.28.2.77","dev":"wwan0"}]'),
        ):
            prove_physical_ue_route(endpoint, "172.28.2.77")
        self.assertEqual("wwan0", PHYSICAL_UE_INTERFACE)

        with patch(
            "synthran.iot_edge_transport._run",
            return_value=CommandResult(0, '[{"dst":"172.28.2.77","dev":"eth0"}]'),
        ):
            with self.assertRaisesRegex(Exception, "not routed through wwan0"):
                prove_physical_ue_route(endpoint, "172.28.2.77")


if __name__ == "__main__":
    unittest.main()
