from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import MagicMock, call, patch

from synthran.iot_source import MQTTEndpoint
from synthran.r2lab.iot_transport import (
    R2LAB_AMBER_INGRESS_PORT,
    R2LabIoTTransportAdapter,
    R2LabIoTTransportSession,
)


class R2LabIoTTransportTests(unittest.TestCase):
    def test_adapter_binds_counted_ingress_to_selected_ue_relay(self) -> None:
        inventory = SimpleNamespace(core_node=SimpleNamespace(name="sopnode-f2"))
        profile = SimpleNamespace()
        relay = MagicMock()
        relay.port = 18887
        processes = [MagicMock(name=f"process-{index}") for index in range(3)]
        for process in processes:
            process.process.poll.return_value = None
        with tempfile.TemporaryDirectory() as directory, patch(
            "synthran.r2lab.iot_transport._validate_ue", return_value=profile
        ), patch(
            "synthran.r2lab.iot_transport._core_address", return_value="10.0.0.2"
        ), patch(
            "synthran.r2lab.iot_transport._prove_ue_route"
        ) as prove_route, patch(
            "synthran.r2lab.iot_transport._ue_relay_process_count", return_value=0
        ), patch(
            "synthran.r2lab.iot_transport._local_port_is_closed", return_value=True
        ), patch(
            "synthran.r2lab.iot_transport._remote_port_is_closed", return_value=True
        ), patch(
            "synthran.r2lab.iot_transport._remote"
        ), patch(
            "synthran.r2lab.iot_transport._transfer_file"
        ), patch(
            "synthran.r2lab.iot_transport.build_physical_ue_stdio_relay_command",
            return_value=("ssh", "ue"),
        ) as relay_command, patch(
            "synthran.r2lab.iot_transport.ManagedPhysicalUeRelay", return_value=relay
        ), patch(
            "synthran.r2lab.iot_transport._ssh_reverse_tunnel_command",
            return_value=("ssh", "reverse"),
        ) as reverse_command, patch(
            "synthran.r2lab.iot_transport.ssh_command",
            return_value=("ssh", "ingress"),
        ), patch(
            "synthran.r2lab.iot_transport._local_forward_command",
            return_value=("ssh", "publisher"),
        ) as local_forward, patch(
            "synthran.r2lab.iot_transport._start_process",
            side_effect=processes,
        ), patch(
            "synthran.r2lab.iot_transport._wait_remote_listener"
        ) as wait_remote, patch(
            "synthran.r2lab.iot_transport._wait_local_tcp"
        ) as wait_local:
            adapter = R2LabIoTTransportAdapter(
                inventory=inventory,
                slice_name="slice-a",
                ue="qfit07",
                repository_root=Path(directory),
            )
            session = adapter.start(
                run_id="physical-iot-001",
                run_directory=Path(directory),
            )

        prove_route.assert_called_once_with("slice-a", profile, "10.0.0.2")
        relay_command.assert_called_once_with(
            slice_name="slice-a",
            ue="qfit07",
            run_id="physical-iot-001",
            central_address="10.0.0.2",
        )
        reverse_command.assert_called_once_with(
            inventory,
            remote_port=18883,
            local_port=18887,
        )
        self.assertEqual(
            [call(inventory, port=18883, process=processes[0]),
             call(inventory, port=R2LAB_AMBER_INGRESS_PORT, process=processes[1])],
            wait_remote.call_args_list,
        )
        local_forward.assert_called_once_with(
            inventory,
            local_port=R2LAB_AMBER_INGRESS_PORT,
            remote_port=R2LAB_AMBER_INGRESS_PORT,
        )
        wait_local.assert_called_once_with(
            R2LAB_AMBER_INGRESS_PORT,
            process=processes[2],
        )
        self.assertEqual(
            MQTTEndpoint("127.0.0.1", R2LAB_AMBER_INGRESS_PORT),
            session.mqtt_endpoint,
        )

    def test_session_cleanup_is_reverse_order_and_checks_all_owned_ports(self) -> None:
        inventory = MagicMock()
        relay = MagicMock()
        processes = [MagicMock(), MagicMock(), MagicMock()]
        events: list[str] = []
        for index, process in enumerate(processes):
            process.name = f"p{index}"
            process.stop.side_effect = lambda index=index: events.append(f"p{index}")
        relay.stop.side_effect = lambda: events.append("relay")
        session = R2LabIoTTransportSession(
            inventory=inventory,
            slice_name="slice-a",
            ue="qfit07",
            run_id="physical-iot-001",
            endpoint=MQTTEndpoint("127.0.0.1", 18886),
            relay_port=18887,
            remote_ingress_port=18886,
            snapshot_remote_path="/tmp/snapshot.json",
            remote_workspace="/tmp/synthran/physical-iot-001",
            processes=processes,
            relay=relay,
        )
        with patch.object(session, "snapshot", side_effect=RuntimeError("none")), patch(
            "synthran.r2lab.iot_transport._remote"
        ), patch(
            "synthran.r2lab.iot_transport._remote_path_exists", return_value=False
        ), patch(
            "synthran.r2lab.iot_transport._local_port_is_closed", return_value=True
        ) as local_closed, patch(
            "synthran.r2lab.iot_transport._remote_port_is_closed", return_value=True
        ) as remote_closed, patch(
            "synthran.r2lab.iot_transport._validate_ue", return_value=SimpleNamespace()
        ), patch(
            "synthran.r2lab.iot_transport._ue_relay_process_count", return_value=0
        ):
            session.stop()
            session.stop()

        self.assertEqual(["p2", "p1", "p0", "relay"], events)
        self.assertIn(call(18886), local_closed.call_args_list)
        self.assertIn(call(18887), local_closed.call_args_list)
        self.assertIn(call(inventory, 18886), remote_closed.call_args_list)
        self.assertIn(call(inventory, 18883), remote_closed.call_args_list)
        self.assertTrue(session.evidence()["cleanup_valid"])


if __name__ == "__main__":
    unittest.main()
