from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

from synthran.amber_experiment_runtime import (
    _edge_bridge_connected,
    _restart_edge_sidecar_and_wait,
    _wait_edge_bridge_connected,
    _wait_unique_remote_listener,
)


class AmberSidecarReadinessTests(unittest.TestCase):
    def test_restart_waits_for_new_ready_container_instance(self) -> None:
        inventory = object()
        restart = MagicMock()
        statuses = (
            (0, True, True, True),
            (0, False, False, False),
            (1, True, True, True),
        )

        with (
            patch(
                "synthran.amber_experiment_runtime._edge_sidecar_status",
                side_effect=statuses,
            ) as status,
            patch(
                "synthran.amber_experiment_runtime._restart_edge_sidecar",
                restart,
            ),
            patch("synthran.amber_experiment_runtime.time.sleep"),
        ):
            _restart_edge_sidecar_and_wait(inventory, "ue-pod", timeout_seconds=30)

        restart.assert_called_once_with(inventory, "ue-pod")
        self.assertEqual(status.call_count, 3)

    def test_bridge_probe_reads_shared_network_socket_state(self) -> None:
        inventory = object()
        with patch(
            "synthran.amber_experiment_runtime._remote",
            return_value="1\n",
        ) as remote:
            connected = _edge_bridge_connected(
                inventory,
                "ue-pod",
                pdu_address="12.1.0.2",
                central_address="10.10.0.2",
                central_port=18884,
            )

        self.assertTrue(connected)
        rendered = " ".join(str(value) for value in remote.call_args.args)
        self.assertIn("/proc/net/tcp", rendered)
        self.assertIn("12.1.0.2", rendered)
        self.assertIn("10.10.0.2", rendered)
        self.assertIn("18884", rendered)

    def test_bridge_wait_requires_established_connection(self) -> None:
        inventory = object()
        with (
            patch(
                "synthran.amber_experiment_runtime._edge_bridge_connected",
                side_effect=(False, False, True),
            ) as connected,
            patch("synthran.amber_experiment_runtime.time.sleep"),
        ):
            _wait_edge_bridge_connected(
                inventory,
                "ue-pod",
                pdu_address="12.1.0.2",
                central_address="10.10.0.2",
                central_port=18884,
                timeout_seconds=30,
            )

        self.assertEqual(connected.call_count, 3)

    def test_central_forward_wait_records_one_remote_listener_pid(self) -> None:
        inventory = object()
        process = MagicMock()
        process.name = "central-forward"
        process.process.poll.return_value = None
        with (
            patch(
                "synthran.amber_experiment_runtime._remote_listener_pids",
                side_effect=({18885: ()}, {18885: (321,)}),
            ) as listeners,
            patch("synthran.amber_experiment_runtime.time.sleep"),
        ):
            observed = _wait_unique_remote_listener(
                inventory,
                18885,
                process=process,
                timeout_seconds=30,
            )

        self.assertEqual((321,), observed)
        self.assertEqual(2, listeners.call_count)

    def test_central_forward_wait_rejects_ambiguous_remote_ownership(self) -> None:
        inventory = object()
        process = MagicMock()
        process.name = "central-forward"
        process.process.poll.return_value = None
        with patch(
            "synthran.amber_experiment_runtime._remote_listener_pids",
            return_value={18885: (321, 322)},
        ):
            with self.assertRaisesRegex(Exception, "ambiguous"):
                _wait_unique_remote_listener(
                    inventory,
                    18885,
                    process=process,
                    timeout_seconds=30,
                )


if __name__ == "__main__":
    unittest.main()
