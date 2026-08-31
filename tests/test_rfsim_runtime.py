from __future__ import annotations

import json
from pathlib import Path
import unittest
from unittest.mock import patch

from synthran.experiment import ExperimentError
from synthran.fiveg_ansible import InventoryHost, NetworkInventory
from synthran.live_preflight import CommandResult
from synthran.network.rfsim import (
    RFSIM_RECOVERY_ATTEMPTS,
    UE_TUNNEL_COMMAND_TIMEOUT_SECONDS,
    RfsimRuntimeState,
    _current_pdu_address,
    _deployment_owner_for_pod,
    _one_active_name,
    _rf_sample_stalled,
    _wait_for_ue_tunnel,
    reconcile_rfsim_runtime,
)


class RfsimRuntimeTests(unittest.TestCase):
    def _inventory(self) -> NetworkInventory:
        return NetworkInventory(
            path=Path("hosts.ini"),
            sha256="0" * 64,
            core_node=InventoryHost(
                "lab-core",
                {
                    "ansible_host": "192.0.2.10",
                    "ansible_user": "root",
                    "ip": "192.0.2.10",
                },
            ),
            ran_node=InventoryHost(
                "lab-ran",
                {
                    "ansible_host": "192.0.2.11",
                    "ansible_user": "root",
                    "ip": "192.0.2.11",
                },
            ),
            all_vars={},
        )

    def test_one_active_name_ignores_terminating_pod(self) -> None:
        payload = {
            "items": [
                {
                    "metadata": {
                        "name": "old",
                        "deletionTimestamp": "2026-08-16T14:00:00Z",
                    }
                },
                {"metadata": {"name": "current"}},
            ]
        }
        self.assertEqual(_one_active_name(payload, label="gNB pod"), "current")

    def test_current_pdu_address_accepts_live_address_in_slice_network(self) -> None:
        inventory = self._inventory()
        output = json.dumps(
            [
                {
                    "ifname": "tun_srsue1",
                    "addr_info": [
                        {
                            "family": "inet",
                            "local": "12.1.0.2",
                            "prefixlen": 24,
                        }
                    ],
                }
            ]
        )
        with patch(
            "synthran.network.rfsim._remote",
            return_value=output,
        ):
            self.assertEqual(
                _current_pdu_address(inventory, "ue-pod"),
                "12.1.0.2",
            )

    def test_current_pdu_address_rejects_address_outside_accepted_network(self) -> None:
        inventory = self._inventory()
        output = json.dumps(
            [
                {
                    "ifname": "tun_srsue1",
                    "addr_info": [{"family": "inet", "local": "10.0.0.8"}],
                }
            ]
        )
        with patch("synthran.network.rfsim._remote", return_value=output):
            with self.assertRaisesRegex(
                ExperimentError,
                "expected exactly one UE PDU address",
            ):
                _current_pdu_address(inventory, "ue-pod")

    def test_deployment_owner_is_resolved_through_replicaset(self) -> None:
        inventory = self._inventory()
        responses = [
            {
                "metadata": {
                    "ownerReferences": [
                        {"kind": "ReplicaSet", "name": "srsran-gnb-abc"}
                    ]
                }
            },
            {
                "metadata": {
                    "ownerReferences": [
                        {"kind": "Deployment", "name": "srsran-gnb"}
                    ]
                }
            },
        ]
        with patch(
            "synthran.network.rfsim._remote_json",
            side_effect=responses,
        ):
            self.assertEqual(
                _deployment_owner_for_pod(inventory, "gnb-pod"),
                "srsran-gnb",
            )

    def test_wait_for_ue_tunnel_accepts_delayed_attachment(self) -> None:
        inventory = self._inventory()
        captured: dict[str, object] = {}

        def fake_remote_result(inv, command, *, timeout_seconds=60):
            captured["command"] = command
            captured["timeout_seconds"] = timeout_seconds
            return CommandResult(0, "", "")

        with patch(
            "synthran.network.rfsim._remote_result",
            side_effect=fake_remote_result,
        ):
            _wait_for_ue_tunnel(inventory, "ue-pod")

        self.assertEqual(
            captured["timeout_seconds"],
            UE_TUNNEL_COMMAND_TIMEOUT_SECONDS,
        )
        command = str(captured["command"])
        self.assertIn("seq 1 60", command)
        self.assertIn("ip link show tun_srsue1", command)
        self.assertIn("pgrep -af", command)

    def test_wait_for_ue_tunnel_distinguishes_dead_process(self) -> None:
        inventory = self._inventory()
        with patch(
            "synthran.network.rfsim._remote_result",
            return_value=CommandResult(2, "", ""),
        ):
            with self.assertRaisesRegex(
                ExperimentError,
                "srsUE process exited before tun_srsue1 became ready",
            ):
                _wait_for_ue_tunnel(inventory, "ue-pod")

    def test_wait_for_ue_tunnel_reports_live_process_timeout(self) -> None:
        inventory = self._inventory()
        with patch(
            "synthran.network.rfsim._remote_result",
            return_value=CommandResult(1, "", ""),
        ):
            with self.assertRaisesRegex(
                ExperimentError,
                "timed out while the srsUE process remained alive",
            ):
                _wait_for_ue_tunnel(inventory, "ue-pod")

    def test_rf_sample_stall_requires_repeated_zero_progress(self) -> None:
        inventory = self._inventory()
        text = "\n".join(
            [
                "Waiting for data.",
                "Waiting for reading samples. Completed 0 of 23040 samples.",
            ]
            * 4
        )
        with patch(
            "synthran.network.rfsim._remote_result",
            return_value=CommandResult(0, text, ""),
        ):
            self.assertTrue(_rf_sample_stalled(inventory, "gnb-pod"))

    def test_reconcile_orders_stop_restart_broker_ue_route_and_returns_live_pdu(self) -> None:
        inventory = self._inventory()
        calls: list[str] = []
        discovery = iter(("ue-new", "gnb-old", "gnb-new"))

        def fake_discover(*args, **kwargs):
            return next(discovery)

        def fake_remote(inv, command, *, label, timeout_seconds=60):
            calls.append(label)
            return ""

        with (
            patch("synthran.network.rfsim._discover_pod", side_effect=fake_discover),
            patch(
                "synthran.network.rfsim._deployment_owner_for_pod",
                return_value="srsran-gnb",
            ),
            patch("synthran.network.rfsim._remote", side_effect=fake_remote),
            patch("synthran.network.rfsim._wait_for_gnb_cell") as wait_cell,
            patch("synthran.network.rfsim._wait_for_broker") as wait_broker,
            patch("synthran.network.rfsim._wait_for_ue_tunnel") as wait_tunnel,
            patch(
                "synthran.network.rfsim._current_pdu_address",
                return_value="12.1.0.2",
            ),
        ):
            state = reconcile_rfsim_runtime(
                inventory,
                network_run_id="network-run-01",
            )

        self.assertEqual(state.ue_pod, "ue-new")
        self.assertEqual(state.gnb_pod, "gnb-new")
        self.assertEqual(state.gnb_deployment, "srsran-gnb")
        self.assertEqual(state.pdu_address, "12.1.0.2")
        self.assertEqual(
            calls,
            [
                "stale RFSIM process cleanup",
                "gNB runtime restart request",
                "gNB runtime rollout",
                "RFSIM tmux session creation",
                "GNU Radio broker start",
                "srsUE start",
                "srsUE route restoration",
            ],
        )
        wait_cell.assert_called_once_with(inventory, "gnb-new")
        wait_broker.assert_called_once_with(inventory, "ue-new")
        wait_tunnel.assert_called_once_with(inventory, "ue-new")

    def test_reconcile_retries_complete_recovery_after_stalled_attach(self) -> None:
        inventory = self._inventory()
        discovery = iter(("ue-new", "gnb-old", "gnb-attempt-1", "gnb-attempt-2"))
        tunnel_attempts = iter((ExperimentError("stalled attach"), None))
        labels: list[str] = []

        def fake_discover(*args, **kwargs):
            return next(discovery)

        def fake_remote(inv, command, *, label, timeout_seconds=60):
            labels.append(label)
            return ""

        def fake_wait_tunnel(*args, **kwargs):
            outcome = next(tunnel_attempts)
            if outcome is not None:
                raise outcome

        with (
            patch("synthran.network.rfsim._discover_pod", side_effect=fake_discover),
            patch(
                "synthran.network.rfsim._deployment_owner_for_pod",
                return_value="srsran-gnb",
            ),
            patch("synthran.network.rfsim._remote", side_effect=fake_remote),
            patch("synthran.network.rfsim._wait_for_gnb_cell"),
            patch("synthran.network.rfsim._wait_for_broker"),
            patch("synthran.network.rfsim._wait_for_ue_tunnel", side_effect=fake_wait_tunnel),
            patch(
                "synthran.network.rfsim._current_pdu_address",
                return_value="12.1.0.6",
            ),
        ):
            state = reconcile_rfsim_runtime(
                inventory,
                network_run_id="network-run-01",
            )

        self.assertEqual(state.gnb_pod, "gnb-attempt-2")
        self.assertEqual(state.pdu_address, "12.1.0.6")
        self.assertEqual(labels.count("stale RFSIM process cleanup"), 2)
        self.assertEqual(labels.count("gNB runtime restart request"), 2)
        self.assertEqual(labels.count("GNU Radio broker start"), 2)
        self.assertEqual(labels.count("srsUE start"), 2)

    def test_reconcile_can_succeed_on_third_complete_attempt(self) -> None:
        inventory = self._inventory()
        state = RfsimRuntimeState(
            ue_pod="ue-new",
            gnb_pod="gnb-third",
            gnb_deployment="srsran-gnb",
            pdu_address="12.1.0.7",
        )
        with (
            patch(
                "synthran.network.rfsim._discover_pod",
                side_effect=("ue-new", "gnb-old"),
            ),
            patch(
                "synthran.network.rfsim._deployment_owner_for_pod",
                return_value="srsran-gnb",
            ),
            patch(
                "synthran.network.rfsim._reconcile_attempt",
                side_effect=(
                    ExperimentError("first stall"),
                    ExperimentError("second stall"),
                    state,
                ),
            ) as attempt,
        ):
            actual = reconcile_rfsim_runtime(
                inventory,
                network_run_id="network-run-01",
            )
        self.assertEqual(actual, state)
        self.assertEqual(attempt.call_count, 3)

    def test_reconcile_reports_all_failed_attempts(self) -> None:
        inventory = self._inventory()
        discovery = iter(("ue-new", "gnb-old", "gnb-1", "gnb-2", "gnb-3"))

        with (
            patch("synthran.network.rfsim._discover_pod", side_effect=lambda *a, **k: next(discovery)),
            patch(
                "synthran.network.rfsim._deployment_owner_for_pod",
                return_value="srsran-gnb",
            ),
            patch("synthran.network.rfsim._remote", return_value=""),
            patch("synthran.network.rfsim._wait_for_gnb_cell"),
            patch("synthran.network.rfsim._wait_for_broker"),
            patch(
                "synthran.network.rfsim._wait_for_ue_tunnel",
                side_effect=ExperimentError("stalled attach"),
            ),
        ):
            with self.assertRaisesRegex(
                ExperimentError,
                f"failed after {RFSIM_RECOVERY_ATTEMPTS} attempts",
            ):
                reconcile_rfsim_runtime(
                    inventory,
                    network_run_id="network-run-01",
                )

    def test_reconcile_persists_network_ownership_on_restarted_gnb(self) -> None:
        inventory = self._inventory()
        commands: list[str] = []
        discovery = iter(("ue-new", "gnb-old", "gnb-new"))

        def fake_remote(inv, command, *, label, timeout_seconds=60):
            commands.append(command)
            return ""

        with (
            patch("synthran.network.rfsim._discover_pod", side_effect=lambda *a, **k: next(discovery)),
            patch(
                "synthran.network.rfsim._deployment_owner_for_pod",
                return_value="srsran-gnb",
            ),
            patch("synthran.network.rfsim._remote", side_effect=fake_remote),
            patch("synthran.network.rfsim._wait_for_gnb_cell"),
            patch("synthran.network.rfsim._wait_for_broker"),
            patch("synthran.network.rfsim._wait_for_ue_tunnel"),
            patch(
                "synthran.network.rfsim._current_pdu_address",
                return_value="12.1.0.2",
            ),
        ):
            reconcile_rfsim_runtime(inventory, network_run_id="network-run-01")

        patch_command = next(
            command for command in commands if "kubectl patch deployment" in command
        )
        self.assertIn("synthran.run/id", patch_command)
        self.assertIn("network-run-01", patch_command)
        self.assertIn("kubectl.kubernetes.io/restartedAt", patch_command)


if __name__ == "__main__":
    unittest.main()
