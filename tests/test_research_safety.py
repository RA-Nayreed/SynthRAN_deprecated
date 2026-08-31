from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from synthran.command_runtime import _load_campaign
from synthran.experiment import ExperimentError
from synthran.fiveg_ansible import InventoryHost, NetworkInventory
from synthran.research import CampaignCondition, ResearchError, build_campaign, save_campaign
from synthran.research_iperf import (
    OwnedIperfServer,
    prove_measurement_peer,
    start_owned_iperf_server,
    stop_owned_iperf_server,
)
from synthran.research.iperf_toolchain import CONTROL_KEEPALIVE_ARG


class OwnedIperfLifecycleTests(unittest.TestCase):
    def _inventory(self) -> NetworkInventory:
        return NetworkInventory(
            path=Path("hosts.ini"),
            sha256="0" * 64,
            core_node=InventoryHost("sopnode-f2", {}),
            ran_node=InventoryHost("sopnode-f3", {}),
            all_vars={"core": "open5gs", "ran": "srsran", "rru": "rfsim"},
        )

    def test_start_defaults_to_external_ran_peer_and_run_owned_pidfile(self) -> None:
        inventory = self._inventory()
        managed = MagicMock()
        managed.process.poll.return_value = None
        locked_binary = "/tmp/synthran-tools/iperf-3.21/bin/iperf3"
        with (
            patch("synthran.research_iperf._reap") as reap,
            patch("synthran.research_iperf.base_runtime._remote"),
            patch(
                "synthran.research_iperf.prepare_locked_iperf_server",
                return_value=locked_binary,
            ) as prepare,
            patch(
                "synthran.research_iperf.base_runtime._start_process",
                return_value=managed,
            ) as start,
            patch(
                "synthran.research_iperf.base_runtime._remote_path_exists",
                return_value=True,
            ),
            patch(
                "synthran.research_iperf.ssh_command",
                return_value=("ssh", "iperf3"),
            ) as ssh,
        ):
            server = start_owned_iperf_server(
                inventory=inventory,
                owner_id="campaign-c01-b01-high",
                port=5201,
                repository_root=Path("."),
                log_path=Path("load-server.log"),
            )
        self.assertEqual(server.server_node, "sopnode-f3")
        self.assertEqual(
            server.pidfile,
            "/tmp/synthran-research/campaign-c01-b01-high/iperf3-5201.pid",
        )
        prepare.assert_called_once_with(
            inventory,
            server_node="sopnode-f3",
            repository_root=Path("."),
        )
        reap.assert_called_once_with(
            inventory,
            server_node="sopnode-f3",
            pidfile=server.pidfile,
            port=5201,
            orphan_only=True,
            label="stale research iperf3 recovery",
        )
        self.assertEqual(ssh.call_args.args[0].name, "sopnode-f3")
        self.assertIn(locked_binary, ssh.call_args.args)
        self.assertIn(CONTROL_KEEPALIVE_ARG, ssh.call_args.args)
        start.assert_called_once()

    def test_core_node_cannot_be_selected_as_measurement_server(self) -> None:
        with self.assertRaisesRegex(ExperimentError, "distinct from the 5G core"):
            start_owned_iperf_server(
                inventory=self._inventory(),
                server_node="sopnode-f2",
                owner_id="campaign-c01-b01-high",
                port=5201,
                repository_root=Path("."),
                log_path=Path("load-server.log"),
            )

    def test_explicit_peer_target_must_belong_to_selected_server(self) -> None:
        inventory = self._inventory()
        with patch(
            "synthran.research_iperf.base_runtime._remote",
            return_value=(
                "2: ens15f0np0    inet 172.28.2.95/26 metric 1024 "
                "brd 172.28.2.127 scope global dynamic ens15f0np0\n"
            ),
        ):
            prove_measurement_peer(
                inventory,
                server_node="sopnode-f3",
                target="172.28.2.95",
            )
            with self.assertRaisesRegex(ExperimentError, "is not assigned"):
                prove_measurement_peer(
                    inventory,
                    server_node="sopnode-f3",
                    target="172.28.2.77",
                )

    def test_stop_reaps_on_same_measurement_server_and_removes_workspace(self) -> None:
        inventory = self._inventory()
        process = MagicMock()
        server = OwnedIperfServer(
            owner_id="campaign-c01-b01-high",
            server_node="sopnode-f3",
            target="172.28.2.95",
            port=5201,
            workspace="/tmp/synthran-research/campaign-c01-b01-high",
            pidfile="/tmp/synthran-research/campaign-c01-b01-high/iperf3-5201.pid",
            process=process,
        )
        with (
            patch("synthran.research_iperf._reap") as reap,
            patch("synthran.research_iperf.base_runtime._remote") as remote,
        ):
            stop_owned_iperf_server(inventory, server)
        process.stop.assert_called_once_with()
        reap.assert_called_once_with(
            inventory,
            server_node="sopnode-f3",
            pidfile=server.pidfile,
            port=5201,
            orphan_only=False,
            label="run-owned research iperf3 cleanup",
        )
        self.assertEqual(remote.call_count, 2)
        self.assertEqual(remote.call_args_list[0].args[0].core_node.name, "sopnode-f3")
        self.assertEqual(
            remote.call_args_list[0].args[1:4], ("rm", "-f", server.pidfile)
        )
        self.assertEqual(
            remote.call_args_list[1].args[1:3], ("rmdir", server.workspace)
        )


class PersistedCampaignIntegrityTests(unittest.TestCase):
    def _campaign(self):
        return build_campaign(
            campaign_id="campaign-c01",
            network_run_id="network-accepted",
            seeds=(7, 17),
            conditions=(
                CampaignCondition("baseline"),
                CampaignCondition("load-80", load_fraction=0.8),
            ),
            campaign_seed=19,
        )

    def test_saved_campaign_round_trips_exact_schedule(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "campaign.json"
            campaign = self._campaign()
            save_campaign(campaign, path)
            loaded = _load_campaign(path)
            self.assertEqual(loaded.runs, campaign.runs)

    def test_mutated_persisted_schedule_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "campaign.json"
            save_campaign(self._campaign(), path)
            value = json.loads(path.read_text(encoding="utf-8"))
            original = value["runs"][0]["condition"]
            value["runs"][0]["condition"] = (
                "baseline" if original != "baseline" else "load-80"
            )
            path.write_text(json.dumps(value), encoding="utf-8")
            with self.assertRaisesRegex(ResearchError, "schedule"):
                _load_campaign(path)


if __name__ == "__main__":
    unittest.main()
