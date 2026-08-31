from __future__ import annotations

import json
from pathlib import Path
import unittest

from synthran.cli import _parser
from synthran.live_preflight import CommandResult
from synthran.r2lab.foundation_topology import _physical_networks_ready
from synthran.r2lab.hardware import PhysicalTopology


def _topology() -> PhysicalTopology:
    return PhysicalTopology(
        core_node="sopnode-f2",
        ran_node="sopnode-f3",
        radio="n300",
        ue="qfit07",
    ).validate()


class UnifiedRunTests(unittest.TestCase):
    def test_run_parser_selects_backend(self) -> None:
        physical = _parser().parse_args(
            (
                "run",
                "--radio",
                "r2lab",
                "--device",
                "n300",
                "--ue",
                "qfit07",
                "--core-node",
                "sopnode-f2",
                "--ran-node",
                "sopnode-f3",
                "--run-id",
                "physical-001",
            )
        )
        self.assertEqual("r2lab", physical.radio)
        self.assertEqual("n300", physical.device)
        self.assertEqual("qfit07", physical.ue)

        virtual = _parser().parse_args(
            (
                "run",
                "--radio",
                "rfsim",
                "--core-node",
                "sopnode-f2",
                "--ran-node",
                "sopnode-f3",
                "--run-id",
                "virtual-001",
                "--quiet",
            )
        )
        self.assertEqual("rfsim", virtual.radio)
        self.assertTrue(virtual.quiet)

    def test_physical_gnb_uses_upstream_role_boundary(self) -> None:
        playbook = Path("deploy/ansible/r2lab-srsran-gnb.yml").read_text(encoding="utf-8")
        self.assertIn("name: 5g/srsRAN/config", playbook)
        self.assertIn("name: 5g/srsRAN/deploy", playbook)
        self.assertIn("tasks_from: deploy_gnb.yml", playbook)
        self.assertIn("rru in [\"n300\", \"n320\"]", playbook)
        self.assertIn("synthran.run/id={{ synthran_run_id }}", playbook)
        self.assertIn(
            "synthran.io/deployment-authority=fiveg_ansible:{{ synthran_fiveg_ansible_commit }}",
            playbook,
        )

    def test_foundation_requires_only_open5gs_n3network(self) -> None:
        topology = _topology()

        def runner_with(names: tuple[str, ...]):
            payload = {"items": [{"metadata": {"name": name}} for name in names]}

            def run(_command, _timeout):
                return CommandResult(0, json.dumps(payload))

            return run

        ready, observed = _physical_networks_ready(
            topology=topology,
            known_hosts=Path("known-hosts"),
            runner=runner_with(("n2network", "n3network", "ru-network")),
            timeout_seconds=30,
        )
        self.assertTrue(ready)
        self.assertEqual(observed, ("n3network",))

        ready, observed = _physical_networks_ready(
            topology=topology,
            known_hosts=Path("known-hosts"),
            runner=runner_with(("n2network", "ru-network")),
            timeout_seconds=30,
        )
        self.assertFalse(ready)
        self.assertEqual(observed, ())


if __name__ == "__main__":
    unittest.main()
