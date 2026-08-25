from __future__ import annotations

import json
from pathlib import Path
import unittest

from synthran.backends import RunCommandAdapter, backend_for_argv
from synthran.cli import _parser
from synthran.dependencies import load_lock
from synthran.live_preflight import CommandResult
from synthran.r2lab.foundation_topology import _physical_networks_ready
from synthran.r2lab.hardware import PhysicalTopology
from synthran.r2lab.n3xx import (
    OPEN5GS_AMF_N2_ADDRESS,
    OPEN5GS_GNB_N2_N3_ADDRESS,
    OPEN5GS_N3_NETWORK,
    OPEN5GS_RU_NETWORK,
    R2LabN3xxError,
    _generated_values,
    _locked_image,
    _validate_render,
)


def _topology() -> PhysicalTopology:
    return PhysicalTopology(
        core_node="sopnode-f2",
        ran_node="sopnode-f3",
        radio="n300",
        ue="qfit07",
    ).validate()


class UnifiedRunTests(unittest.TestCase):
    def test_run_command_is_backend_selecting_operator_surface(self) -> None:
        adapter = backend_for_argv(("run", "--radio", "r2lab"))
        self.assertIsInstance(adapter, RunCommandAdapter)

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
        self.assertEqual(physical.command, "run")
        self.assertEqual(physical.radio, "r2lab")
        self.assertEqual(physical.device, "n300")
        self.assertEqual(physical.ue, "qfit07")

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
            )
        )
        self.assertEqual(virtual.radio, "rfsim")
        self.assertIsNone(virtual.device)
        self.assertIsNone(virtual.ue)

    def test_n300_generated_values_restore_open5gs_runtime_network(self) -> None:
        lock = load_lock(Path("dependencies.lock.yml"))
        values = _generated_values(topology=_topology(), lock=lock)

        self.assertEqual(values["namespace"], "open5gs")
        self.assertEqual(values["n3networkName"], OPEN5GS_N3_NETWORK)
        self.assertEqual(OPEN5GS_N3_NETWORK, "n3network")
        self.assertEqual(values["gnbIp"], OPEN5GS_GNB_N2_N3_ADDRESS)
        self.assertEqual(OPEN5GS_GNB_N2_N3_ADDRESS, "10.10.3.234")
        self.assertEqual(
            values["gnbConfig"],
            {
                "cu_cp": {
                    "amf": {
                        "addr": OPEN5GS_AMF_N2_ADDRESS,
                        "port": 38412,
                        "bind_addr": OPEN5GS_GNB_N2_N3_ADDRESS,
                    }
                },
                "cu_up": {
                    "ngu": {
                        "socket": [
                            {"bind_addr": OPEN5GS_GNB_N2_N3_ADDRESS}
                        ]
                    }
                },
            },
        )

    def test_n3xx_render_rejects_empty_n3_attachment(self) -> None:
        topology = _topology()
        lock = load_lock(Path("dependencies.lock.yml"))
        repository, tag, digest = _locked_image(lock, topology.radio_profile)
        self.assertIsNotNone(digest)
        render = f'''\
apiVersion: apps/v1
kind: Deployment
spec:
  replicas: 0
  strategy:
    type: Recreate
  template:
    spec:
      nodeName: {topology.ran_node}
      containers:
        - name: gnb
          image: {repository}:{tag}@{digest}
      annotations:
        k8s.v1.cni.cncf.io/networks: |
          [
            {{ "name": "{OPEN5GS_N3_NETWORK}", "interface": "n3", "ips": [ "{OPEN5GS_GNB_N2_N3_ADDRESS}/24" ] }},
            {{ "name": "{OPEN5GS_RU_NETWORK}", "interface": "ru1", "ips": [ "192.168.235.240/24" ] }}
          ]
'''
        _validate_render(
            text=render,
            topology=topology,
            repository=repository,
            tag=tag,
            digest=digest,
            ru_pod_address="192.168.235.240",
        )

        broken = render.replace(f'"{OPEN5GS_N3_NETWORK}"', '""', 1)
        with self.assertRaisesRegex(
            R2LabN3xxError, "Open5GS N3 network attachment"
        ):
            _validate_render(
                text=broken,
                topology=topology,
                repository=repository,
                tag=tag,
                digest=digest,
                ru_pod_address="192.168.235.240",
            )

    def test_foundation_requires_n3_and_ru_network_attachments(self) -> None:
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
        self.assertEqual(observed, ("n3network", "ru-network"))

        ready, observed = _physical_networks_ready(
            topology=topology,
            known_hosts=Path("known-hosts"),
            runner=runner_with(("n2network", "ru-network")),
            timeout_seconds=30,
        )
        self.assertFalse(ready)
        self.assertEqual(observed, ("ru-network",))


if __name__ == "__main__":
    unittest.main()
