from __future__ import annotations

import io
import json
import os
from pathlib import Path
import tempfile
import unittest

from synthran.backends.run import _RunProgress
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

    def test_progress_persists_the_same_stream_shown_to_operator(self) -> None:
        previous = Path.cwd()
        with tempfile.TemporaryDirectory() as directory:
            os.chdir(directory)
            try:
                stream = io.StringIO()
                progress = _RunProgress(
                    stream=stream,
                    run_id="physical-001",
                    radio="r2lab",
                )
                progress.start("provider", "select SLICES context")
                print("[synthran] TASK: Open5GS core", file=progress.child_stream)
                progress.done("provider", "ready")
                progress.close()

                self.assertEqual(
                    stream.getvalue().splitlines(),
                    [
                        "→ provider: select SLICES context",
                        "[synthran] TASK: Open5GS core",
                        "✓ provider: ready",
                    ],
                )
                event_path = Path(".synthran/events/physical-001.jsonl")
                payloads = [json.loads(line) for line in event_path.read_text().splitlines()]
                self.assertEqual(
                    [item["message"] for item in payloads],
                    stream.getvalue().splitlines(),
                )
                self.assertTrue(all(item["radio"] == "r2lab" for item in payloads))
            finally:
                os.chdir(previous)

    def test_quiet_run_still_persists_events(self) -> None:
        previous = Path.cwd()
        with tempfile.TemporaryDirectory() as directory:
            os.chdir(directory)
            try:
                stream = io.StringIO()
                progress = _RunProgress(
                    enabled=False,
                    stream=stream,
                    run_id="virtual-001",
                    radio="rfsim",
                )
                progress.start("provider")
                progress.done("provider")
                progress.close()
                self.assertEqual("", stream.getvalue())
                self.assertEqual(
                    2,
                    len(Path(".synthran/events/virtual-001.jsonl").read_text().splitlines()),
                )
            finally:
                os.chdir(previous)

    def test_n300_generated_values_restore_open5gs_runtime_network(self) -> None:
        lock = load_lock(Path("dependencies.lock.yml"))
        values = _generated_values(topology=_topology(), lock=lock)
        self.assertEqual(values["namespace"], "open5gs")
        self.assertEqual(values["n3networkName"], OPEN5GS_N3_NETWORK)
        self.assertEqual(values["gnbIp"], OPEN5GS_GNB_N2_N3_ADDRESS)
        self.assertEqual(
            values["gnbConfig"]["cu_cp"]["amf"]["addr"],
            OPEN5GS_AMF_N2_ADDRESS,
        )

    def test_n3xx_render_rejects_empty_n3_attachment(self) -> None:
        topology = _topology()
        lock = load_lock(Path("dependencies.lock.yml"))
        repository, tag, digest = _locked_image(lock, topology.radio_profile)
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
        with self.assertRaisesRegex(R2LabN3xxError, "Open5GS N3 network attachment"):
            _validate_render(
                text=broken,
                topology=topology,
                repository=repository,
                tag=tag,
                digest=digest,
                ru_pod_address="192.168.235.240",
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
