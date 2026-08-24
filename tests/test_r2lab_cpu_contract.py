from __future__ import annotations

import hashlib
from pathlib import Path
import unittest

from synthran.dependencies import load_lock
from synthran.r2lab.deployment import (
    PHYSICAL_GNB_CPU_COUNT,
    PHYSICAL_GNB_MEMORY,
    PhysicalChartBindings,
    R2LabPhysicalHelmError,
    build_physical_chart_bundle,
    build_physical_deployment_plan,
    validate_physical_helm_render,
)


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
SOURCE_VALUES = """gnbConfig:
  ru_sdr:
    device_driver: uhd
    device_args: addr=192.0.2.103,name=ni-n3xx-31D98C7,product=n300,num_recv_frames=32,num_send_frames=32,recv_frame_size=8000,send_frame_size=8000
    srate: 61.44
    tx_gain: 35
    rx_gain: 60
  cell_cfg:
    dl_arfcn: 640000
    band: 78
    channel_bandwidth_MHz: 20
    common_scs: 30
    pdcch:
      common:
        ss0_index: 0
        coreset0_index: 12
    prach:
      prach_config_index: 1
"""


class R2LabPhysicalCpuContractTests(unittest.TestCase):
    def setUp(self) -> None:
        lock = load_lock(REPOSITORY_ROOT / "dependencies.lock.yml")
        self.bundle = build_physical_chart_bundle(
            lock=lock,
            plan=build_physical_deployment_plan(run_id="r2lab-cpu-contract"),
            bindings=PhysicalChartBindings(
                amf_n2_address="198.51.100.200",
                gnb_n2_address="198.51.100.234",
                n300_address="192.0.2.103",
                ru_pod_address="192.0.2.240",
                ru_subnet="192.0.2.0/24",
            ),
        )

    def _render(self, *, resources: bool = True, cpu_limit: str | None = None) -> str:
        image = self.bundle.values["image"]
        image_ref = f"{image['repository']}:{image['tag']}@{image['digest']}"
        resource_block = ""
        if resources:
            limit = cpu_limit or str(PHYSICAL_GNB_CPU_COUNT)
            resource_block = f"""
          resources:
            requests:
              memory: \"{PHYSICAL_GNB_MEMORY}\"
              cpu: \"{PHYSICAL_GNB_CPU_COUNT}\"
            limits:
              memory: \"{PHYSICAL_GNB_MEMORY}\"
              cpu: \"{limit}\"
"""
        return f"""apiVersion: apps/v1
kind: Deployment
spec:
  replicas: 0
  strategy:
    type: Recreate
  template:
    spec:
      containers:
        - name: gnb
          image: {image_ref}
{resource_block}---
apiVersion: v1
kind: ConfigMap
data:
  srsran-gnb.yaml: |
    ru_sdr:
      device_driver: uhd
      device_args: addr=192.0.2.103,name=ni-n3xx-31D98C7,product=n300,num_recv_frames=32,num_send_frames=32,recv_frame_size=8000,send_frame_size=8000
      srate: 61.44
      tx_gain: 35
      rx_gain: 60
    cell_cfg:
      dl_arfcn: 640000
      band: 78
      channel_bandwidth_MHz: 20
      common_scs: 30
      pdcch:
        common:
          ss0_index: 0
          coreset0_index: 12
      prach:
        prach_config_index: 1
"""

    def validate(self, text: str):
        return validate_physical_helm_render(
            text=text,
            bundle=self.bundle,
            source_values_text=SOURCE_VALUES,
            source_values_sha256=hashlib.sha256(SOURCE_VALUES.encode()).hexdigest(),
        )

    def test_bundle_requests_whole_equal_cpu_and_memory_resources(self) -> None:
        resources = self.bundle.values["resources"]
        self.assertTrue(resources["define"])
        request = resources["requests"]["tcpdump"]
        limit = resources["limits"]["tcpdump"]
        self.assertEqual(str(PHYSICAL_GNB_CPU_COUNT), request["cpu"])
        self.assertEqual(PHYSICAL_GNB_MEMORY, request["memory"])
        self.assertEqual(request, limit)
        self.assertEqual(8, PHYSICAL_GNB_CPU_COUNT)
        self.assertTrue(self.bundle.review["guaranteed_qos_requested"])
        self.assertTrue(self.bundle.review["exclusive_cpu_manager_eligible"])
        self.assertEqual(8, self.bundle.review["exclusive_cpu_count"])
        self.assertNotIn("cell_cfg", self.bundle.values["gnbConfig"])

    def test_offline_render_accepts_exact_guaranteed_resource_contract(self) -> None:
        evidence = self.validate(self._render())
        self.assertEqual(0, evidence.replicas)
        self.assertEqual("Recreate", evidence.strategy)
        self.assertEqual(640000, evidence.carrier_arfcn)

    def test_offline_render_rejects_missing_resource_contract(self) -> None:
        with self.assertRaisesRegex(R2LabPhysicalHelmError, "resource block"):
            self.validate(self._render(resources=False))

    def test_offline_render_rejects_nonmatching_cpu_limit(self) -> None:
        with self.assertRaisesRegex(R2LabPhysicalHelmError, "requests and limits"):
            self.validate(self._render(cpu_limit="7"))


if __name__ == "__main__":
    unittest.main()
