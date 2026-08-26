from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

from synthran.r2lab.foundation_topology import REQUIRED_PHYSICAL_NETWORK_ATTACHMENTS
from synthran.r2lab.hardware import PhysicalTopology
from synthran.r2lab.n3xx import (
    OPEN5GS_GNB_N2_N3_ADDRESS,
    OPEN5GS_N3_NETWORK,
    OPEN5GS_RU_NETWORK,
    R2LabN3xxError,
    _validate_render,
)
from synthran.r2lab.ue_ansible import (
    R2LabUeAnsibleError,
    _apply_connect_convergence,
    _harden_role_tree,
)


UPSTREAM_MBIM_BLOCK = '''        - name: "MBIM: stop.sh + start.sh on {{ ue_item }} if wwan0 not reachable"
          shell: >
            ssh -o ConnectTimeout=5 -o BatchMode=yes -o StrictHostKeyChecking=no
            root@{{ ue_item }}
            'stop.sh; start.sh -F {{ current_dnn }}'
          when:
            - ue_mode == 'mbim'
            - current_dnn is defined
            - not wwan0_up
          ignore_errors: "{{ ignore_task_errors | default(true) }}"
'''


class PhysicalFoundationOwnershipTests(unittest.TestCase):
    def test_open5gs_foundation_owns_only_n3network(self) -> None:
        self.assertEqual((OPEN5GS_N3_NETWORK,), REQUIRED_PHYSICAL_NETWORK_ATTACHMENTS)
        self.assertNotIn(OPEN5GS_RU_NETWORK, REQUIRED_PHYSICAL_NETWORK_ATTACHMENTS)

    def test_n3xx_render_still_requires_ru_network(self) -> None:
        topology = PhysicalTopology(
            core_node="sopnode-f2",
            ran_node="sopnode-f3",
            radio="n300",
            ue="qfit07",
        ).validate()
        ru_address = "192.168.235.240"
        valid = f'''
apiVersion: apps/v1
kind: Deployment
spec:
  replicas: 0
  strategy:
    type: Recreate
  template:
    spec:
      nodeName: sopnode-f3
      containers:
        - name: gnb
          image: example/srsran:locked
metadata:
  annotations:
    k8s.v1.cni.cncf.io/networks: '[{{"name":"{OPEN5GS_N3_NETWORK}","ips":["{OPEN5GS_GNB_N2_N3_ADDRESS}/24"]}},{{"name":"{OPEN5GS_RU_NETWORK}","ips":["{ru_address}/24"]}}]'
'''
        _validate_render(
            text=valid,
            topology=topology,
            repository="example/srsran",
            tag="locked",
            digest=None,
            ru_pod_address=ru_address,
        )
        with self.assertRaisesRegex(R2LabN3xxError, "RU network attachment"):
            _validate_render(
                text=valid.replace(
                    f'"name":"{OPEN5GS_RU_NETWORK}"', '"name":"missing-ru"'
                ),
                topology=topology,
                repository="example/srsran",
                tag="locked",
                digest=None,
                ru_pod_address=ru_address,
            )


class MbimConvergenceHardeningTests(unittest.TestCase):
    def test_upstream_connect_copy_is_convergent_and_strict(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            role = Path(directory) / "connect"
            path = role / "tasks" / "main.yml"
            path.parent.mkdir(parents=True)
            path.write_text("---\n" + UPSTREAM_MBIM_BLOCK, encoding="utf-8")

            _apply_connect_convergence(path)
            _harden_role_tree(role, slice_name="oulu_user")
            rendered = path.read_text(encoding="utf-8")

        self.assertNotIn("stop.sh; start.sh", rendered)
        self.assertEqual(1, rendered.count("'stop.sh'"))
        self.assertEqual(1, rendered.count("start.sh -F {{ current_dnn }} -q"))
        self.assertIn("until: mbim_start.rc == 0", rendered)
        self.assertIn("retries: 10", rendered)
        self.assertIn("delay: 3", rendered)
        self.assertIn("StrictHostKeyChecking=yes", rendered)
        self.assertIn("UserKnownHostsFile=/home/oulu_user/.ssh/known_hosts", rendered)
        self.assertNotIn("StrictHostKeyChecking=no", rendered)
        self.assertNotIn("UserKnownHostsFile=/dev/null", rendered)

    def test_connect_convergence_fails_closed_on_upstream_drift(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "main.yml"
            path.write_text("---\n# changed upstream\n", encoding="utf-8")
            with self.assertRaisesRegex(R2LabUeAnsibleError, "drifted"):
                _apply_connect_convergence(path)


if __name__ == "__main__":
    unittest.main()
