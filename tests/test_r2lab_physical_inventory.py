from __future__ import annotations

import hashlib
from pathlib import Path
import tempfile
import unittest

from synthran.r2lab.hardware import PhysicalTopology
from synthran.r2lab.physical_inventory import (
    R2LabPhysicalInventoryError,
    load_physical_inventory,
)


INVENTORY = '''[webshell]
localhost ansible_connection=local

[core_node]
sopnode-f2 ansible_user=root nic_interface=eth1 ip=192.0.2.10 storage=disk1

[ran_node]
sopnode-f3 ansible_user=root nic_interface=eth1 ip=192.0.2.11 storage=disk1 boot_mode=live

[monitor_node]

[sopnodes:children]
core_node
ran_node

[k8s_workers:children]
ran_node

[all:vars]
core="open5gs"
ran="srsRAN"
core_node_name="sopnode-f2"
ran_node_name="sopnode-f3"
rru="n300"
bridge_enabled=true
monitoring_enabled=false
'''


def topology(radio: str = "n300") -> PhysicalTopology:
    return PhysicalTopology(
        core_node="sopnode-f2",
        ran_node="sopnode-f3",
        radio=radio,
        ue="qfit07",
    ).validate()


class PhysicalInventoryTests(unittest.TestCase):
    def write_inventory(self, root: str, text: str = INVENTORY) -> Path:
        path = Path(root) / "hosts.ini"
        path.write_text(text, encoding="utf-8", newline="\n")
        return path

    def test_accepts_exact_physical_radio_and_preserves_truthful_digest(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = self.write_inventory(directory)
            inventory = load_physical_inventory(path, topology=topology())

        self.assertEqual("n300", inventory.radio)
        self.assertEqual("sopnode-f2", inventory.core_node.name)
        self.assertEqual("sopnode-f3", inventory.ran_node.name)
        self.assertEqual(hashlib.sha256(INVENTORY.encode()).hexdigest(), inventory.sha256)

    def test_rejects_radio_that_does_not_match_persisted_topology(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = self.write_inventory(directory)
            with self.assertRaisesRegex(
                R2LabPhysicalInventoryError,
                "radio does not match persisted topology",
            ):
                load_physical_inventory(path, topology=topology("n320"))

    def test_rejects_compute_node_drift(self) -> None:
        text = INVENTORY.replace("sopnode-f3", "sopnode-f1")
        with tempfile.TemporaryDirectory() as directory:
            path = self.write_inventory(directory, text)
            with self.assertRaisesRegex(
                R2LabPhysicalInventoryError,
                "RAN node does not match persisted topology",
            ):
                load_physical_inventory(path, topology=topology())

    def test_rejects_duplicate_radio_authority(self) -> None:
        text = INVENTORY + 'rru="n300"\n'
        with tempfile.TemporaryDirectory() as directory:
            path = self.write_inventory(directory, text)
            with self.assertRaisesRegex(
                R2LabPhysicalInventoryError,
                "radio does not match persisted topology",
            ):
                load_physical_inventory(path, topology=topology())


if __name__ == "__main__":
    unittest.main()
