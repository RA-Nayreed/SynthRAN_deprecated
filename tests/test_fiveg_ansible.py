from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

from synthran.fiveg_ansible import (
    FiveGAnsibleError,
    load_inventory,
    parse_inventory,
    run_offline_doctor,
)


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
FIXTURE = REPOSITORY_ROOT / "tests" / "fixtures" / "inventory_open5gs_srsran_rfsim.ini"


class InventoryContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.text = FIXTURE.read_text(encoding="utf-8")

    def test_reads_generated_inventory_without_certifying_topology(self) -> None:
        inventory = parse_inventory(self.text, source=Path("hosts.ini"))
        self.assertEqual("open5gs", inventory.core)
        self.assertEqual("srsRAN", inventory.ran)
        self.assertEqual("rfsim", inventory.radio)
        self.assertEqual("lab-core", inventory.core_node.name)
        self.assertEqual("lab-ran", inventory.ran_node.name)
        self.assertFalse(inventory.monitoring_enabled)
        self.assertNotIn("ip", inventory.redacted_summary())
        self.assertNotIn("ansible_user", inventory.redacted_summary())

    def test_accepts_other_upstream_core_ran_radio_values(self) -> None:
        text = (
            self.text.replace('core="open5gs"', 'core="free5gc"')
            .replace('ran="srsRAN"', 'ran="oai"')
            .replace('rru="rfsim"', 'rru="n300"')
        )
        inventory = parse_inventory(text, source=Path("hosts.ini"))
        self.assertEqual("free5gc", inventory.core)
        self.assertEqual("oai", inventory.ran)
        self.assertEqual("n300", inventory.radio)

    def test_accepts_monitoring_as_upstream_capability(self) -> None:
        text = self.text.replace("monitoring_enabled=false", "monitoring_enabled=true")
        inventory = parse_inventory(text, source=Path("hosts.ini"))
        self.assertTrue(inventory.monitoring_enabled)

    def test_rejects_mismatched_node_alias(self) -> None:
        text = self.text.replace('ran_node_name="lab-ran"', 'ran_node_name="other"')
        with self.assertRaisesRegex(FiveGAnsibleError, "ran_node_name must match"):
            parse_inventory(text, source=Path("hosts.ini"))

    def test_preserves_hash_characters_in_all_vars(self) -> None:
        text = self.text + "dpdk_interface_c=eth1#0-1\n"
        inventory = parse_inventory(text, source=Path("hosts.ini"))
        self.assertEqual("eth1#0-1", inventory.all_vars["dpdk_interface_c"])

    def test_load_inventory_reads_fixture(self) -> None:
        inventory = load_inventory(FIXTURE)
        self.assertEqual(FIXTURE, inventory.path)


class DoctorTests(unittest.TestCase):
    def test_offline_doctor_fails_without_the_locked_checkout(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            report = run_offline_doctor(
                inventory_path=FIXTURE,
                lock_path=REPOSITORY_ROOT / "dependencies.lock.yml",
                dependency_root=Path(directory),
            )
        self.assertFalse(report.ready)
        failed = {check.name: check.detail for check in report.checks if not check.passed}
        self.assertIn("fiveg-ansible", failed)
        self.assertNotIn(directory, report.render())


if __name__ == "__main__":
    unittest.main()
