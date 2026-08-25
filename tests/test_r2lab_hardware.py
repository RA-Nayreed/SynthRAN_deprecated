from __future__ import annotations

import unittest

from synthran.r2lab.hardware import (
    RADIOS,
    UES,
    PhysicalTopology,
    R2LabHardwareError,
    capabilities,
)


class R2LabHardwareCatalogTests(unittest.TestCase):
    def test_n3xx_radios_are_both_executable(self) -> None:
        self.assertTrue(RADIOS["n300"].executable)
        self.assertTrue(RADIOS["n320"].executable)
        self.assertEqual("n3xx", RADIOS["n300"].family)
        self.assertEqual("n3xx", RADIOS["n320"].family)
        self.assertNotEqual(RADIOS["n300"].values_file, RADIOS["n320"].values_file)

    def test_catalog_keeps_non_n3xx_hardware_truthful(self) -> None:
        self.assertEqual("sopnode-f3", RADIOS["benetel1"].fixed_ran_node)
        self.assertFalse(RADIOS["benetel1"].executable)
        self.assertFalse(RADIOS["benetel2"].executable)
        self.assertFalse(RADIOS["liteon"].executable)
        self.assertIn("placeholder", RADIOS["liteon"].reason or "")
        self.assertFalse(RADIOS["jaguar"].executable)
        self.assertFalse(RADIOS["panther"].executable)

    def test_all_six_qfits_are_available(self) -> None:
        expected = {"qfit07", "qfit09", "qfit18", "qfit29", "qfit32", "qfit34"}
        self.assertEqual(expected, {name for name in UES if name.startswith("qfit")})
        self.assertTrue(all(UES[name].mode == "mbim" for name in expected))
        self.assertTrue(all(UES[name].data_interface == "wwan0" for name in expected))

    def test_all_nine_qhats_are_available_and_redcap_is_qmi(self) -> None:
        expected = {
            "qhat01", "qhat02", "qhat03", "qhat10", "qhat11",
            "qhat20", "qhat21", "qhat22", "qhat23",
        }
        self.assertEqual(expected, {name for name in UES if name.startswith("qhat")})
        for name in ("qhat20", "qhat21", "qhat22", "qhat23"):
            self.assertEqual("qmi", UES[name].mode)
        for name in ("qhat01", "qhat02", "qhat03", "qhat10", "qhat11"):
            self.assertEqual("mbim", UES[name].mode)

    def test_topology_allows_selectable_compute_nodes_radio_and_ue(self) -> None:
        topology = PhysicalTopology(
            core_node="sopnode-f1",
            ran_node="sopnode-w3",
            radio="n320",
            ue="qhat23",
        ).validate()
        self.assertEqual("n320", topology.radio_profile.name)
        self.assertEqual("qmi", topology.ue_profile.mode)
        self.assertEqual(("sopnode-f1", "sopnode-w3"), topology.nodes)

    def test_topology_rejects_same_compute_node(self) -> None:
        with self.assertRaisesRegex(R2LabHardwareError, "must differ"):
            PhysicalTopology(
                core_node="sopnode-f1",
                ran_node="sopnode-f1",
                radio="n300",
                ue="qfit09",
            ).validate()

    def test_ofh_and_placeholder_profiles_are_not_misreported_as_n3xx(self) -> None:
        with self.assertRaisesRegex(R2LabHardwareError, "not executable"):
            PhysicalTopology(
                core_node="sopnode-f2",
                ran_node="sopnode-f3",
                radio="benetel1",
                ue="qfit09",
            ).validate()
        with self.assertRaisesRegex(R2LabHardwareError, "not executable"):
            PhysicalTopology(
                core_node="sopnode-f2",
                ran_node="sopnode-f3",
                radio="liteon",
                ue="qfit09",
            ).validate()

    def test_capabilities_expose_supported_nodes_without_one_fixed_pair(self) -> None:
        payload = capabilities()
        self.assertIn("sopnode-f1", payload["compute_nodes"])
        self.assertIn("sopnode-f2", payload["compute_nodes"])
        self.assertIn("sopnode-f3", payload["compute_nodes"])
        self.assertIn("sopnode-w3", payload["compute_nodes"])
        self.assertEqual(["n300", "n320"], payload["canonical_executable_radios"])
        self.assertIn("qfit34", payload["canonical_executable_ues"])
        self.assertIn("qhat23", payload["canonical_executable_ues"])


if __name__ == "__main__":
    unittest.main()
