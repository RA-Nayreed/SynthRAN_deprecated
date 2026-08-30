from __future__ import annotations

import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from synthran.ambient_contract import (
    DEFAULT_ENERGY_NODE_VARIATION,
    DEFAULT_ENERGY_POWER_SCALE,
    ENERGY_NODE_VARIATION_ENV,
    ENERGY_POWER_SCALE_ENV,
    ENERGY_TRACE_SHA256,
    ambient_model_descriptor,
    deterministic_node_energy_factor,
    energy_treatment,
)
from synthran.iot_source import (
    AMBIENT_PROFILE,
    AmberSourceAdapter,
    IoTSourceSpec,
)


class AmbientEnergyTreatmentContractTests(unittest.TestCase):
    def test_default_treatment_preserves_existing_profile_descriptor(self) -> None:
        with patch.dict(
            os.environ,
            {
                ENERGY_POWER_SCALE_ENV: str(DEFAULT_ENERGY_POWER_SCALE),
                ENERGY_NODE_VARIATION_ENV: str(DEFAULT_ENERGY_NODE_VARIATION),
            },
            clear=False,
        ):
            descriptor = ambient_model_descriptor(ENERGY_TRACE_SHA256)
        self.assertNotIn("treatment", descriptor["energy"])

    def test_non_default_treatment_is_explicit_in_profile_descriptor(self) -> None:
        with patch.dict(
            os.environ,
            {
                ENERGY_POWER_SCALE_ENV: "0.5",
                ENERGY_NODE_VARIATION_ENV: "0.1",
            },
            clear=False,
        ):
            descriptor = ambient_model_descriptor(ENERGY_TRACE_SHA256)
        treatment = descriptor["energy"]["treatment"]
        self.assertEqual(0.5, treatment["external_power_scale"])
        self.assertEqual(0.1, treatment["node_variation_fraction"])
        self.assertEqual("sha256-symmetric-v1", treatment["node_factor_rule"])
        self.assertFalse(treatment["wpt_scaled"])

    def test_node_energy_factor_is_stable_and_bounded(self) -> None:
        first = deterministic_node_energy_factor(424242, 3, 0.15)
        second = deterministic_node_energy_factor(424242, 3, 0.15)
        other = deterministic_node_energy_factor(424242, 4, 0.15)
        self.assertEqual(first, second)
        self.assertNotEqual(first, other)
        self.assertGreaterEqual(first, 0.85)
        self.assertLessEqual(first, 1.15)

    def test_invalid_energy_treatment_fails_closed(self) -> None:
        with patch.dict(
            os.environ,
            {
                ENERGY_POWER_SCALE_ENV: "0",
                ENERGY_NODE_VARIATION_ENV: "0",
            },
            clear=False,
        ):
            with self.assertRaisesRegex(ValueError, "power scale"):
                energy_treatment()
        with patch.dict(
            os.environ,
            {
                ENERGY_POWER_SCALE_ENV: "1",
                ENERGY_NODE_VARIATION_ENV: "0.6",
            },
            clear=False,
        ):
            with self.assertRaisesRegex(ValueError, "node variation"):
                energy_treatment()

    def test_pinned_amber_plan_records_energy_treatment_and_provenance(self) -> None:
        repository_root = Path(__file__).resolve().parents[1]
        amber_checkout = repository_root / ".deps" / "amber"
        if not (amber_checkout / ".git").exists():
            self.skipTest("pinned Amber checkout is not available")

        adapter = AmberSourceAdapter(
            repository_root=repository_root,
            dependency_root=repository_root / ".deps",
        )
        spec = IoTSourceSpec(
            run_id="ambient-energy-treatment-test",
            network_run_id="ambient-energy-network-test",
            profile=AMBIENT_PROFILE,
            seed=424242,
            sensor_period_seconds=10,
        )
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with patch.dict(
                os.environ,
                {
                    ENERGY_POWER_SCALE_ENV: "1.0",
                    ENERGY_NODE_VARIATION_ENV: "0.0",
                },
                clear=False,
            ):
                control = adapter.prepare(spec, 40, root / "control")
            with patch.dict(
                os.environ,
                {
                    ENERGY_POWER_SCALE_ENV: "0.5",
                    ENERGY_NODE_VARIATION_ENV: "0.1",
                },
                clear=False,
            ):
                stressed = adapter.prepare(spec, 40, root / "stressed")

        self.assertNotEqual(control.profile_digest, stressed.profile_digest)
        collect_events = [
            event
            for event in stressed.events
            if event.details.get("collect_received") is True
        ]
        self.assertTrue(collect_events)
        self.assertTrue(
            all(event.details.get("energy_power_scale") == 0.5 for event in collect_events)
        )
        self.assertTrue(
            all(
                0.9 <= float(event.details.get("energy_node_factor")) <= 1.1
                for event in collect_events
            )
        )
        self.assertTrue(
            all(
                event.details.get("external_harvest_power_collect_w") is not None
                and event.details.get("wpt_harvest_power_collect_w") is not None
                and event.details.get("selected_harvest_power_collect_w") is not None
                and event.details.get("selected_harvest_source_collect")
                in {"external", "wpt", "tie", "combined"}
                for event in collect_events
            )
        )
        transmitted = [event for event in stressed.events if event.transmitted]
        self.assertTrue(transmitted)
        self.assertTrue(
            all(
                event.details.get("capacitor_voltage_post_tx_v") is not None
                and event.details.get("capacitor_voltage_min_tx_v") is not None
                and event.details.get("capacitor_energy_pre_tx_j") is not None
                and event.details.get("capacitor_energy_post_tx_j") is not None
                and event.details.get("selected_harvest_power_tx_w") is not None
                for event in transmitted
            )
        )


if __name__ == "__main__":
    unittest.main()
