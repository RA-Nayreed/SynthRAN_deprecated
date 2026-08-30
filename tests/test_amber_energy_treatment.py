from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

from synthran.ambient_contract import (
    ENERGY_TRACE_SHA256,
    ambient_model_descriptor,
    clear_run_energy_treatment,
    deterministic_node_energy_factor,
    validate_energy_treatment,
)
from synthran.iot_source import (
    AMBIENT_PROFILE,
    TRANSPORT_PROFILE,
    AmberSourceAdapter,
    IoTSourceError,
    IoTSourceSpec,
)
from synthran.research import LoadSpec
from synthran.research.v2 import AmberResearchSpec


class AmbientEnergyTreatmentContractTests(unittest.TestCase):
    def test_default_treatment_preserves_existing_profile_descriptor(self) -> None:
        descriptor = ambient_model_descriptor(ENERGY_TRACE_SHA256)
        self.assertNotIn("treatment", descriptor["energy"])

    def test_non_default_treatment_is_explicit_in_profile_descriptor(self) -> None:
        descriptor = ambient_model_descriptor(
            ENERGY_TRACE_SHA256,
            energy_power_scale=0.5,
            energy_node_variation=0.1,
        )
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
        with self.assertRaisesRegex(ValueError, "power scale"):
            validate_energy_treatment(0.0, 0.0)
        with self.assertRaisesRegex(ValueError, "node variation"):
            validate_energy_treatment(1.0, 0.6)

    def test_transport_profile_rejects_energy_treatment(self) -> None:
        with self.assertRaisesRegex(IoTSourceError, "ambient-v1"):
            IoTSourceSpec(
                run_id="transport-energy-treatment-test",
                network_run_id="transport-energy-network-test",
                profile=TRANSPORT_PROFILE,
                energy_power_scale=0.5,
            )

    def test_research_treatment_is_consumed_by_live_source_spec(self) -> None:
        run_id = "ambient-energy-handoff-test"
        try:
            spec = AmberResearchSpec(
                campaign_id="ambient-energy-handoff-campaign",
                run_id=run_id,
                network_run_id="ambient-energy-handoff-network",
                condition="baseline",
                iot_profile=AMBIENT_PROFILE,
                energy_power_scale=0.5,
                energy_node_variation=0.1,
                load=LoadSpec(),
                probe_target="172.28.2.77",
            )
            self.assertEqual(
                {
                    "external_power_scale": 0.5,
                    "node_variation_fraction": 0.1,
                },
                spec.energy_treatment,
            )
            source_spec = IoTSourceSpec(
                run_id=run_id,
                network_run_id="ambient-energy-handoff-network",
                profile=AMBIENT_PROFILE,
            )
            self.assertEqual(0.5, source_spec.energy_power_scale)
            self.assertEqual(0.1, source_spec.energy_node_variation)

            # The registration is one-shot. A second independent source spec
            # for the same run ID falls back to the normal control treatment.
            second = IoTSourceSpec(
                run_id=run_id,
                network_run_id="ambient-energy-handoff-network",
                profile=AMBIENT_PROFILE,
            )
            self.assertEqual(1.0, second.energy_power_scale)
            self.assertEqual(0.0, second.energy_node_variation)
        finally:
            clear_run_energy_treatment(run_id)

    def test_pinned_amber_plan_records_energy_treatment_and_provenance(self) -> None:
        repository_root = Path(__file__).resolve().parents[1]
        amber_checkout = repository_root / ".deps" / "amber"
        if not (amber_checkout / ".git").exists():
            self.skipTest("pinned Amber checkout is not available")

        adapter = AmberSourceAdapter(
            repository_root=repository_root,
            dependency_root=repository_root / ".deps",
        )
        control_spec = IoTSourceSpec(
            run_id="ambient-energy-control-test",
            network_run_id="ambient-energy-network-test",
            profile=AMBIENT_PROFILE,
            seed=424242,
            sensor_period_seconds=10,
        )
        stressed_spec = IoTSourceSpec(
            run_id="ambient-energy-stressed-test",
            network_run_id="ambient-energy-network-test",
            profile=AMBIENT_PROFILE,
            seed=424242,
            sensor_period_seconds=10,
            energy_power_scale=0.5,
            energy_node_variation=0.1,
        )
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            control = adapter.prepare(control_spec, 40, root / "control")
            stressed = adapter.prepare(stressed_spec, 40, root / "stressed")

        self.assertNotEqual(control.profile_digest, stressed.profile_digest)
        self.assertEqual(
            {
                "external_power_scale": 0.5,
                "node_variation_fraction": 0.1,
            },
            stressed.spec.energy_treatment,
        )
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
