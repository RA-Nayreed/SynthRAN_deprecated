from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

from synthran import cli
from synthran.ambient_contract import (
    ALOHA_SLOT_RULE,
    ALOHA_SLOTS,
    ENERGY_TRACE_SHA256,
    ambient_model_descriptor,
    deterministic_aloha_slot,
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
    def test_default_treatment_preserves_explicit_access_contract(self) -> None:
        descriptor = ambient_model_descriptor(ENERGY_TRACE_SHA256)
        self.assertNotIn("treatment", descriptor["energy"])
        access = descriptor["access"]
        self.assertEqual("deterministic-uniform-hash", access["slot_selection"])
        self.assertEqual(ALOHA_SLOT_RULE, access["slot_rule"])
        self.assertEqual(["iot_seed", "node_id", "frame_index"], access["slot_key"])
        self.assertTrue(access["energy_treatment_invariant"])

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

    def test_aloha_slot_rule_is_stable_bounded_and_seeded(self) -> None:
        expected = [11, 13, 2, 1, 11, 12, 2, 4, 14, 1]
        actual = [deterministic_aloha_slot(424242, 3, frame) for frame in range(10)]
        self.assertEqual(expected, actual)
        self.assertTrue(all(0 <= slot < ALOHA_SLOTS for slot in actual))
        self.assertEqual(
            actual,
            [deterministic_aloha_slot(424242, 3, frame) for frame in range(10)],
        )
        self.assertNotEqual(
            actual,
            [deterministic_aloha_slot(424243, 3, frame) for frame in range(10)],
        )
        with self.assertRaisesRegex(ValueError, "non-negative"):
            deterministic_aloha_slot(424242, -1, 0)
        with self.assertRaisesRegex(ValueError, "positive"):
            deterministic_aloha_slot(424242, 0, 0, 0)

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

    def test_research_and_source_specs_share_explicit_treatment(self) -> None:
        research_spec = AmberResearchSpec(
            campaign_id="ambient-energy-explicit-campaign",
            run_id="ambient-energy-explicit-test",
            network_run_id="ambient-energy-explicit-network",
            condition="baseline",
            iot_profile=AMBIENT_PROFILE,
            energy_power_scale=0.5,
            energy_node_variation=0.1,
            load=LoadSpec(),
            probe_target="172.28.2.77",
        )
        source_spec = IoTSourceSpec(
            run_id=research_spec.run_id,
            network_run_id=research_spec.network_run_id,
            profile=research_spec.iot_profile,
            seed=research_spec.iot_seed,
            sensor_period_seconds=research_spec.sensor_period_seconds,
            energy_power_scale=research_spec.energy_power_scale,
            energy_node_variation=research_spec.energy_node_variation,
        )
        expected = {
            "external_power_scale": 0.5,
            "node_variation_fraction": 0.1,
        }
        self.assertEqual(expected, research_spec.energy_treatment)
        self.assertEqual(expected, source_spec.energy_treatment)

    def test_public_surface_has_no_research_or_energy_calibration_command(self) -> None:
        root = cli._subparsers(cli._parser())
        self.assertNotIn("research", root.choices)
        self.assertNotIn("energy-calibrate", root.choices)
        self.assertIn("run", root.choices)
        self.assertIn("calibrate", root.choices)
        self.assertIn("analyze", root.choices)

    def test_run_plan_carries_energy_treatment_directly(self) -> None:
        args = cli._parser().parse_args(
            [
                "run",
                "--plan",
                "--campaign-id",
                "ambient-energy-plan-campaign",
                "--network-run-id",
                "ambient-energy-plan-network",
                "--run-id",
                "ambient-energy-plan-run",
                "--condition",
                "baseline",
                "--iot-profile",
                AMBIENT_PROFILE,
                "--energy-power-scale",
                "0.5",
                "--energy-node-variation",
                "0.1",
            ]
        )
        spec = cli._amber_research_spec(args)
        self.assertEqual(0.5, spec.energy_power_scale)
        self.assertEqual(0.1, spec.energy_node_variation)
        self.assertEqual(
            {
                "external_power_scale": 0.5,
                "node_variation_fraction": 0.1,
            },
            spec.energy_treatment,
        )

    def test_pinned_amber_plan_records_treatment_and_preserves_aloha_opportunities(self) -> None:
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

        def offered_slots(plan):
            return {
                (event.planned_at_ms, event.sensor_id): event.slot_index
                for event in plan.events
                if event.details.get("collect_received") is True
            }

        control_slots = offered_slots(control)
        stressed_slots = offered_slots(stressed)
        self.assertTrue(control_slots)
        self.assertEqual(control_slots, stressed_slots)
        self.assertTrue(all(slot is not None for slot in control_slots.values()))

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
            all(event.details.get("aloha_slot_rule") == ALOHA_SLOT_RULE for event in collect_events)
        )
        self.assertTrue(
            all(
                event.details.get("aloha_frame_index") == event.sequence - 1
                for event in collect_events
            )
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
