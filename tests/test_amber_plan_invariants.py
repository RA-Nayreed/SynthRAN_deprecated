from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from synthran.ambient_contract import ALOHA_SLOT_RULE
from synthran.iot_source import (
    AMBIENT_PROFILE,
    AmberSourceAdapter,
    IoTSourceSpec,
)


class AmberPlanInvariantTests(unittest.TestCase):
    def test_pinned_amber_plan_obeys_scientific_contract(self) -> None:
        repository_root = Path(__file__).resolve().parents[1]
        amber_checkout = repository_root / ".deps" / "amber"
        if not (amber_checkout / ".git").exists():
            self.skipTest("pinned Amber checkout is not available")

        spec = IoTSourceSpec(
            run_id="ambient-plan-invariant-test",
            network_run_id="ambient-network-invariant-test",
            profile=AMBIENT_PROFILE,
            seed=424242,
            sensor_period_seconds=10,
        )
        with tempfile.TemporaryDirectory() as temporary:
            plan = AmberSourceAdapter(
                repository_root=repository_root,
                dependency_root=repository_root / ".deps",
            ).prepare(spec, 40, Path(temporary))

            self.assertEqual(40, plan.planned_count)
            allowed = {
                "decoded",
                "capture-decoded",
                "sic-recovered",
                "collision",
                "transmit-undecoded",
                "downlink-sensitivity",
                "startup",
                "energy-below-threshold",
                "slot-missed",
                "controller-not-ready",
            }
            self.assertTrue(all(event.outcome in allowed for event in plan.events))
            self.assertFalse(
                any(event.outcome == "energy-or-controller-silence" for event in plan.events)
            )
            self.assertTrue(
                all(
                    event.slot_index is None or 0 <= event.slot_index < 16
                    for event in plan.events
                )
            )
            self.assertTrue(
                all(event.details.get("uplink_dbm") is not None for event in plan.events)
            )

            collect_events = [
                event for event in plan.events if event.details.get("collect_received") is True
            ]
            self.assertTrue(collect_events)
            self.assertTrue(
                all(event.details.get("aloha_slot_rule") == ALOHA_SLOT_RULE for event in collect_events)
            )
            self.assertTrue(
                all(
                    event.details.get("aloha_frame_index") == event.sequence - 1
                    for event in collect_events
                )
            )

            transmitted = [event for event in plan.events if event.transmitted]
            self.assertTrue(transmitted, "corrected ambient plan produced no transmissions")
            self.assertTrue(
                all(
                    isinstance(event.details.get("amber_payload"), int)
                    and 100 <= int(event.details["amber_payload"]) <= 255
                    for event in transmitted
                ),
                "transmitted Amber packets did not pass through sensing/processing",
            )
            self.assertTrue(
                all(
                    event.details.get("capacitor_voltage_start_v") is not None
                    and event.details.get("capacitor_voltage_end_v") is not None
                    for event in plan.events
                )
            )
            self.assertTrue(
                all(
                    event.details.get("capacitor_voltage_collect_v") is not None
                    for event in collect_events
                )
            )

            scenario = json.loads(plan.scenario_path.read_text(encoding="utf-8"))
            model = scenario["profile"]["model"]
            self.assertEqual("current-frame-only", model["access"]["frame_scope"])
            self.assertEqual(16, model["access"]["slots"])
            self.assertEqual(
                "deterministic-uniform-hash",
                model["access"]["slot_selection"],
            )
            self.assertEqual(ALOHA_SLOT_RULE, model["access"]["slot_rule"])
            self.assertEqual(
                ["iot_seed", "node_id", "frame_index"],
                model["access"]["slot_key"],
            )
            self.assertTrue(model["access"]["energy_treatment_invariant"])
            self.assertEqual(
                "listening-sensing-processing-wait-slot-transmitting",
                model["controller"]["data_path"],
            )


if __name__ == "__main__":
    unittest.main()
