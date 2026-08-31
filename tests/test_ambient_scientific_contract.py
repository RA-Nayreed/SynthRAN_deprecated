from __future__ import annotations

from types import SimpleNamespace
import unittest

from synthran.ambient_contract import (
    ALOHA_SLOT_RULE,
    ENERGY_TRACE_SHA256,
    ambient_model_descriptor,
)
from synthran.amber_planner import (
    _collision_resolution,
    _per_node_uplink,
    _voltage_at,
)
from synthran.iot_source import (
    AMBIENT_PROFILE,
    IoTSourceSpec,
    profile_descriptor,
    profile_digest,
)


class AmbientScientificContractTests(unittest.TestCase):
    def test_profile_digest_contains_complete_result_affecting_contract(self) -> None:
        trace = ENERGY_TRACE_SHA256
        model = ambient_model_descriptor(trace)
        self.assertEqual("current-frame-only", model["access"]["frame_scope"])
        self.assertEqual(16, model["access"]["slots"])
        self.assertEqual("deterministic-uniform-hash", model["access"]["slot_selection"])
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
        self.assertEqual(1, model["energy"]["simulation_row_period_ms"])
        self.assertEqual(
            "one-trace-row-per-simulation-millisecond",
            model["energy"]["simulation_replay"],
        )
        self.assertEqual(
            {
                "rows": 2999,
                "first": 0.0,
                "last": 2.998,
                "step": 0.001,
                "units": "undeclared-in-workbook",
            },
            model["energy"]["trace_time_axis"],
        )
        self.assertEqual("max", model["energy"]["combine_mode"])
        self.assertTrue(model["collision"]["sic"])
        self.assertEqual(3.0, model["collision"]["required_sinr_db"])
        self.assertEqual(0.9, model["collision"]["cancellation_factor"])
        self.assertEqual(100e6, model["collision"]["bandwidth_hz"])
        self.assertEqual(-100.0, model["radio"]["node"]["sensitivity_dbm"])
        self.assertEqual(46.0, model["radio"]["base_station"]["sector_power_dbm"])

        spec = IoTSourceSpec(
            run_id="ambient-contract-test",
            network_run_id="network-contract-test",
            profile=AMBIENT_PROFILE,
            seed=424242,
            sensor_period_seconds=10,
        )
        descriptor = profile_descriptor(
            spec,
            amber_commit="b" * 40,
            energy_trace_sha256=trace,
        )
        self.assertEqual(model, descriptor["model"])
        self.assertEqual(64, len(profile_digest(descriptor)))

    def test_profile_rejects_a_different_energy_workbook(self) -> None:
        with self.assertRaisesRegex(ValueError, "pinned scientific contract"):
            ambient_model_descriptor("a" * 64)

    def test_voltage_lookup_uses_requested_instant_not_period_end(self) -> None:
        cap = SimpleNamespace(
            initial_voltage=0.0,
            voltage_history=[
                (0.001, 0.2),
                (0.005, 0.8),
                (0.010, 1.7),
            ],
        )
        self.assertEqual(0.0, _voltage_at(cap, 0.0))
        self.assertEqual(0.2, _voltage_at(cap, 1.0))
        self.assertEqual(0.8, _voltage_at(cap, 7.0))
        self.assertEqual(1.7, _voltage_at(cap, 10.0))

    def test_uplink_rssi_is_resolved_per_node_from_sector_powers(self) -> None:
        uplink = {
            "per_sector_powers": {
                "BS0_S0": {0: -81.0, 1: -72.0},
                "BS0_S1": {0: -60.0, 1: -90.0},
                "BS0_S2": {0: -75.0, 1: -65.0},
            }
        }
        result = _per_node_uplink(uplink, [0, 1])
        self.assertEqual((-60.0, "BS0_S1"), result[0])
        self.assertEqual((-65.0, "BS0_S2"), result[1])

    def test_collision_resolution_exposes_capture_and_sic(self) -> None:
        packets = [
            SimpleNamespace(
                end_ms=100.0,
                subcarrier_shift=0,
                rssi_dbm=-50.0,
                collided=False,
            ),
            SimpleNamespace(
                end_ms=102.0,
                subcarrier_shift=0,
                rssi_dbm=-60.0,
                collided=False,
            ),
        ]

        class FakePacketAnalysis:
            @staticmethod
            def thermal_noise_watts(bandwidth_hz, noise_figure_db):
                del bandwidth_hz, noise_figure_db
                return 1e-12

            @staticmethod
            def apply_sic(**kwargs):
                del kwargs
                return [0, 1], [12.0, 6.0]

        result = _collision_resolution(packets, FakePacketAnalysis)
        self.assertEqual(("capture-decoded", 2), result[id(packets[0])])
        self.assertEqual(("sic-recovered", 2), result[id(packets[1])])


if __name__ == "__main__":
    unittest.main()
