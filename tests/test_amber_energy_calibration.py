from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import patch

from synthran.ambient_contract import ENERGY_TRACE_SHA256
from synthran.research.energy_calibration import execute_energy_calibration


def _event(
    *,
    planned_at_ms: int,
    outcome: str,
    transmitted: bool,
    decoded: bool,
    collect_v: float,
    selected_source: str,
) -> SimpleNamespace:
    return SimpleNamespace(
        planned_at_ms=planned_at_ms,
        outcome=outcome,
        transmitted=transmitted,
        decoded=decoded,
        details={
            "capacitor_voltage_collect_v": collect_v,
            "capacitor_voltage_slot_v": max(0.0, collect_v - 0.05),
            "capacitor_voltage_min_tx_v": (
                max(0.0, collect_v - 0.1) if transmitted else None
            ),
            "selected_harvest_power_collect_w": 2.0e-4,
            "external_harvest_power_collect_w": 1.5e-4,
            "wpt_harvest_power_collect_w": 1.0e-4,
            "selected_harvest_source_collect": selected_source,
        },
    )


def _plan(spec, energy_losses: int) -> SimpleNamespace:
    outcomes = ["decoded", "decoded", "collision", "decoded"]
    for index in range(energy_losses):
        outcomes[index] = "energy-below-threshold"
    events = []
    for index, outcome in enumerate(outcomes):
        transmitted = outcome != "energy-below-threshold"
        decoded = outcome == "decoded"
        events.append(
            _event(
                planned_at_ms=30_000 + index * 10_000,
                outcome=outcome,
                transmitted=transmitted,
                decoded=decoded,
                collect_v=(1.4 if outcome == "energy-below-threshold" else 2.0),
                selected_source=("external" if index % 2 == 0 else "wpt"),
            )
        )
    digest_token = int(round(float(spec.energy_power_scale) * 1_000_000))
    return SimpleNamespace(
        spec=spec,
        events=tuple(events),
        profile_digest=f"{digest_token:064x}"[-64:],
        amber_commit="a" * 40,
        energy_trace_sha256=ENERGY_TRACE_SHA256,
    )


class _FakeAmberSourceAdapter:
    def __init__(self, **_kwargs) -> None:
        pass

    def prepare(self, spec, _duration_seconds, _run_directory):
        scale = round(float(spec.energy_power_scale), 2)
        energy_losses = {1.0: 0, 0.5: 1, 0.25: 2}[scale]
        return _plan(spec, energy_losses)


class _FlatAmberSourceAdapter:
    def __init__(self, **_kwargs) -> None:
        pass

    def prepare(self, spec, _duration_seconds, _run_directory):
        return _plan(spec, 1)


class AmberEnergyCalibrationTests(unittest.TestCase):
    def test_calibration_selects_in_band_energy_treatment(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with patch(
                "synthran.research.energy_calibration.AmberSourceAdapter",
                _FakeAmberSourceAdapter,
            ):
                result_path = execute_energy_calibration(
                    calibration_id="ambient-energy-selection-test",
                    network_run_id="accepted-rfsim-network",
                    scales=(1.0, 0.5, 0.25),
                    seed=424242,
                    sensor_period_seconds=10,
                    warmup_seconds=30,
                    duration_seconds=40,
                    target_energy_loss_min=0.20,
                    target_energy_loss_max=0.30,
                    repository_root=root,
                    dependency_root=root / ".deps",
                    calibration_root=root / "calibrations",
                )
            result = json.loads(result_path.read_text(encoding="utf-8"))

        self.assertTrue(result["target_band_found"])
        self.assertTrue(result["treatment_response_observed"])
        self.assertTrue(result["calibration_valid"])
        self.assertEqual(0.5, result["recommended"]["power_scale"])
        self.assertEqual(0.25, result["recommended"]["energy_loss_ratio"])
        self.assertTrue(result["recommended"]["target_band_match"])
        self.assertEqual(0.5, result["closest_observed"]["power_scale"])
        rows = {row["power_scale"]: row for row in result["runs"]}
        self.assertEqual(0.0, rows[1.0]["energy_loss_ratio"])
        self.assertEqual(0.25, rows[0.5]["energy_loss_ratio"])
        self.assertEqual(0.5, rows[0.25]["energy_loss_ratio"])
        self.assertEqual(1, rows[0.5]["outcomes"]["collision"])
        self.assertEqual(1, rows[0.5]["outcomes"]["energy-below-threshold"])
        self.assertEqual(
            {"external": 2, "wpt": 2},
            rows[0.5]["selected_harvest_source_counts"],
        )
        self.assertEqual(
            {"external": 0.5, "wpt": 0.5},
            rows[0.5]["selected_harvest_source_fractions"],
        )
        self.assertEqual("p05000", rows[0.5]["artifact_directory"])

    def test_calibration_does_not_recommend_out_of_band_single_scale(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with patch(
                "synthran.research.energy_calibration.AmberSourceAdapter",
                _FakeAmberSourceAdapter,
            ):
                result_path = execute_energy_calibration(
                    calibration_id="ambient-energy-no-band-test",
                    network_run_id="accepted-rfsim-network",
                    scales=(1.0,),
                    seed=424242,
                    sensor_period_seconds=10,
                    warmup_seconds=30,
                    duration_seconds=40,
                    target_energy_loss_min=0.20,
                    target_energy_loss_max=0.30,
                    repository_root=root,
                    dependency_root=root / ".deps",
                    calibration_root=root / "calibrations",
                )
            result = json.loads(result_path.read_text(encoding="utf-8"))

        self.assertFalse(result["target_band_found"])
        self.assertIsNone(result["treatment_response_observed"])
        self.assertFalse(result["calibration_valid"])
        self.assertIsNone(result["recommended"])
        self.assertEqual(1.0, result["closest_observed"]["power_scale"])
        self.assertFalse(result["closest_observed"]["target_band_match"])

    def test_flat_multi_scale_response_is_not_calibrated(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with patch(
                "synthran.research.energy_calibration.AmberSourceAdapter",
                _FlatAmberSourceAdapter,
            ):
                result_path = execute_energy_calibration(
                    calibration_id="ambient-energy-flat-response-test",
                    network_run_id="accepted-rfsim-network",
                    scales=(1.0, 0.5, 0.25),
                    seed=424242,
                    sensor_period_seconds=10,
                    warmup_seconds=30,
                    duration_seconds=40,
                    target_energy_loss_min=0.20,
                    target_energy_loss_max=0.30,
                    repository_root=root,
                    dependency_root=root / ".deps",
                    calibration_root=root / "calibrations",
                )
            result = json.loads(result_path.read_text(encoding="utf-8"))

        self.assertTrue(result["target_band_found"])
        self.assertFalse(result["treatment_response_observed"])
        self.assertFalse(result["calibration_valid"])
        self.assertIsNone(result["recommended"])
        self.assertEqual(0.25, result["closest_observed"]["energy_loss_ratio"])


if __name__ == "__main__":
    unittest.main()
