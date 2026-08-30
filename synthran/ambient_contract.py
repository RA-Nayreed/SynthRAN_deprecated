"""Reproducible scientific contract for the Amber Ambient-IoT profile."""

from __future__ import annotations

import hashlib
from typing import Any


FREQUENCY_HZ = 924_000_000.0
PATHLOSS_MODEL = "macro"
LOS = True
NODE_MIN_RADIUS_M = 5.0
NODE_MAX_RADIUS_M = 40.0
NODE_HEIGHT_M = 1.5
NODE_SENSITIVITY_DBM = -100.0
NODE_EFFICIENCY = 0.7
NODE_ANTENNA = "omni"
NODE_SUBCARRIER_SHIFT = 0

BS_SECTOR_AZIMUTHS_DEG = (0.0, 120.0, 240.0)
BS_SECTOR_BEAMWIDTH_DEG = 65.0
BS_SECTOR_POWER_DBM = 46.0
BS_SECTOR_GAIN_DBI = 15.0
BS_SECTOR_SENSITIVITY_DBM = -100.0
BS_HEIGHT_M = 25.0

ENERGY_MODE = "hybrid"
ENERGY_COMBINE_MODE = "max"
ENERGY_TRACE_COLUMN = "V_IM"
ENERGY_TRACE_TIME_COLUMN = "Time"
ENERGY_TRACE_RESISTANCE_OHM = 5000.0
ENERGY_TRACE_SIMULATION_ROW_PERIOD_MS = 1
ENERGY_TRACE_LOOPS = True
ENERGY_TRACE_SHA256 = "0647749f8a290f96e8b450bf61fe7a893139e16ca57a2d8fa327f6cddbd4fc73"
# Observed from this exact pinned workbook. AMBER ignores the Time column and
# advances one V_IM row per env.timeout(1); the stored axis has no declared unit.
ENERGY_TRACE_EXPECTED_ROWS = 2999
ENERGY_TRACE_TIME_FIRST = 0.0
ENERGY_TRACE_TIME_LAST = 2.998
ENERGY_TRACE_TIME_STEP = 0.001
ENERGY_TRACE_TIME_UNITS = "undeclared-in-workbook"

DEFAULT_ENERGY_POWER_SCALE = 1.0
DEFAULT_ENERGY_NODE_VARIATION = 0.0
MAX_ENERGY_NODE_VARIATION = 0.5

CAPACITANCE_F = 300e-6
R_SERIES_OHM = 5000.0
R_LEAKAGE_OHM = 100000.0
CAPACITOR_DT_SECONDS = 0.001
CAPACITOR_INITIAL_V = 0.0
CAPACITOR_MAX_V = 2.0

THRESHOLD_LOW_V = 1.3
THRESHOLD_HIGH_V = 1.7
STARTUP_MAX_MS = 2000
CURRENT_LISTENING_A = 1.4e-4
CURRENT_SENSING_A = 0.512e-3
CURRENT_PROCESSING_A = 1.28e-3
CURRENT_TRANSMITTING_A = 5e-3
DURATION_LISTENING_MS = 5
DURATION_SENSING_MS = 2
DURATION_PROCESSING_MS = 5
DURATION_TRANSMITTING_MS = 15

ALOHA_SLOTS = 16
COMMAND_MS = 5
SLOT_MS = 8
COLLISION_WINDOW_MS = 5.0
REQUIRED_SINR_DB = 3.0
SIC_ENABLED = True
SIC_CANCELLATION_FACTOR = 0.9
NOISE_FIGURE_DB = 6.0
BANDWIDTH_HZ = 100e6


def validate_energy_treatment(
    power_scale: float,
    node_variation: float,
) -> tuple[float, float]:
    """Validate and normalize the explicit Ambient-IoT energy treatment."""

    try:
        scale = float(power_scale)
        variation = float(node_variation)
    except (TypeError, ValueError) as exc:
        raise ValueError("Ambient energy treatment must contain decimal numbers") from exc
    if not math_is_finite(scale) or not math_is_finite(variation):
        raise ValueError("Ambient energy treatment values must be finite")
    if not 0.0 < scale <= 1.0:
        raise ValueError("Ambient energy power scale must be in (0, 1]")
    if not 0.0 <= variation <= MAX_ENERGY_NODE_VARIATION:
        raise ValueError(
            f"Ambient energy node variation must be in [0, {MAX_ENERGY_NODE_VARIATION}]"
        )
    return scale, variation


def math_is_finite(value: float) -> bool:
    return value == value and value not in (float("inf"), float("-inf"))


def deterministic_node_energy_factor(
    seed: int,
    node_id: int,
    variation: float,
) -> float:
    """Return a stable per-node energy multiplier without perturbing AMBER RNG."""

    if seed < 0 or node_id < 0:
        raise ValueError("energy factor seed and node ID must be non-negative")
    _, variation = validate_energy_treatment(1.0, variation)
    if variation == 0.0:
        return 1.0
    digest = hashlib.sha256(
        f"ambient-energy-factor-v1:{seed}:{node_id}".encode("ascii")
    ).digest()
    unit = int.from_bytes(digest[:8], "big") / float((1 << 64) - 1)
    return 1.0 + variation * (2.0 * unit - 1.0)


def ambient_model_descriptor(
    energy_trace_sha256: str,
    *,
    energy_power_scale: float = DEFAULT_ENERGY_POWER_SCALE,
    energy_node_variation: float = DEFAULT_ENERGY_NODE_VARIATION,
) -> dict[str, Any]:
    """Return every result-affecting assumption in ``ambient-v1``."""

    if energy_trace_sha256 != ENERGY_TRACE_SHA256:
        raise ValueError("ambient energy trace does not match the pinned scientific contract")
    power_scale, node_variation = validate_energy_treatment(
        energy_power_scale,
        energy_node_variation,
    )
    energy: dict[str, Any] = {
        "mode": ENERGY_MODE,
        "combine_mode": ENERGY_COMBINE_MODE,
        "trace_sha256": energy_trace_sha256,
        "trace_column": ENERGY_TRACE_COLUMN,
        "trace_time_column": ENERGY_TRACE_TIME_COLUMN,
        "trace_resistance_ohm": ENERGY_TRACE_RESISTANCE_OHM,
        "simulation_row_period_ms": ENERGY_TRACE_SIMULATION_ROW_PERIOD_MS,
        "simulation_replay": "one-trace-row-per-simulation-millisecond",
        "trace_time_axis": {
            "rows": ENERGY_TRACE_EXPECTED_ROWS,
            "first": ENERGY_TRACE_TIME_FIRST,
            "last": ENERGY_TRACE_TIME_LAST,
            "step": ENERGY_TRACE_TIME_STEP,
            "units": ENERGY_TRACE_TIME_UNITS,
        },
        "trace_loops": ENERGY_TRACE_LOOPS,
        "shared_environmental_trace": True,
    }
    # Preserve the accepted scale=1, zero-variation profile identity exactly.
    if (
        power_scale != DEFAULT_ENERGY_POWER_SCALE
        or node_variation != DEFAULT_ENERGY_NODE_VARIATION
    ):
        energy["treatment"] = {
            "external_power_scale": power_scale,
            "node_variation_fraction": node_variation,
            "node_factor_rule": "sha256-symmetric-v1",
            "wpt_scaled": False,
        }

    return {
        "radio": {
            "frequency_hz": FREQUENCY_HZ,
            "pathloss": PATHLOSS_MODEL,
            "los": LOS,
            "node": {
                "placement": "seeded-uniform-radius-and-angle",
                "radius_m": [NODE_MIN_RADIUS_M, NODE_MAX_RADIUS_M],
                "height_m": NODE_HEIGHT_M,
                "sensitivity_dbm": NODE_SENSITIVITY_DBM,
                "efficiency": NODE_EFFICIENCY,
                "antenna": NODE_ANTENNA,
                "subcarrier_shift": NODE_SUBCARRIER_SHIFT,
            },
            "base_station": {
                "sector_azimuths_deg": list(BS_SECTOR_AZIMUTHS_DEG),
                "sector_beamwidth_deg": BS_SECTOR_BEAMWIDTH_DEG,
                "sector_power_dbm": BS_SECTOR_POWER_DBM,
                "sector_gain_dbi": BS_SECTOR_GAIN_DBI,
                "sector_sensitivity_dbm": BS_SECTOR_SENSITIVITY_DBM,
                "height_m": BS_HEIGHT_M,
            },
        },
        "energy": energy,
        "capacitor": {
            "capacitance_f": CAPACITANCE_F,
            "r_series_ohm": R_SERIES_OHM,
            "r_leakage_ohm": R_LEAKAGE_OHM,
            "dt_seconds": CAPACITOR_DT_SECONDS,
            "initial_voltage_v": CAPACITOR_INITIAL_V,
            "maximum_voltage_v": CAPACITOR_MAX_V,
        },
        "controller": {
            "thresholds_v": {
                "low": THRESHOLD_LOW_V,
                "high": THRESHOLD_HIGH_V,
            },
            "max_startup_time_ms": STARTUP_MAX_MS,
            "currents_a": {
                "listening": CURRENT_LISTENING_A,
                "sensing": CURRENT_SENSING_A,
                "processing": CURRENT_PROCESSING_A,
                "transmitting": CURRENT_TRANSMITTING_A,
            },
            "durations_ms": {
                "listening": DURATION_LISTENING_MS,
                "sensing": DURATION_SENSING_MS,
                "processing": DURATION_PROCESSING_MS,
                "transmitting": DURATION_TRANSMITTING_MS,
            },
            "data_path": "listening-sensing-processing-wait-slot-transmitting",
        },
        "access": {
            "protocol": "periodic-framed-slotted-aloha",
            "frame_scope": "current-frame-only",
            "slots": ALOHA_SLOTS,
            "slot_ms": SLOT_MS,
            "command_ms": COMMAND_MS,
            "slot_selection": "uniform-random",
        },
        "collision": {
            "collision_window_ms": COLLISION_WINDOW_MS,
            "required_sinr_db": REQUIRED_SINR_DB,
            "sic": SIC_ENABLED,
            "cancellation_factor": SIC_CANCELLATION_FACTOR,
            "noise_figure_db": NOISE_FIGURE_DB,
            "bandwidth_hz": BANDWIDTH_HZ,
            "decoded_labels": [
                "decoded",
                "capture-decoded",
                "sic-recovered",
            ],
        },
        "payload": {
            "source_outcome": "amber",
            "telemetry_value": "synthran-canonical-sensor-sequence",
            "amber_sensor_payload_recorded_as_diagnostic": True,
        },
    }
