"""Offline calibration of Amber Ambient-IoT harvested-energy treatments."""

from __future__ import annotations

from collections import Counter
from pathlib import Path
import statistics
from typing import Any, Mapping, Sequence

from synthran.ambient_contract import (
    CAPACITOR_MAX_V,
    DEFAULT_ENERGY_NODE_VARIATION,
    validate_energy_treatment,
)
from synthran.iot_source import (
    AMBIENT_PROFILE,
    AMBER_SOURCE_ID,
    AmberSourceAdapter,
    IoTSourceSpec,
    profile_descriptor,
    profile_digest,
)
from synthran.research import ID_RE, ResearchError, _identifier, atomic_json


ENERGY_CALIBRATION_SCHEMA = "synthran/amber-energy-calibration/v1alpha1"
_ENERGY_LOSS_OUTCOMES = frozenset({"energy-below-threshold"})


def parse_energy_scales(raw: str) -> tuple[float, ...]:
    """Parse a comma-separated ordered set of harvested-power multipliers."""

    values: list[float] = []
    for item in raw.split(","):
        text = item.strip()
        if not text:
            continue
        try:
            scale = float(text)
        except ValueError as exc:
            raise ResearchError(f"invalid Ambient energy scale: {text!r}") from exc
        try:
            scale, _ = validate_energy_treatment(scale, 0.0)
        except ValueError as exc:
            raise ResearchError(str(exc)) from exc
        if scale not in values:
            values.append(scale)
    if not values:
        raise ResearchError("energy calibration requires at least one power scale")
    return tuple(values)


def _range(values: Sequence[float]) -> dict[str, float | None]:
    if not values:
        return {"minimum": None, "mean": None, "maximum": None}
    return {
        "minimum": min(values),
        "mean": statistics.fmean(values),
        "maximum": max(values),
    }


def _measurement_row(
    plan,
    *,
    warmup_seconds: int,
    duration_seconds: int,
    target_energy_loss_min: float,
    target_energy_loss_max: float,
) -> dict[str, Any]:
    start_ms = warmup_seconds * 1000
    end_ms = (warmup_seconds + duration_seconds) * 1000
    events = [
        event
        for event in plan.events
        if start_ms <= event.planned_at_ms < end_ms
    ]
    if not events:
        raise ResearchError("energy calibration measurement window has no opportunities")

    outcomes = Counter(event.outcome for event in events)
    transmitted = sum(event.transmitted for event in events)
    decoded = sum(event.decoded for event in events)
    energy_losses = sum(outcomes[name] for name in _ENERGY_LOSS_OUTCOMES)
    planned = len(events)
    energy_loss_ratio = energy_losses / planned

    collect_voltages = [
        float(event.details["capacitor_voltage_collect_v"])
        for event in events
        if event.details.get("capacitor_voltage_collect_v") is not None
    ]
    slot_voltages = [
        float(event.details["capacitor_voltage_slot_v"])
        for event in events
        if event.details.get("capacitor_voltage_slot_v") is not None
    ]
    min_tx_voltages = [
        float(event.details["capacitor_voltage_min_tx_v"])
        for event in events
        if event.details.get("capacitor_voltage_min_tx_v") is not None
    ]
    selected_collect_power = [
        float(event.details["selected_harvest_power_collect_w"])
        for event in events
        if event.details.get("selected_harvest_power_collect_w") is not None
    ]
    external_collect_power = [
        float(event.details["external_harvest_power_collect_w"])
        for event in events
        if event.details.get("external_harvest_power_collect_w") is not None
    ]
    wpt_collect_power = [
        float(event.details["wpt_harvest_power_collect_w"])
        for event in events
        if event.details.get("wpt_harvest_power_collect_w") is not None
    ]
    selected_sources = Counter(
        str(event.details["selected_harvest_source_collect"])
        for event in events
        if event.details.get("selected_harvest_source_collect") is not None
    )
    selected_total = sum(selected_sources.values())
    selected_source_fractions = {
        source: count / selected_total
        for source, count in sorted(selected_sources.items())
    } if selected_total else {}
    ceiling_count = sum(
        value >= CAPACITOR_MAX_V - 1e-9 for value in collect_voltages
    )

    return {
        "profile_digest": plan.profile_digest,
        "energy_treatment": plan.spec.energy_treatment,
        "source_window_start_ms": start_ms,
        "source_window_end_ms": end_ms,
        "planned_opportunities": planned,
        "transmitted_opportunities": transmitted,
        "decoded_opportunities": decoded,
        "source_loss": planned - decoded,
        "outcomes": dict(sorted(outcomes.items())),
        "energy_loss_outcomes": sorted(_ENERGY_LOSS_OUTCOMES),
        "energy_loss_count": energy_losses,
        "energy_loss_ratio": energy_loss_ratio,
        "transmission_ratio": transmitted / planned,
        "decode_ratio": decoded / planned,
        "target_energy_loss_band": [
            target_energy_loss_min,
            target_energy_loss_max,
        ],
        "target_band_match": (
            target_energy_loss_min <= energy_loss_ratio <= target_energy_loss_max
        ),
        "capacitor_voltage_collect_v": _range(collect_voltages),
        "capacitor_voltage_slot_v": _range(slot_voltages),
        "capacitor_voltage_min_tx_v": _range(min_tx_voltages),
        "capacitor_collect_ceiling_fraction": (
            ceiling_count / len(collect_voltages) if collect_voltages else None
        ),
        "harvest_power_collect_w": {
            "external": _range(external_collect_power),
            "wpt": _range(wpt_collect_power),
            "selected": _range(selected_collect_power),
        },
        "selected_harvest_source_counts": dict(sorted(selected_sources.items())),
        "selected_harvest_source_fractions": selected_source_fractions,
    }


def _selection(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "power_scale": row["power_scale"],
        "energy_loss_ratio": row["energy_loss_ratio"],
        "decode_ratio": row["decode_ratio"],
        "target_band_match": row["target_band_match"],
        "profile_digest": row["profile_digest"],
    }


def _response_signature(row: Mapping[str, Any]) -> tuple[Any, ...]:
    voltage = row.get("capacitor_voltage_collect_v")
    collect_mean = voltage.get("mean") if isinstance(voltage, Mapping) else None
    return (
        int(row["energy_loss_count"]),
        int(row["transmitted_opportunities"]),
        int(row["decoded_opportunities"]),
        round(float(collect_mean), 9) if collect_mean is not None else None,
        tuple(sorted(dict(row["selected_harvest_source_counts"]).items())),
    )


def execute_energy_calibration(
    *,
    calibration_id: str,
    network_run_id: str,
    scales: Sequence[float],
    seed: int,
    sensor_period_seconds: int,
    warmup_seconds: int,
    duration_seconds: int,
    energy_node_variation: float = DEFAULT_ENERGY_NODE_VARIATION,
    target_energy_loss_min: float = 0.15,
    target_energy_loss_max: float = 0.35,
    repository_root: Path,
    dependency_root: Path,
    calibration_root: Path,
) -> Path:
    """Prepare source-only Amber plans and select a useful energy operating point."""

    _identifier(calibration_id, ID_RE, "energy calibration ID")
    if seed < 0:
        raise ResearchError("energy calibration seed must be non-negative")
    if not 1 <= sensor_period_seconds <= 3600:
        raise ResearchError("energy calibration sensor period is invalid")
    if warmup_seconds < 0 or duration_seconds < 30:
        raise ResearchError("energy calibration measurement window is invalid")
    if not 0.0 <= target_energy_loss_min <= target_energy_loss_max <= 1.0:
        raise ResearchError("energy calibration target loss band must be within [0,1]")
    try:
        _, variation = validate_energy_treatment(1.0, energy_node_variation)
    except ValueError as exc:
        raise ResearchError(str(exc)) from exc

    normalized_scales: list[float] = []
    for raw in scales:
        try:
            scale, _ = validate_energy_treatment(float(raw), variation)
        except ValueError as exc:
            raise ResearchError(str(exc)) from exc
        if scale not in normalized_scales:
            normalized_scales.append(scale)
    if not normalized_scales:
        raise ResearchError("energy calibration requires at least one power scale")

    root = calibration_root.resolve() / calibration_id
    try:
        root.mkdir(parents=True, exist_ok=False)
    except FileExistsError as exc:
        raise ResearchError(
            "energy calibration directory already exists; choose a new calibration ID"
        ) from exc

    adapter = AmberSourceAdapter(
        repository_root=repository_root,
        dependency_root=dependency_root,
    )
    total_seconds = warmup_seconds + duration_seconds
    rows: list[dict[str, Any]] = []
    base_model_digest: str | None = None

    for index, scale in enumerate(normalized_scales, start=1):
        token = f"p{int(round(scale * 10000)):05d}"
        run_id = f"ecal-{seed}-{index:02d}-{token}"
        spec = IoTSourceSpec(
            run_id=run_id,
            network_run_id=network_run_id,
            source=AMBER_SOURCE_ID,
            profile=AMBIENT_PROFILE,
            seed=seed,
            sensor_period_seconds=sensor_period_seconds,
            energy_power_scale=scale,
            energy_node_variation=variation,
        )
        plan = adapter.prepare(spec, total_seconds, root / token)
        if base_model_digest is None:
            control_spec = IoTSourceSpec(
                run_id="energy-calibration-base-model",
                network_run_id=network_run_id,
                source=AMBER_SOURCE_ID,
                profile=AMBIENT_PROFILE,
                seed=seed,
                sensor_period_seconds=sensor_period_seconds,
            )
            descriptor = profile_descriptor(
                control_spec,
                amber_commit=plan.amber_commit,
                energy_trace_sha256=plan.energy_trace_sha256,
            )
            base_model_digest = profile_digest(descriptor)
        row = _measurement_row(
            plan,
            warmup_seconds=warmup_seconds,
            duration_seconds=duration_seconds,
            target_energy_loss_min=target_energy_loss_min,
            target_energy_loss_max=target_energy_loss_max,
        )
        row["power_scale"] = scale
        row["node_variation_fraction"] = variation
        row["run_id"] = run_id
        row["artifact_directory"] = token
        rows.append(row)

    midpoint = (target_energy_loss_min + target_energy_loss_max) / 2.0
    in_band = [row for row in rows if row["target_band_match"]]
    closest = min(
        rows,
        key=lambda row: (
            abs(float(row["energy_loss_ratio"]) - midpoint),
            -float(row["decode_ratio"]),
            -float(row["power_scale"]),
        ),
    )
    response_observed = (
        len({_response_signature(row) for row in rows}) > 1
        if len(rows) > 1
        else None
    )
    calibration_valid = bool(in_band) and response_observed is True
    recommended = (
        min(
            in_band,
            key=lambda row: (
                abs(float(row["energy_loss_ratio"]) - midpoint),
                -float(row["decode_ratio"]),
                -float(row["power_scale"]),
            ),
        )
        if calibration_valid
        else None
    )

    result = {
        "schema": ENERGY_CALIBRATION_SCHEMA,
        "calibration_id": calibration_id,
        "network_run_id": network_run_id,
        "iot_source": AMBER_SOURCE_ID,
        "iot_profile": AMBIENT_PROFILE,
        "seed": seed,
        "sensor_period_seconds": sensor_period_seconds,
        "warmup_seconds": warmup_seconds,
        "duration_seconds": duration_seconds,
        "energy_node_variation": variation,
        "base_model_digest": base_model_digest,
        "target_energy_loss_band": [
            target_energy_loss_min,
            target_energy_loss_max,
        ],
        "target_band_found": bool(in_band),
        "treatment_response_observed": response_observed,
        "calibration_valid": calibration_valid,
        "runs": rows,
        "recommended": _selection(recommended) if recommended is not None else None,
        "closest_observed": _selection(closest),
    }
    result_path = root / "energy-calibration.json"
    atomic_json(result_path, result)
    return result_path
