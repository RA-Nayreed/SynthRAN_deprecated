"""Isolated Amber event-plan worker.

The worker imports Amber only from the verified detached checkout supplied by
``AmberSourceAdapter``. It writes immutable source opportunities; no MQTT or
5G activity occurs here.
"""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
import random
import sys
from typing import Any, Mapping

from synthran.ambient_contract import (
    ALOHA_SLOTS,
    BANDWIDTH_HZ,
    BS_HEIGHT_M,
    BS_SECTOR_AZIMUTHS_DEG,
    BS_SECTOR_BEAMWIDTH_DEG,
    BS_SECTOR_GAIN_DBI,
    BS_SECTOR_POWER_DBM,
    BS_SECTOR_SENSITIVITY_DBM,
    CAPACITANCE_F,
    CAPACITOR_DT_SECONDS,
    CAPACITOR_INITIAL_V,
    COLLISION_WINDOW_MS,
    COMMAND_MS,
    CURRENT_LISTENING_A,
    CURRENT_PROCESSING_A,
    CURRENT_SENSING_A,
    CURRENT_TRANSMITTING_A,
    DURATION_LISTENING_MS,
    DURATION_PROCESSING_MS,
    DURATION_SENSING_MS,
    DURATION_TRANSMITTING_MS,
    ENERGY_COMBINE_MODE,
    ENERGY_MODE,
    ENERGY_TRACE_COLUMN,
    ENERGY_TRACE_RESISTANCE_OHM,
    FREQUENCY_HZ,
    LOS,
    NODE_EFFICIENCY,
    NODE_HEIGHT_M,
    NODE_MAX_RADIUS_M,
    NODE_MIN_RADIUS_M,
    NODE_SENSITIVITY_DBM,
    NOISE_FIGURE_DB,
    PATHLOSS_MODEL,
    REQUIRED_SINR_DB,
    R_LEAKAGE_OHM,
    R_SERIES_OHM,
    SIC_CANCELLATION_FACTOR,
    SIC_ENABLED,
    SLOT_MS,
    STARTUP_MAX_MS,
    THRESHOLD_HIGH_V,
    THRESHOLD_LOW_V,
)
from synthran.iot_source import (
    AMBIENT_PROFILE,
    AMBER_SOURCE_ID,
    IOT_SOURCE_EVENT_SCHEMA_V2,
    TRANSPORT_PROFILE,
)


def _load_input(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise RuntimeError("planner input must be a JSON object")
    return value


def _sensor_id(node_id: int) -> str:
    return f"sensor-{node_id + 1:02d}"


def _value_milli(node_id: int, sequence: int) -> int:
    return (node_id + 1) * 1000 + sequence % 1000


def _event(
    config: Mapping[str, Any],
    *,
    node_id: int,
    sequence: int,
    planned_at_ms: int,
    transmitted: bool,
    decoded: bool,
    outcome: str,
    slot_index: int | None = None,
    details: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "schema": IOT_SOURCE_EVENT_SCHEMA_V2,
        "run_id": config["run_id"],
        "source": AMBER_SOURCE_ID,
        "profile": config["iot_profile"],
        "profile_digest": config["profile_digest"],
        "planned_at_ms": planned_at_ms,
        "sensor_id": _sensor_id(node_id),
        "sequence": sequence,
        "value_milli": _value_milli(node_id, sequence),
        "transmitted": transmitted,
        "decoded": decoded,
        "outcome": outcome,
        "slot_index": slot_index,
        "details": dict(details or {}),
    }


def _transport_plan(config: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Ideal Amber transport profile: deterministic source, no scientific loss."""

    import simpy
    from amber import radiodevices

    duration_ms = int(config["duration_seconds"]) * 1000
    period_ms = int(config["sensor_period_seconds"]) * 1000
    count = int(config["sensor_count"])
    positions = (
        (-30.0, -20.0),
        (-10.0, -20.0),
        (10.0, -20.0),
        (30.0, -20.0),
        (-40.0, 10.0),
        (-20.0, 10.0),
        (0.0, 10.0),
        (20.0, 10.0),
        (40.0, 10.0),
        (0.0, 40.0),
    )
    nodes = [
        radiodevices.Node(
            id=index,
            x=positions[index][0],
            y=positions[index][1],
            height=1.5,
            sensitivity_dbm=-100.0,
            efficiency=1.0,
        )
        for index in range(count)
    ]
    env = simpy.Environment()
    records: list[dict[str, Any]] = []

    def source(node):
        sequence = 1
        while env.now < duration_ms:
            planned = int(env.now)
            records.append(
                _event(
                    config,
                    node_id=node.id,
                    sequence=sequence,
                    planned_at_ms=planned,
                    transmitted=True,
                    decoded=True,
                    outcome="decoded",
                    slot_index=node.id,
                    details={
                        "model": "ideal-unicast",
                        "x_m": node.x,
                        "y_m": node.y,
                        "energy": "ideal",
                        "coverage": "ideal",
                    },
                )
            )
            sequence += 1
            yield env.timeout(period_ms)

    for node in nodes:
        env.process(source(node))
    env.run(until=duration_ms)
    records.sort(key=lambda item: (item["planned_at_ms"], item["sensor_id"]))
    return records


def _voltage_at(cap: Any, time_ms: float) -> float | None:
    """Return the last capacitor sample at or before an exact simulation instant."""

    if time_ms <= 0:
        return float(getattr(cap, "initial_voltage", 0.0))
    candidate: float | None = None
    for time_s, value in cap.voltage_history:
        if time_s * 1000.0 > time_ms:
            break
        candidate = float(value)
    return candidate


def _per_node_uplink(uplink: Mapping[str, Any], node_ids: list[int]) -> dict[int, tuple[float | None, str | None]]:
    """Resolve the strongest BS-sector RSSI for each individual Amber node."""

    per_sector = uplink.get("per_sector_powers")
    if not isinstance(per_sector, Mapping):
        return {node_id: (None, None) for node_id in node_ids}
    result: dict[int, tuple[float | None, str | None]] = {}
    for node_id in node_ids:
        candidates: list[tuple[float, str]] = []
        for sector_name, powers in per_sector.items():
            if not isinstance(sector_name, str) or not isinstance(powers, Mapping):
                continue
            raw = powers.get(node_id)
            if raw is None:
                raw = powers.get(str(node_id))
            if raw is None:
                continue
            try:
                candidates.append((float(raw), sector_name))
            except (TypeError, ValueError):
                continue
        result[node_id] = max(candidates, default=(None, None), key=lambda item: item[0] if item[0] is not None else -1e9)
    return result


def _collision_resolution(packets: list[Any], packet_analysis: Any) -> dict[int, tuple[str, int]]:
    """Reconstruct AMBER's exact collision groups and expose decode mechanism.

    AMBER stores only the final ``collided`` boolean on ``RxPacket``.  For
    research evidence we replay the same grouping and the same AMBER
    ``apply_sic`` implementation, then assert that our reconstruction agrees
    with the engine's final collision flag.
    """

    labels: dict[int, tuple[str, int]] = {}
    by_subcarrier: dict[int, list[Any]] = {}
    for packet in packets:
        by_subcarrier.setdefault(int(packet.subcarrier_shift), []).append(packet)

    noise_w = packet_analysis.thermal_noise_watts(BANDWIDTH_HZ, NOISE_FIGURE_DB)
    for rx_list in by_subcarrier.values():
        rx_list.sort(key=lambda packet: float(packet.end_ms))
        used: set[int] = set()
        for index, packet in enumerate(rx_list):
            if index in used:
                continue
            group = [index]
            for other_index, other in enumerate(rx_list):
                if other_index == index or other_index in used:
                    continue
                if abs(float(packet.end_ms) - float(other.end_ms)) <= COLLISION_WINDOW_MS:
                    group.append(other_index)
            used.update(group)
            if len(group) == 1:
                labels[id(packet)] = ("decoded", 1)
                continue

            powers = [float(rx_list[item].rssi_dbm) for item in group]
            decoded_local, _ = packet_analysis.apply_sic(
                powers_dbm=powers,
                noise_w=noise_w,
                required_sinr_db=REQUIRED_SINR_DB,
                cancellation_factor=(SIC_CANCELLATION_FACTOR if SIC_ENABLED else 0.0),
            )
            decoded_global = [group[item] for item in decoded_local]
            if not SIC_ENABLED and decoded_global:
                decoded_global = decoded_global[:1]
            decoded_set = set(decoded_global)
            for rank, global_index in enumerate(decoded_global):
                labels[id(rx_list[global_index])] = (
                    "capture-decoded" if rank == 0 else "sic-recovered",
                    len(group),
                )
            for global_index in group:
                if global_index not in decoded_set:
                    labels[id(rx_list[global_index])] = ("collision", len(group))

    for packet in packets:
        mechanism, _ = labels.get(id(packet), ("decoded", 1))
        reconstructed_collision = mechanism == "collision"
        if reconstructed_collision != bool(packet.collided):
            raise RuntimeError("AMBER collision reconstruction disagrees with BS engine")
    return labels


def _ambient_plan(config: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Run the reproducible ten-node Amber hybrid-energy ALOHA model."""

    import simpy
    from amber import (
        backscatter,
        bsengine,
        capacitor,
        controller,
        energy,
        packet_analysis,
        propagation,
        radiodevices,
    )

    seed = int(config["iot_seed"])
    random.seed(seed)
    duration_ms = int(config["duration_seconds"]) * 1000
    period_ms = int(config["sensor_period_seconds"]) * 1000
    count = int(config["sensor_count"])
    trace_path = Path(str(config["energy_trace_path"]))
    if not trace_path.is_file():
        raise RuntimeError("ambient-v1 energy trace is missing")

    active_frame_ms = COMMAND_MS + SLOT_MS * ALOHA_SLOTS
    if period_ms <= active_frame_ms:
        raise RuntimeError("ambient-v1 sensor period is too short for its ALOHA frame")
    idle_ms = period_ms - active_frame_ms

    class PeriodicAlohaNode(backscatter.BackscatterModule):
        """One current-frame ALOHA opportunity while preserving AMBER's controller FSM."""

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            # The profile has no registration handshake.  Mark the tag as
            # registered so AMBER's Controller performs listening, sensing,
            # processing and transmitting energy states normally.
            self.state = "registered"
            self.capacitor_ref = None
            self.controller_ref = None
            self.collect_history: list[dict[str, Any]] = []
            self.tx_runtime_history: list[dict[str, Any]] = []

        def attach_runtime(self, cap: Any, ctrl: Any) -> None:
            self.capacitor_ref = cap
            self.controller_ref = ctrl

        def handle_command(self, cmd, bs_id, data):
            del bs_id
            if cmd != "collect":
                return
            frame_slots = int(data.get("frame_slots", 0))
            if frame_slots != ALOHA_SLOTS:
                raise RuntimeError("ambient-v1 collect command advertised the wrong frame size")
            # BSEngine deliberately looks ahead across two frames.  This
            # profile is one opportunity per period, so expose only this
            # command's current 16-slot frame to the node.
            self.rx_slots = list(self.rx_slots[:frame_slots])
            self.chosen_slot_idx = (
                random.randrange(len(self.rx_slots)) if self.rx_slots else -1
            )
            self.state = "registered"
            self.last_tx_command_time = self.env.now
            chosen_slot = (
                self.rx_slots[self.chosen_slot_idx]
                if 0 <= self.chosen_slot_idx < len(self.rx_slots)
                else None
            )
            cap_v = (
                float(self.capacitor_ref.voltage)
                if self.capacitor_ref is not None
                else None
            )
            ctrl = self.controller_ref
            self.collect_history.append(
                {
                    "time_ms": float(self.env.now),
                    "slot_index": (
                        self.chosen_slot_idx if self.chosen_slot_idx >= 0 else None
                    ),
                    "slot_start_ms": float(chosen_slot[0]) if chosen_slot else None,
                    "slot_end_ms": float(chosen_slot[1]) if chosen_slot else None,
                    "capacitor_voltage_v": cap_v,
                    "controller_active": bool(getattr(ctrl, "is_active", False)),
                    "controller_state": str(getattr(ctrl, "state_name", "unknown")),
                    "startup_delay_ms": (
                        float(getattr(ctrl, "startup_delay_s", 0.0)) * 1000.0
                        if ctrl is not None
                        else None
                    ),
                }
            )

        def do_transmit(self, tx_info):
            self.tx_runtime_history.append(
                {
                    "time_ms": float(self.env.now),
                    "capacitor_voltage_v": (
                        float(self.capacitor_ref.voltage)
                        if self.capacitor_ref is not None
                        else None
                    ),
                    "controller_state": str(
                        getattr(self.controller_ref, "state_name", "unknown")
                    ),
                    "controller_active": bool(
                        getattr(self.controller_ref, "is_active", False)
                    ),
                    "slot_index": (
                        self.chosen_slot_idx if self.chosen_slot_idx >= 0 else None
                    ),
                    "payload": tx_info.get("payload"),
                }
            )
            return super().do_transmit(tx_info)

    def periodic_policy(bs):
        del bs
        while True:
            frame = [
                (
                    "tx",
                    COMMAND_MS,
                    "collect",
                    {
                        "cmd": "collect",
                        "target": -1,
                        "frame_slots": ALOHA_SLOTS,
                    },
                )
            ]
            frame.extend(
                ("rx", SLOT_MS, f"slot-{index:02d}")
                for index in range(ALOHA_SLOTS)
            )
            frame.append(("tx", idle_ms, "idle", {"cmd": "idle", "target": -2}))
            yield frame

    nodes = []
    for node_id in range(count):
        radius = random.uniform(NODE_MIN_RADIUS_M, NODE_MAX_RADIUS_M)
        angle = random.uniform(0.0, 2.0 * math.pi)
        nodes.append(
            radiodevices.Node(
                id=node_id,
                x=radius * math.cos(angle),
                y=radius * math.sin(angle),
                height=NODE_HEIGHT_M,
                sensitivity_dbm=NODE_SENSITIVITY_DBM,
                efficiency=NODE_EFFICIENCY,
            )
        )

    bs = radiodevices.BaseStation(
        id=0,
        x=0.0,
        y=0.0,
        site_radius=2.0,
        sectors=[
            radiodevices.Sector(
                azimuth_deg=angle,
                beamwidth_deg=BS_SECTOR_BEAMWIDTH_DEG,
                power=BS_SECTOR_POWER_DBM,
                antenna_gain_dbi=BS_SECTOR_GAIN_DBI,
                sensitivity_dbm=BS_SECTOR_SENSITIVITY_DBM,
                height=BS_HEIGHT_M,
            )
            for angle in BS_SECTOR_AZIMUTHS_DEG
        ],
    )

    params = controller.ControllerParams(
        currents=controller.CurrentsA(
            listening=CURRENT_LISTENING_A,
            sensing=CURRENT_SENSING_A,
            processing=CURRENT_PROCESSING_A,
            transmitting=CURRENT_TRANSMITTING_A,
        ),
        durations_ms=controller.DurationsMs(
            listening=DURATION_LISTENING_MS,
            sensing=DURATION_SENSING_MS,
            processing=DURATION_PROCESSING_MS,
            transmitting=DURATION_TRANSMITTING_MS,
        ),
        thresholds_v=controller.VoltageThresholdsV(
            low=THRESHOLD_LOW_V,
            high=THRESHOLD_HIGH_V,
        ),
        max_startup_time_ms=STARTUP_MAX_MS,
    )
    cap_params = capacitor.CapacitorParams(
        dt=CAPACITOR_DT_SECONDS,
        R_series=R_SERIES_OHM,
        R_leakage=R_LEAKAGE_OHM,
        C=CAPACITANCE_F,
    )

    env = simpy.Environment()
    energy_source = energy.EnvEnergySource(
        env=env,
        file_path=str(trace_path),
        column=ENERGY_TRACE_COLUMN,
        resistance=ENERGY_TRACE_RESISTANCE_OHM,
    )
    if len(getattr(energy_source, "_voltages", ())) < 1:
        raise RuntimeError("ambient-v1 energy trace is empty")
    coverage = propagation.CoverageMap(
        base_stations=[bs],
        nodes=nodes,
        freq_hz=FREQUENCY_HZ,
        pathloss_model=PATHLOSS_MODEL,
        los=LOS,
        node_energy_mode=ENERGY_MODE,
        node_ext_power_fn=lambda node: energy_source.ext_power,
        combine_mode=ENERGY_COMBINE_MODE,
        bandwidth_hz=BANDWIDTH_HZ,
        noise_figure_db=NOISE_FIGURE_DB,
    )
    coverage.compute_coverage_map(-120.0, 120.0, -120.0, 120.0, step_m=2.0)
    downlink = coverage.compute_bs_to_point(nodes)
    coverage.calculate_node_power(nodes, downlink)
    uplink = coverage.compute_point_to_bs(nodes)

    capacitors = []
    modules = []
    controllers = []
    for node in nodes:
        cap = capacitor.Capacitor(
            env=env,
            id=node.id,
            params=cap_params,
            initial_voltage=CAPACITOR_INITIAL_V,
        )
        capacitors.append(cap)
        module = PeriodicAlohaNode(
            env=env,
            node=node,
            bs_processes=[],
            uplink_results=uplink,
            downlink_results=downlink,
        )
        modules.append(module)
        ctrl = controller.Controller(
            env=env,
            capacitor_ctrl=cap,
            node=node,
            params=params,
            backscatter=module,
            coverage_map=coverage,
            downlink_results=downlink,
        )
        controllers.append(ctrl)
        module.attach_runtime(cap, ctrl)

    behavior = bsengine.BSBehavior(
        env=env,
        base_station=bs,
        policy=periodic_policy,
        backscatter_modules=modules,
        required_sinr_db=REQUIRED_SINR_DB,
        enable_sic=SIC_ENABLED,
        cancellation_factor=SIC_CANCELLATION_FACTOR,
        noise_figure_db=NOISE_FIGURE_DB,
        bandwidth_hz=BANDWIDTH_HZ,
    )
    for module in modules:
        module.bs_processes = [behavior]

    env.run(until=duration_ms)

    all_rx_packets = list(behavior.rx_packets)
    resolution = _collision_resolution(all_rx_packets, packet_analysis)
    uplink_by_node = _per_node_uplink(uplink, [node.id for node in nodes])
    downlink_by_node = (
        downlink.get("best_pw_dbm", {}) if isinstance(downlink, Mapping) else {}
    )

    periods = (duration_ms + period_ms - 1) // period_ms
    records: list[dict[str, Any]] = []
    for period_index in range(periods):
        start = period_index * period_ms
        end = min(start + period_ms, duration_ms)
        sequence = period_index + 1
        for node, module, cap, ctrl in zip(nodes, modules, capacitors, controllers):
            tx_records = [
                record
                for record in module.tx_records
                if start <= record.end_ms < end and record.payload_type == "data"
            ]
            tx_runtime = [
                record
                for record in module.tx_runtime_history
                if start <= record["time_ms"] < end
            ]
            rx_packets = [
                packet
                for packet in all_rx_packets
                if start <= packet.start_ms < end and packet.node_id == node.id
            ]
            decoded_packets = [packet for packet in rx_packets if not packet.collided]
            collect_records = [
                record
                for record in module.collect_history
                if start <= record["time_ms"] < end
            ]
            collect = collect_records[0] if collect_records else None
            transmitted = bool(tx_records)
            decoded = bool(decoded_packets)

            decode_mechanism: str | None = None
            collision_group_size = 0
            if decoded_packets:
                decode_mechanism, collision_group_size = resolution.get(
                    id(decoded_packets[0]), ("decoded", 1)
                )
                outcome = decode_mechanism
            elif transmitted and any(packet.collided for packet in rx_packets):
                outcome = "collision"
                collision_group_size = max(
                    (
                        resolution.get(id(packet), ("collision", 1))[1]
                        for packet in rx_packets
                        if packet.collided
                    ),
                    default=1,
                )
            elif transmitted:
                outcome = "transmit-undecoded"
            elif collect is None:
                outcome = "downlink-sensitivity"
            else:
                startup_delay_ms = float(collect.get("startup_delay_ms") or 0.0)
                slot_start_ms = collect.get("slot_start_ms")
                slot_voltage = (
                    _voltage_at(cap, float(slot_start_ms))
                    if slot_start_ms is not None
                    else None
                )
                collect_voltage = collect.get("capacitor_voltage_v")
                if float(collect["time_ms"]) <= startup_delay_ms:
                    outcome = "startup"
                elif (
                    collect_voltage is not None
                    and not bool(collect.get("controller_active"))
                    and float(collect_voltage) < THRESHOLD_HIGH_V
                ):
                    outcome = "energy-below-threshold"
                elif slot_voltage is not None and slot_voltage <= THRESHOLD_LOW_V:
                    outcome = "energy-below-threshold"
                elif collect.get("slot_index") is not None:
                    outcome = "slot-missed"
                else:
                    outcome = "controller-not-ready"

            slot_index = tx_records[0].slot_idx if tx_records else (
                int(collect["slot_index"])
                if collect is not None and collect.get("slot_index") is not None
                else None
            )
            uplink_dbm, uplink_sector = uplink_by_node[node.id]
            collect_voltage = (
                collect.get("capacitor_voltage_v") if collect is not None else None
            )
            slot_start_ms = collect.get("slot_start_ms") if collect is not None else None
            tx_voltage = (
                tx_runtime[0].get("capacitor_voltage_v") if tx_runtime else None
            )
            records.append(
                _event(
                    config,
                    node_id=node.id,
                    sequence=sequence,
                    planned_at_ms=start,
                    transmitted=transmitted,
                    decoded=decoded,
                    outcome=outcome,
                    slot_index=slot_index,
                    details={
                        "x_m": node.x,
                        "y_m": node.y,
                        "downlink_dbm": downlink_by_node.get(node.id),
                        "uplink_dbm": uplink_dbm,
                        "uplink_sector": uplink_sector,
                        "tx_records": len(tx_records),
                        "rx_packets": len(rx_packets),
                        "collided_packets": sum(packet.collided for packet in rx_packets),
                        "collision_group_size": collision_group_size,
                        "decode_mechanism": decode_mechanism,
                        "amber_payload": tx_records[0].payload if tx_records else None,
                        "collect_received": collect is not None,
                        "controller_active_at_collect": (
                            collect.get("controller_active") if collect is not None else None
                        ),
                        "controller_state_at_collect": (
                            collect.get("controller_state") if collect is not None else None
                        ),
                        "controller_state_final": str(ctrl.state_name),
                        "startup_delay_ms": (
                            collect.get("startup_delay_ms") if collect is not None else None
                        ),
                        "slot_start_ms": slot_start_ms,
                        "slot_end_ms": (
                            collect.get("slot_end_ms") if collect is not None else None
                        ),
                        "capacitor_voltage_start_v": _voltage_at(cap, float(start)),
                        "capacitor_voltage_collect_v": collect_voltage,
                        "capacitor_voltage_slot_v": (
                            _voltage_at(cap, float(slot_start_ms))
                            if slot_start_ms is not None
                            else None
                        ),
                        "capacitor_voltage_tx_v": tx_voltage,
                        "capacitor_voltage_end_v": _voltage_at(cap, float(end)),
                        # Backward-compatible diagnostic now means the voltage
                        # at the actual collect opportunity, not period end.
                        "capacitor_voltage_v": collect_voltage,
                    },
                )
            )

    records.sort(key=lambda item: (item["planned_at_ms"], item["sensor_id"]))
    return records


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Prepare an immutable Amber source-event plan")
    parser.add_argument("--amber-root", type=Path, required=True)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)

    amber_root = args.amber_root.resolve()
    sys.path.insert(0, str(amber_root))
    config = _load_input(args.input)
    profile = config.get("iot_profile")
    if profile == TRANSPORT_PROFILE:
        events = _transport_plan(config)
    elif profile == AMBIENT_PROFILE:
        events = _ambient_plan(config)
    else:
        raise RuntimeError(f"unsupported Amber profile: {profile!r}")

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        "".join(
            json.dumps(event, sort_keys=True, separators=(",", ":")) + "\n"
            for event in events
        ),
        encoding="utf-8",
        newline="\n",
    )
    print(
        json.dumps(
            {
                "profile": profile,
                "planned": len(events),
                "decoded": sum(event["decoded"] for event in events),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
