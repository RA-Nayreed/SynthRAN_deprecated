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


def _ambient_plan(config: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Run a seeded ten-node Amber hybrid-energy framed-slotted ALOHA model."""

    import simpy
    from amber import backscatter, bsengine, capacitor, controller, energy, propagation, radiodevices

    seed = int(config["iot_seed"])
    random.seed(seed)
    duration_ms = int(config["duration_seconds"]) * 1000
    period_ms = int(config["sensor_period_seconds"]) * 1000
    count = int(config["sensor_count"])
    trace_path = Path(str(config["energy_trace_path"]))
    if not trace_path.is_file():
        raise RuntimeError("ambient-v1 energy trace is missing")

    command_ms = 5
    slot_ms = 8
    n_slots = 16
    active_frame_ms = command_ms + slot_ms * n_slots
    if period_ms <= active_frame_ms:
        raise RuntimeError("ambient-v1 sensor period is too short for its ALOHA frame")
    idle_ms = period_ms - active_frame_ms

    class PeriodicAlohaNode(backscatter.BackscatterModule):
        def handle_command(self, cmd, bs_id, data):
            del bs_id, data
            if cmd == "collect":
                self.chosen_slot_idx = (
                    random.randint(0, len(self.rx_slots) - 1) if self.rx_slots else -1
                )
                self.state = "active"
                self.last_tx_command_time = self.env.now

        def build_tx_payload(self, sensor_data):
            if self.state == "active":
                return {"type": "data", "payload": sensor_data}
            return None

    def periodic_policy(bs):
        del bs
        while True:
            frame = [
                ("tx", command_ms, "collect", {"cmd": "collect", "target": -1})
            ]
            frame.extend(("rx", slot_ms, f"slot-{index:02d}") for index in range(n_slots))
            frame.append(
                ("tx", idle_ms, "idle", {"cmd": "idle", "target": -2})
            )
            yield frame

    nodes = []
    for node_id in range(count):
        radius = random.uniform(5.0, 40.0)
        angle = random.uniform(0.0, 2.0 * math.pi)
        nodes.append(
            radiodevices.Node(
                id=node_id,
                x=radius * math.cos(angle),
                y=radius * math.sin(angle),
                height=1.5,
                sensitivity_dbm=-100.0,
                efficiency=0.7,
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
                beamwidth_deg=65.0,
                power=46.0,
                antenna_gain_dbi=15.0,
                sensitivity_dbm=-100.0,
                height=25.0,
            )
            for angle in (0.0, 120.0, 240.0)
        ],
    )

    params = controller.ControllerParams(
        currents=controller.CurrentsA(
            listening=1.4e-4,
            sensing=0.512e-3,
            processing=1.28e-3,
            transmitting=5e-3,
        ),
        durations_ms=controller.DurationsMs(
            listening=5,
            sensing=2,
            processing=5,
            transmitting=15,
        ),
        thresholds_v=controller.VoltageThresholdsV(low=1.3, high=1.7),
    )
    cap_params = capacitor.CapacitorParams(
        dt=0.001,
        R_series=5000.0,
        R_leakage=100000.0,
        C=300e-6,
    )

    env = simpy.Environment()
    energy_source = energy.EnvEnergySource(
        env=env,
        file_path=str(trace_path),
        column="V_IM",
        resistance=5000.0,
    )
    coverage = propagation.CoverageMap(
        base_stations=[bs],
        nodes=nodes,
        freq_hz=924e6,
        pathloss_model="macro",
        los=True,
        node_energy_mode="hybrid",
        node_ext_power_fn=lambda node: energy_source.ext_power,
        combine_mode="max",
    )
    coverage.compute_coverage_map(-120.0, 120.0, -120.0, 120.0, step_m=2.0)
    downlink = coverage.compute_bs_to_point(nodes)
    coverage.calculate_node_power(nodes, downlink)
    uplink = coverage.compute_point_to_bs(nodes)

    capacitors = []
    modules = []
    for node in nodes:
        cap = capacitor.Capacitor(
            env=env,
            id=node.id,
            params=cap_params,
            initial_voltage=0.0,
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
        controller.Controller(
            env=env,
            capacitor_ctrl=cap,
            node=node,
            params=params,
            backscatter=module,
            coverage_map=coverage,
            downlink_results=downlink,
        )

    behavior = bsengine.BSBehavior(
        env=env,
        base_station=bs,
        policy=periodic_policy,
        backscatter_modules=modules,
        enable_sic=True,
    )
    for module in modules:
        module.bs_processes = [behavior]

    env.run(until=duration_ms)

    periods = (duration_ms + period_ms - 1) // period_ms
    best_uplink = uplink.get("best_pw_dbm", {}) if isinstance(uplink, dict) else {}
    records: list[dict[str, Any]] = []
    for period_index in range(periods):
        start = period_index * period_ms
        end = min(start + period_ms, duration_ms)
        sequence = period_index + 1
        for node, module, cap in zip(nodes, modules, capacitors):
            tx_records = [
                record
                for record in module.tx_records
                if start <= record.end_ms < end and record.payload_type == "data"
            ]
            rx_packets = [
                packet
                for packet in behavior.rx_packets
                if start <= packet.start_ms < end and packet.node_id == node.id
            ]
            decoded_packets = [packet for packet in rx_packets if not packet.collided]
            transmitted = bool(tx_records)
            decoded = bool(decoded_packets)
            if decoded:
                outcome = "decoded"
            elif transmitted and any(packet.collided for packet in rx_packets):
                outcome = "collision"
            elif transmitted:
                outcome = "transmit-undecoded"
            else:
                received_collect = any(
                    start <= record.end_ms < end and record.cmd == "collect"
                    for record in module.rx_records
                )
                outcome = (
                    "energy-or-controller-silence"
                    if received_collect
                    else "downlink-sensitivity"
                )

            slot_index = tx_records[0].slot_idx if tx_records else None
            voltage = None
            if cap.voltage_history:
                before_end = [value for time_s, value in cap.voltage_history if time_s * 1000 <= end]
                voltage = before_end[-1] if before_end else None
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
                        "uplink_dbm": best_uplink.get(node.id),
                        "tx_records": len(tx_records),
                        "rx_packets": len(rx_packets),
                        "collided_packets": sum(packet.collided for packet in rx_packets),
                        "capacitor_voltage_v": voltage,
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
