"""Capacity calibration for an already accepted RFSIM deployment.

This module is experiment instrumentation only. It performs no network
reconciliation or deployment mutation: 5g-Ansible owns the network, while
SynthRAN verifies the current PDU path, runs a bounded measurement, and removes
only the route/server state it created for that measurement.
"""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

from synthran.dependencies import DependencyLock
from synthran.experiment import live as experiment_live
from synthran.fiveg_ansible import NetworkInventory
from synthran.network.runtime import verify_network_path
from synthran.research import CAPACITY_SCHEMA, ResearchError
from synthran.research.instrumentation import (
    _check_research_tools,
    _extract_iperf_bps,
    _install_target_route,
    _kubectl_exec_command,
    _remove_target_route,
)
from synthran.research.iperf import (
    OwnedIperfServer,
    start_owned_iperf_server,
    stop_owned_iperf_server,
)


def _discover_ue_pod(inventory: NetworkInventory) -> str:
    payload = experiment_live._remote_json(
        inventory,
        "KUBECONFIG=/etc/kubernetes/admin.conf kubectl get pods "
        "-n open5gs -l app=srsran,component=ue -o json",
        label="RFSIM UE observation",
    )
    items = payload.get("items")
    if not isinstance(items, list):
        raise ResearchError("RFSIM UE observation is malformed")
    active: list[Mapping[str, Any]] = []
    for item in items:
        if not isinstance(item, Mapping):
            continue
        metadata = item.get("metadata")
        if not isinstance(metadata, Mapping) or metadata.get("deletionTimestamp") is not None:
            continue
        active.append(item)
    if len(active) != 1:
        raise ResearchError("capacity calibration requires exactly one active RFSIM UE pod")
    metadata = active[0].get("metadata")
    name = metadata.get("name") if isinstance(metadata, Mapping) else None
    if not isinstance(name, str) or not name:
        raise ResearchError("RFSIM UE pod identity is malformed")
    return name


def calibrate_capacity(
    *,
    inventory: NetworkInventory,
    lock: DependencyLock,
    network_run_id: str,
    target: str,
    repository_root: Path,
    output_path: Path,
    duration_seconds: int = 10,
    server_port: int = 5201,
) -> Mapping[str, Any]:
    """Measure reference capacity without repairing or redeploying the network."""

    if duration_seconds < 5 or duration_seconds > 120:
        raise ResearchError("calibration duration must be between 5 and 120 seconds")
    report = verify_network_path(
        inventory=inventory,
        lock=lock,
        run_id=network_run_id,
        timeout_seconds=120,
    )
    if not report.ready or not isinstance(report.pdu_address, str):
        raise ResearchError(
            "capacity calibration requires a currently path-proven network"
        )
    ue_pod = _discover_ue_pod(inventory)
    pdu_address = report.pdu_address
    _check_research_tools(inventory, ue_pod, load_enabled=True)
    route_installed = _install_target_route(
        inventory,
        ue_pod,
        pdu_address=pdu_address,
        target=target,
    )
    owner_id = "cal-" + hashlib.sha256(
        f"{network_run_id}:{target}:{server_port}".encode("utf-8")
    ).hexdigest()[:16]
    server: OwnedIperfServer | None = None
    failure: Exception | None = None
    payload: Mapping[str, Any] | None = None
    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        server = start_owned_iperf_server(
            inventory=inventory,
            owner_id=owner_id,
            port=server_port,
            repository_root=repository_root,
            log_path=output_path.with_suffix(".server.log"),
        )
        command = _kubectl_exec_command(
            inventory,
            ue_pod,
            "iperf3",
            "-c",
            target,
            "-B",
            pdu_address,
            "-p",
            str(server_port),
            "--connect-timeout",
            "5000",
            "-t",
            str(duration_seconds),
            "-J",
        )
        result = experiment_live._run(command, timeout_seconds=duration_seconds + 20)
        if result.returncode != 0:
            raise ResearchError("capacity calibration client failed")
        try:
            value = json.loads(result.stdout)
        except json.JSONDecodeError as exc:
            raise ResearchError(
                "capacity calibration produced invalid iperf3 JSON"
            ) from exc
        if not isinstance(value, Mapping):
            raise ResearchError("capacity calibration result is malformed")
        capacity = _extract_iperf_bps(value)
        if capacity is None or capacity <= 0:
            raise ResearchError(
                "capacity calibration did not report positive throughput"
            )
        payload = {
            "schema": CAPACITY_SCHEMA,
            "network_run_id": network_run_id,
            "ue_pod": ue_pod,
            "ue_interface": "tun_srsue1",
            "pdu_address": pdu_address,
            "target": target,
            "duration_seconds": duration_seconds,
            "reference_capacity_bps": round(capacity),
            "measured_at_utc": datetime.now(timezone.utc)
            .isoformat()
            .replace("+00:00", "Z"),
        }
        output_path.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    except Exception as exc:
        failure = exc

    cleanup_errors: list[str] = []
    if server is not None:
        try:
            stop_owned_iperf_server(inventory, server)
        except Exception as exc:
            cleanup_errors.append(f"iperf3 server: {exc}")
    if route_installed:
        try:
            _remove_target_route(
                inventory,
                ue_pod,
                pdu_address=pdu_address,
                target=target,
            )
        except Exception as exc:
            cleanup_errors.append(f"target route: {exc}")
    if failure is not None and cleanup_errors:
        raise ResearchError(
            "capacity calibration failed and cleanup failed closed: "
            f"{failure}; {'; '.join(cleanup_errors)}"
        ) from failure
    if failure is not None:
        raise failure
    if cleanup_errors:
        raise ResearchError(
            "capacity calibration cleanup failed closed: " + "; ".join(cleanup_errors)
        )
    assert payload is not None
    return payload
