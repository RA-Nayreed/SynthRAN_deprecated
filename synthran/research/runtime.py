"""Capacity calibration for the accepted RFSIM research path."""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

from synthran.dependencies import DependencyLock
from synthran.experiment import live as base_runtime
from synthran.fiveg_ansible import NetworkInventory
from synthran.network.rfsim import reconcile_rfsim_runtime
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
    """Measure one reference capacity without owning the accepted network."""

    if not 5 <= duration_seconds <= 120:
        raise ResearchError("calibration duration must be between 5 and 120 seconds")
    base = verify_network_path(
        inventory=inventory,
        lock=lock,
        run_id=network_run_id,
        timeout_seconds=120,
    )
    if not base.ready:
        raise ResearchError(
            "capacity calibration requires a currently path-proven network"
        )

    state = reconcile_rfsim_runtime(inventory, network_run_id=network_run_id)
    ue_pod = state.ue_pod
    _check_research_tools(inventory, ue_pod, load_enabled=True)
    route_installed = _install_target_route(
        inventory,
        ue_pod,
        pdu_address=state.pdu_address,
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
            state.pdu_address,
            "-p",
            str(server_port),
            "--connect-timeout",
            "5000",
            "-t",
            str(duration_seconds),
            "-J",
        )
        result = base_runtime._run(command, timeout_seconds=duration_seconds + 20)
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
            "pdu_address": state.pdu_address,
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
                pdu_address=state.pdu_address,
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
