"""Read-only observation helpers for already-deployed 5G experiment paths."""

from __future__ import annotations

import ipaddress
from typing import Any, Mapping

from synthran.experiment import EXPECTED_PDU_NETWORK, ExperimentError
from synthran.experiment.live import _remote, _remote_json
from synthran.fiveg_ansible import NetworkInventory


KUBERNETES_NAMESPACE = "open5gs"
NETWORK_RUN_LABEL = "synthran.run/id"
RFSIM_UE_INTERFACE = "tun_srsue1"


def _one_active_name(payload: Mapping[str, Any], *, label: str) -> str:
    items = payload.get("items")
    if not isinstance(items, list):
        raise ExperimentError(f"{label} discovery returned malformed data")
    active: list[Mapping[str, Any]] = []
    for item in items:
        if not isinstance(item, Mapping):
            continue
        metadata = item.get("metadata")
        if not isinstance(metadata, Mapping):
            continue
        if metadata.get("deletionTimestamp") is not None:
            continue
        active.append(item)
    if len(active) != 1:
        raise ExperimentError(f"expected exactly one active {label}, found {len(active)}")
    metadata = active[0].get("metadata")
    name = metadata.get("name") if isinstance(metadata, Mapping) else None
    if not isinstance(name, str) or not name:
        raise ExperimentError(f"{label} metadata is malformed")
    return name


def discover_rfsim_ue_pod(
    inventory: NetworkInventory,
    network_run_id: str,
) -> str:
    """Return the one active upstream-owned RFSIM UE pod for a network run."""

    selector = f"app=srsran,component=ue,{NETWORK_RUN_LABEL}={network_run_id}"
    payload = _remote_json(
        inventory,
        "KUBECONFIG=/etc/kubernetes/admin.conf kubectl get pods "
        f"-n {KUBERNETES_NAMESPACE} -l {selector} -o json",
        label="active RFSIM UE observation",
    )
    return _one_active_name(payload, label="RFSIM UE pod")


def rfsim_pdu_address(inventory: NetworkInventory, ue_pod: str) -> str:
    """Read the current IPv4 PDU address without mutating UE state."""

    raw = _remote(
        inventory,
        "sh",
        "-c",
        "KUBECONFIG=/etc/kubernetes/admin.conf kubectl exec "
        f"-n {KUBERNETES_NAMESPACE} {ue_pod} -c ue -- "
        f"ip -j address show dev {RFSIM_UE_INTERFACE}",
        label="RFSIM PDU address observation",
    )
    import json

    try:
        interfaces = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ExperimentError("RFSIM PDU address observation returned invalid JSON") from exc
    if not isinstance(interfaces, list):
        raise ExperimentError("RFSIM PDU address observation returned malformed data")
    candidates: list[str] = []
    for interface in interfaces:
        if not isinstance(interface, Mapping):
            continue
        for entry in interface.get("addr_info", []):
            if not isinstance(entry, Mapping) or entry.get("family") != "inet":
                continue
            local = entry.get("local")
            if not isinstance(local, str):
                continue
            try:
                address = ipaddress.ip_address(local)
            except ValueError:
                continue
            if address in EXPECTED_PDU_NETWORK:
                candidates.append(str(address))
    if len(candidates) != 1:
        raise ExperimentError(
            f"expected exactly one UE PDU address in {EXPECTED_PDU_NETWORK}, found {len(candidates)}"
        )
    return candidates[0]


def interface_counter(
    inventory: NetworkInventory,
    ue_pod: str,
    interface: str,
    counter: str,
) -> int:
    """Read one interface byte counter from the existing UE pod."""

    if counter not in {"rx_bytes", "tx_bytes"}:
        raise ExperimentError("unsupported interface counter")
    output = _remote(
        inventory,
        "sh",
        "-c",
        "KUBECONFIG=/etc/kubernetes/admin.conf kubectl exec "
        f"-n {KUBERNETES_NAMESPACE} {ue_pod} -c ue -- "
        f"cat /sys/class/net/{interface}/statistics/{counter}",
        label=f"{interface} {counter} observation",
    ).strip()
    if not output.isdigit():
        raise ExperimentError(f"{interface} {counter} observation returned invalid data")
    return int(output)


def core_address(inventory: NetworkInventory) -> str:
    value = inventory.core_node.variables.get("ip")
    if not value:
        raise ExperimentError("upstream inventory is missing the core node IP address")
    try:
        ipaddress.ip_address(value)
    except ValueError as exc:
        raise ExperimentError("upstream inventory has an invalid core node IP address") from exc
    return value
