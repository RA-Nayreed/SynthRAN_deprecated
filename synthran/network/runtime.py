"""Read-only network evidence for experiments on a 5g-Ansible deployment."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import ipaddress
import json
from pathlib import Path
import re
import tempfile
from typing import Any, Callable, Mapping

from synthran.dependencies import DependencyLock
from synthran.fiveg_ansible import NetworkInventory
from synthran.live_preflight import (
    LivePreflightError,
    Runner,
    ssh_command,
    subprocess_runner,
)


DEPLOYMENT_SCHEMA = "fiveg/deployment-manifest/v1"
NETWORK_EVIDENCE_SCHEMA = "synthran/network-evidence/v1alpha1"
RUN_ID_RE = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
KUBERNETES_NAME_RE = re.compile(r"^[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?$")
RFSIM_NAMESPACE = "open5gs"
RFSIM_INTERFACE = "tun_srsue1"
RFSIM_PDU_NETWORK = ipaddress.ip_network("12.1.0.0/16")


class NetworkRuntimeError(RuntimeError):
    """Raised when upstream provenance or experiment-path observation fails."""


def validate_run_id(value: str) -> str:
    if not RUN_ID_RE.fullmatch(value):
        raise NetworkRuntimeError(
            "run ID must be 1-63 lowercase letters, numbers, or internal hyphens"
        )
    return value


def atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    content = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        newline="\n",
        dir=path.parent,
        delete=False,
    ) as temporary:
        temporary.write(content)
        temporary_path = Path(temporary.name)
    temporary_path.replace(path)


def _locked_fiveg_commit(lock: DependencyLock) -> str:
    dependency = next((item for item in lock.git if item.name == "fiveg_ansible"), None)
    if dependency is None:
        raise NetworkRuntimeError("dependency lock does not define fiveg_ansible")
    return dependency.commit


def load_deployment_manifest(
    *,
    path: Path,
    run_id: str,
    lock: DependencyLock,
    **_: Any,
) -> Mapping[str, Any]:
    """Validate upstream deployment identity without reconstructing its truth."""

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise NetworkRuntimeError("5g-Ansible deployment manifest was not found") from exc
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise NetworkRuntimeError("5g-Ansible deployment manifest must be readable JSON") from exc
    if not isinstance(payload, dict) or payload.get("schema") != DEPLOYMENT_SCHEMA:
        raise NetworkRuntimeError("5g-Ansible deployment manifest schema is unsupported")
    if payload.get("id") != validate_run_id(run_id):
        raise NetworkRuntimeError("5g-Ansible deployment manifest ID does not match")
    if payload.get("state") != "ready":
        raise NetworkRuntimeError("5g-Ansible deployment is not ready")
    if payload.get("fiveg_ansible_commit") != _locked_fiveg_commit(lock):
        raise NetworkRuntimeError("5g-Ansible deployment provenance does not match the lock")
    return payload


@dataclass(frozen=True)
class VerificationCheck:
    name: str
    passed: bool
    detail: str


@dataclass(frozen=True)
class NetworkVerificationReport:
    run_id: str
    generated_at_utc: datetime
    inventory_sha256: str
    dependencies: Mapping[str, str]
    checks: tuple[VerificationCheck, ...]
    pdu_address: str | None = None

    @property
    def ready(self) -> bool:
        return bool(self.checks) and all(check.passed for check in self.checks)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": NETWORK_EVIDENCE_SCHEMA,
            "run_id": self.run_id,
            "generated_at_utc": self.generated_at_utc.isoformat().replace("+00:00", "Z"),
            "inventory_sha256": self.inventory_sha256,
            "dependencies": dict(self.dependencies),
            "path": {
                "experiment_requirement": "amber-rfsim-pdu",
                "ue_interface": RFSIM_INTERFACE,
                "pdu_address": self.pdu_address,
                "pdu_network": str(RFSIM_PDU_NETWORK),
            },
            "checks": [
                {"name": item.name, "passed": item.passed, "detail": item.detail}
                for item in self.checks
            ],
            "ready": self.ready,
        }

    def render(self) -> str:
        lines = [f"SynthRAN RFSIM path verification ({self.run_id})"]
        for check in self.checks:
            lines.append(f"[{'PASS' if check.passed else 'FAIL'}] {check.name}: {check.detail}")
        lines.append(f"Result: {'PATH PROVEN' if self.ready else 'NOT PROVEN'}")
        return "\n".join(lines)


def _remote_json(
    runner: Runner,
    inventory: NetworkInventory,
    command: str,
    timeout_seconds: int,
    label: str,
) -> Any:
    result = runner(
        ssh_command(inventory.core_node, "sh", "-c", command),
        timeout_seconds,
    )
    if result.returncode != 0:
        raise NetworkRuntimeError(f"{label} failed")
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise NetworkRuntimeError(f"{label} did not return JSON") from exc


def _one_ready_pod(payload: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(payload, dict) or not isinstance(payload.get("items"), list):
        raise NetworkRuntimeError(f"{label} pod evidence is malformed")
    active = [
        item
        for item in payload["items"]
        if isinstance(item, dict)
        and not (
            isinstance(item.get("metadata"), dict)
            and item["metadata"].get("deletionTimestamp") is not None
        )
    ]
    if len(active) != 1:
        raise NetworkRuntimeError(f"expected exactly one active {label} pod")
    pod = active[0]
    metadata, status = pod.get("metadata"), pod.get("status")
    if not isinstance(metadata, dict) or not isinstance(status, dict):
        raise NetworkRuntimeError(f"{label} pod evidence is incomplete")
    conditions = status.get("conditions")
    ready = isinstance(conditions, list) and any(
        isinstance(item, dict)
        and item.get("type") == "Ready"
        and item.get("status") == "True"
        for item in conditions
    )
    name = metadata.get("name")
    if not ready or not isinstance(name, str) or not KUBERNETES_NAME_RE.fullmatch(name):
        raise NetworkRuntimeError(f"{label} pod is not safely Ready")
    return pod


def verify_network_path(
    *,
    inventory: NetworkInventory,
    lock: DependencyLock,
    run_id: str,
    runner: Runner = subprocess_runner,
    timeout_seconds: int = 30,
    now: datetime | None = None,
) -> NetworkVerificationReport:
    """Read-only proof of the current Amber-over-RFSIM PDU path."""

    run_id = validate_run_id(run_id)
    checks: list[VerificationCheck] = []
    pdu_address: str | None = None

    def record(name: str, operation: Callable[[], str]) -> None:
        try:
            detail = operation()
        except (NetworkRuntimeError, LivePreflightError) as exc:
            checks.append(VerificationCheck(name, False, str(exc)))
        else:
            checks.append(VerificationCheck(name, True, detail))

    def discover(selector: str, label: str) -> Mapping[str, Any]:
        return _one_ready_pod(
            _remote_json(
                runner,
                inventory,
                "KUBECONFIG=/etc/kubernetes/admin.conf "
                f"kubectl get pods -n {RFSIM_NAMESPACE} -l {selector} -o json",
                timeout_seconds,
                f"{label} discovery",
            ),
            label,
        )

    pods: dict[str, Mapping[str, Any]] = {}
    for label, selector in (
        ("gnb", "app=srsran,component=gnb"),
        ("srsue", "app=srsran,component=ue"),
        ("slice1-upf", "app=open5gs,nf=upf,name=upf1"),
    ):
        def operation(label: str = label, selector: str = selector) -> str:
            pods[label] = discover(selector, label)
            return "exactly one active Ready pod observed"
        record(label, operation)

    gnb = pods.get("gnb")
    if gnb is None:
        checks.append(VerificationCheck("gnb-cell", False, "gNB pod is unavailable"))
    else:
        gnb_name = str(gnb["metadata"]["name"])

        def cell() -> str:
            result = runner(
                ssh_command(
                    inventory.core_node,
                    "sh",
                    "-c",
                    "KUBECONFIG=/etc/kubernetes/admin.conf "
                    f"kubectl exec -n {RFSIM_NAMESPACE} {gnb_name} -c gnb-logs -- "
                    "grep -q 'Cell was activated' /var/log/gnb.log",
                ),
                timeout_seconds,
            )
            if result.returncode != 0:
                raise NetworkRuntimeError("gNB cell activation was not observed")
            return "current gNB log reports an activated cell"
        record("gnb-cell", cell)

    ue = pods.get("srsue")
    if ue is None:
        checks.append(VerificationCheck("ue-tunnel", False, "srsUE pod is unavailable"))
    else:
        ue_name = str(ue["metadata"]["name"])

        def tunnel() -> str:
            nonlocal pdu_address
            interfaces = _remote_json(
                runner,
                inventory,
                "KUBECONFIG=/etc/kubernetes/admin.conf "
                f"kubectl exec -n {RFSIM_NAMESPACE} {ue_name} -c ue -- "
                f"ip -j address show dev {RFSIM_INTERFACE}",
                timeout_seconds,
                "UE tunnel probe",
            )
            if not isinstance(interfaces, list) or len(interfaces) != 1:
                raise NetworkRuntimeError("UE tunnel evidence is malformed")
            interface = interfaces[0]
            if not isinstance(interface, dict) or "UP" not in interface.get("flags", []):
                raise NetworkRuntimeError(f"{RFSIM_INTERFACE} is not UP")
            candidates: list[ipaddress.IPv4Address] = []
            for item in interface.get("addr_info", []):
                if isinstance(item, dict) and item.get("family") == "inet":
                    try:
                        address = ipaddress.ip_address(item.get("local"))
                    except ValueError:
                        continue
                    if isinstance(address, ipaddress.IPv4Address) and address in RFSIM_PDU_NETWORK:
                        candidates.append(address)
            if len(candidates) != 1:
                raise NetworkRuntimeError("UE does not have exactly one expected PDU address")
            routes = _remote_json(
                runner,
                inventory,
                "KUBECONFIG=/etc/kubernetes/admin.conf "
                f"kubectl exec -n {RFSIM_NAMESPACE} {ue_name} -c ue -- ip -j route show",
                timeout_seconds,
                "UE route probe",
            )
            if not isinstance(routes, list) or not any(
                isinstance(route, dict)
                and route.get("dst") == str(RFSIM_PDU_NETWORK)
                and route.get("dev") == RFSIM_INTERFACE
                for route in routes
            ):
                raise NetworkRuntimeError("UE PDU route is missing")
            pdu_address = str(candidates[0])
            return f"{RFSIM_INTERFACE} is UP with the expected PDU address and route"
        record("ue-tunnel", tunnel)

    upf = pods.get("slice1-upf")
    if upf is None:
        checks.append(VerificationCheck("upf-route", False, "UPF pod is unavailable"))
    else:
        upf_name = str(upf["metadata"]["name"])

        def upf_route() -> str:
            routes = _remote_json(
                runner,
                inventory,
                "KUBECONFIG=/etc/kubernetes/admin.conf "
                f"kubectl exec -n {RFSIM_NAMESPACE} {upf_name} -- ip -j route show",
                timeout_seconds,
                "UPF route probe",
            )
            if not isinstance(routes, list) or not any(
                isinstance(route, dict)
                and route.get("dst") == str(RFSIM_PDU_NETWORK)
                and route.get("dev") == "ogstun"
                for route in routes
            ):
                raise NetworkRuntimeError("UPF PDU route through ogstun is missing")
            return "PDU network is currently routed through ogstun"
        record("upf-route", upf_route)

    dependencies = {
        item.name: item.commit for item in lock.git if item.name == "fiveg_ansible"
    }
    return NetworkVerificationReport(
        run_id=run_id,
        generated_at_utc=(now or datetime.now(timezone.utc)).astimezone(timezone.utc),
        inventory_sha256=inventory.sha256,
        dependencies=dependencies,
        checks=tuple(checks),
        pdu_address=pdu_address,
    )


def save_network_evidence(
    report: NetworkVerificationReport,
    destination: Path,
    manifest_path: Path | None = None,
) -> None:
    """Persist experiment evidence without mutating the upstream manifest."""

    del manifest_path
    atomic_json(destination, report.to_dict())
