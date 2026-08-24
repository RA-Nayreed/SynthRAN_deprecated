"""Evidence-backed acceptance of the reused SLICES and Open5GS foundation."""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import shlex
from typing import Mapping, Sequence

from synthran.live_preflight import Runner, subprocess_runner
from synthran.r2lab.acceptance import (
    PhysicalAcceptanceStage,
    PhysicalRunEvidence,
    R2LabAcceptanceError,
)
from synthran.r2lab.controller import authorize_physical_start
from synthran.r2lab.authority import PhysicalAuthorityGuard
from synthran.r2lab.deployment import (
    CORE_NODE,
    NAMESPACE,
    PhysicalStartAuthority,
    RAN_NODE,
)
from synthran.r2lab.handoff import (
    PhysicalNamespaceHandoffResult,
    R2LabPhysicalHandoffError,
    execute_physical_namespace_handoff,
)


REQUIRED_OPEN5GS_NFS = frozenset({"amf", "smf", "upf"})


class R2LabPhysicalFoundationError(RuntimeError):
    """Raised when the reused physical foundation cannot be accepted safely."""


@dataclass(frozen=True)
class PhysicalFoundationResult:
    run_id: str
    previous_run_id: str
    handoff: PhysicalNamespaceHandoffResult
    ready_node_count: int
    ready_open5gs_pod_count: int
    evidence_path: Path

    def to_dict(self) -> dict[str, object]:
        return {
            "run_id": self.run_id,
            "previous_run_id": self.previous_run_id,
            "namespace_changed": self.handoff.changed,
            "ready_node_count": self.ready_node_count,
            "ready_open5gs_pod_count": self.ready_open5gs_pod_count,
            "next_stage": PhysicalAcceptanceStage.GNB_N2.value,
            "status": "foundation-ready",
            "hardware_mutation": False,
        }


def _ssh(known_hosts: Path, *remote: str) -> tuple[str, ...]:
    return (
        "ssh",
        "-o",
        "BatchMode=yes",
        "-o",
        "ConnectTimeout=10",
        "-o",
        "StrictHostKeyChecking=yes",
        "-o",
        f"UserKnownHostsFile={known_hosts}",
        "-o",
        "GlobalKnownHostsFile=/dev/null",
        f"root@{CORE_NODE}",
        shlex.join(remote),
    )


def _checked(
    runner: Runner,
    command: Sequence[str],
    *,
    timeout_seconds: int,
    label: str,
) -> str:
    try:
        result = runner(command, timeout_seconds)
    except Exception as exc:
        raise R2LabPhysicalFoundationError(f"{label} could not be observed") from exc
    if result.returncode != 0:
        raise R2LabPhysicalFoundationError(f"{label} returned nonzero")
    return result.stdout


def _json_object(text: str, label: str) -> Mapping[str, object]:
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise R2LabPhysicalFoundationError(f"{label} did not return JSON") from exc
    if not isinstance(payload, dict):
        raise R2LabPhysicalFoundationError(f"{label} returned malformed JSON")
    return payload


def _ready_node_count(payload: Mapping[str, object]) -> int:
    items = payload.get("items")
    if not isinstance(items, list):
        raise R2LabPhysicalFoundationError("Kubernetes node evidence is malformed")
    ready_nodes: set[str] = set()
    for item in items:
        if not isinstance(item, dict):
            raise R2LabPhysicalFoundationError("Kubernetes node evidence is malformed")
        metadata = item.get("metadata")
        status = item.get("status")
        if not isinstance(metadata, dict) or not isinstance(status, dict):
            raise R2LabPhysicalFoundationError("Kubernetes node evidence is incomplete")
        name = metadata.get("name")
        conditions = status.get("conditions")
        if not isinstance(name, str) or not isinstance(conditions, list):
            raise R2LabPhysicalFoundationError(
                "Kubernetes node readiness is unavailable"
            )
        if any(
            isinstance(condition, dict)
            and condition.get("type") == "Ready"
            and condition.get("status") == "True"
            for condition in conditions
        ):
            ready_nodes.add(name.split(".", 1)[0])
    expected = {CORE_NODE, RAN_NODE}
    if not expected.issubset(ready_nodes):
        raise R2LabPhysicalFoundationError(
            "Kubernetes does not report both selected SLICES nodes Ready"
        )
    return len(expected)


def _require_ready_open5gs_pod(
    payload: Mapping[str, object], network_function: str
) -> None:
    items = payload.get("items")
    if not isinstance(items, list) or len(items) != 1:
        raise R2LabPhysicalFoundationError(
            f"Open5GS {network_function.upper()} does not have exactly one pod"
        )
    item = items[0]
    if not isinstance(item, dict):
        raise R2LabPhysicalFoundationError("Open5GS pod evidence is malformed")
    metadata = item.get("metadata")
    status = item.get("status")
    if not isinstance(metadata, dict) or not isinstance(status, dict):
        raise R2LabPhysicalFoundationError("Open5GS pod evidence is incomplete")
    labels = metadata.get("labels")
    if (
        not isinstance(labels, dict)
        or labels.get("app") != "open5gs"
        or labels.get("nf") != network_function
    ):
        raise R2LabPhysicalFoundationError("Open5GS pod identity is inconsistent")
    containers = status.get("containerStatuses")
    if (
        metadata.get("deletionTimestamp") is not None
        or not isinstance(containers, list)
        or not containers
        or not all(
            isinstance(container, dict) and container.get("ready") is True
            for container in containers
        )
    ):
        raise R2LabPhysicalFoundationError(
            f"Open5GS {network_function.upper()} pod is not Running and ready"
        )


def _foundation_evidence(run_id: str) -> PhysicalRunEvidence:
    evidence = PhysicalRunEvidence(run_id=run_id)
    for stage, source in (
        (PhysicalAcceptanceStage.RESOURCE_AUTHORITY, "current-r2lab-claim-lease-n300"),
        (PhysicalAcceptanceStage.SLICES_FOUNDATION, "current-slices-f2-f3-authority"),
        (PhysicalAcceptanceStage.KUBERNETES, "selected-slices-nodes-ready"),
        (PhysicalAcceptanceStage.OPEN5GS, "owned-open5gs-core-ready"),
    ):
        evidence = evidence.pass_stage(stage, source=source)
    return evidence


def execute_physical_foundation_acceptance(
    *,
    run_id: str,
    previous_run_id: str,
    slice_name: str,
    owner: str,
    allocation_id: str | None,
    known_hosts: Path,
    run_root: Path = Path(".synthran/r2lab"),
    r2lab_runner: Runner = subprocess_runner,
    foundation_runner: Runner = subprocess_runner,
    timeout_seconds: int = 120,
) -> PhysicalFoundationResult:
    """Accept only a current, healthy, stopped physical foundation."""

    if timeout_seconds < 30 or timeout_seconds > 600:
        raise R2LabPhysicalFoundationError(
            "foundation timeout must be between 30 and 600 seconds"
        )
    known_hosts = known_hosts.expanduser().resolve()

    def verify_r2lab_authority() -> PhysicalStartAuthority:
        return authorize_physical_start(
            run_id=run_id,
            slice_name=slice_name,
            run_root=run_root,
            runner=r2lab_runner,
            timeout_seconds=timeout_seconds,
        )

    try:
        authority = PhysicalAuthorityGuard.open(
            lease_verifier=verify_r2lab_authority,
            allocation_runner=foundation_runner,
            owner=owner,
            allocation_id=allocation_id,
            timeout_seconds=timeout_seconds,
        )
    except RuntimeError as exc:
        raise R2LabPhysicalFoundationError(
            "current physical authority was not proven"
        ) from exc

    ready_nodes = _ready_node_count(
        _json_object(
            _checked(
                foundation_runner,
                _ssh(known_hosts, "kubectl", "get", "nodes", "-o", "json"),
                timeout_seconds=timeout_seconds,
                label="Kubernetes node readiness query",
            ),
            "Kubernetes node readiness query",
        )
    )
    for network_function in sorted(REQUIRED_OPEN5GS_NFS):
        label = f"Open5GS {network_function.upper()} readiness query"
        _require_ready_open5gs_pod(
            _json_object(
                _checked(
                    foundation_runner,
                    _ssh(
                        known_hosts,
                        "kubectl",
                        "get",
                        "pods",
                        "-n",
                        NAMESPACE,
                        "-l",
                        f"app=open5gs,nf={network_function}",
                        "-o",
                        "json",
                    ),
                    timeout_seconds=timeout_seconds,
                    label=label,
                ),
                label,
            ),
            network_function,
        )
    ready_open5gs_pods = len(REQUIRED_OPEN5GS_NFS)

    try:
        handoff = execute_physical_namespace_handoff(
            from_run_id=previous_run_id,
            to_run_id=run_id,
            known_hosts=known_hosts,
            runner=foundation_runner,
            authority_verifier=authority.verify,
            timeout_seconds=timeout_seconds,
        )
    except R2LabPhysicalHandoffError as exc:
        raise R2LabPhysicalFoundationError(
            "physical namespace ownership was not proven"
        ) from exc

    try:
        authority.verify()
    except RuntimeError as exc:
        raise R2LabPhysicalFoundationError(
            "physical authority changed during foundation verification"
        ) from exc
    observed_owner = _checked(
        foundation_runner,
        _ssh(
            known_hosts,
            "kubectl",
            "get",
            "namespace",
            NAMESPACE,
            "-o",
            "jsonpath={.metadata.labels.synthran\\.run/id}",
        ),
        timeout_seconds=timeout_seconds,
        label="Open5GS namespace ownership verification",
    ).strip()
    if observed_owner != run_id:
        raise R2LabPhysicalFoundationError(
            "Open5GS namespace ownership changed during foundation verification"
        )

    try:
        authority.verify()
    except RuntimeError as exc:
        raise R2LabPhysicalFoundationError(
            "physical authority changed during foundation verification"
        ) from exc

    evidence_path = run_root.resolve() / run_id / "physical-run.json"
    try:
        evidence = _foundation_evidence(run_id)
        if evidence_path.exists():
            existing = PhysicalRunEvidence.read_json(evidence_path)
            if existing != evidence:
                raise R2LabPhysicalFoundationError(
                    "physical run evidence already contains different state"
                )
        else:
            evidence.write_json(evidence_path)
    except R2LabAcceptanceError as exc:
        raise R2LabPhysicalFoundationError(
            "physical foundation evidence could not be persisted safely"
        ) from exc

    return PhysicalFoundationResult(
        run_id=run_id,
        previous_run_id=previous_run_id,
        handoff=handoff,
        ready_node_count=ready_nodes,
        ready_open5gs_pod_count=ready_open5gs_pods,
        evidence_path=evidence_path,
    )
