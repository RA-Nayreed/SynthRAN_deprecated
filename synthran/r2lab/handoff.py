"""Guarded ownership handoff for an already-running physical Open5GS foundation.

This module exists for the physical R2Lab path where a follow-up run reuses the
same prepared Open5GS namespace but must not inherit a stale SynthRAN run owner.
The handoff is intentionally narrow: it never deploys workloads, never touches
R2Lab power state, and never starts or scales the physical gNB.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import json
from pathlib import Path
import re
import shlex
from typing import Callable, Sequence

from synthran.live_preflight import (
    CommandResult,
    verify_allocations,
    verify_reservation,
)
from synthran.network.runtime import validate_run_id
from synthran.r2lab.deployment import (
    CORE_NODE,
    DEPLOYMENT_RUN_LABEL,
    GNB_SELECTOR,
    NAMESPACE,
    RAN_NODE,
    RELEASE,
)


_SAFE_AUTHORITY_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
Runner = Callable[[Sequence[str], int], CommandResult]


class R2LabPhysicalHandoffError(RuntimeError):
    """Raised when physical namespace ownership cannot be transferred safely."""


@dataclass(frozen=True)
class PhysicalNamespaceHandoffResult:
    from_run_id: str
    to_run_id: str
    changed: bool
    deployment_present: bool
    desired_replicas: int | None
    gnb_pod_count: int

    def to_dict(self) -> dict[str, object]:
        return {
            "from_run_id": self.from_run_id,
            "to_run_id": self.to_run_id,
            "changed": self.changed,
            "deployment_present": self.deployment_present,
            "desired_replicas": self.desired_replicas,
            "gnb_pod_count": self.gnb_pod_count,
            "status": "namespace-handed-off" if self.changed else "namespace-already-owned",
            "hardware_mutation": False,
        }


def _validate_run(value: str, label: str) -> str:
    try:
        validated = validate_run_id(value)
    except Exception as exc:
        raise R2LabPhysicalHandoffError(f"{label}: {exc}") from exc
    if validated != value:
        raise R2LabPhysicalHandoffError(f"{label} is not canonical")
    return value


def _validate_authority(value: str, label: str) -> str:
    if not _SAFE_AUTHORITY_RE.fullmatch(value):
        raise R2LabPhysicalHandoffError(f"{label} contains unsafe characters")
    return value


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
    *, runner: Runner, command: Sequence[str], timeout_seconds: int, label: str
) -> CommandResult:
    try:
        result = runner(command, timeout_seconds)
    except Exception as exc:
        raise R2LabPhysicalHandoffError(f"{label} could not be observed") from exc
    if result.returncode != 0:
        raise R2LabPhysicalHandoffError(f"{label} returned nonzero")
    return result


def _parse_existing_deployment(text: str, *, expected_owner: str) -> tuple[bool, int | None]:
    if not text.strip():
        return False, None
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise R2LabPhysicalHandoffError("existing physical gNB Deployment is not JSON") from exc
    if not isinstance(payload, dict):
        raise R2LabPhysicalHandoffError("existing physical gNB Deployment is malformed")
    metadata = payload.get("metadata")
    spec = payload.get("spec")
    if not isinstance(metadata, dict) or not isinstance(spec, dict):
        raise R2LabPhysicalHandoffError("existing physical gNB Deployment is incomplete")
    labels = metadata.get("labels")
    desired = spec.get("replicas")
    if not isinstance(labels, dict):
        raise R2LabPhysicalHandoffError("existing physical gNB Deployment ownership is missing")
    if labels.get(DEPLOYMENT_RUN_LABEL) != expected_owner:
        raise R2LabPhysicalHandoffError(
            "existing physical gNB Deployment is not owned by the expected previous run"
        )
    if not isinstance(desired, int) or isinstance(desired, bool):
        raise R2LabPhysicalHandoffError("existing physical gNB replica state is malformed")
    if desired != 0:
        raise R2LabPhysicalHandoffError(
            "existing physical gNB is not stopped; ownership handoff requires replicas=0"
        )
    return True, desired


def _parse_pod_count(text: str) -> int:
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise R2LabPhysicalHandoffError("physical gNB pod query did not return JSON") from exc
    items = payload.get("items") if isinstance(payload, dict) else None
    if not isinstance(items, list):
        raise R2LabPhysicalHandoffError("physical gNB pod query returned malformed JSON")
    return len(items)


def execute_physical_namespace_handoff(
    *,
    from_run_id: str,
    to_run_id: str,
    owner: str,
    reservation_id: str,
    allocation_id: str,
    known_hosts: Path,
    now: datetime,
    runner: Runner,
    timeout_seconds: int = 120,
) -> PhysicalNamespaceHandoffResult:
    """Transfer only the Open5GS namespace owner after proving a stopped gNB.

    The operation is retry-safe. If the namespace already belongs to ``to_run_id``
    it performs all read-only safety checks and returns without another mutation.
    """

    from_run_id = _validate_run(from_run_id, "previous run ID")
    to_run_id = _validate_run(to_run_id, "new run ID")
    if from_run_id == to_run_id:
        raise R2LabPhysicalHandoffError("previous and new run IDs must differ")
    owner = _validate_authority(owner, "owner")
    reservation_id = _validate_authority(reservation_id, "reservation ID")
    allocation_id = _validate_authority(allocation_id, "allocation ID")
    if now.tzinfo is None:
        raise R2LabPhysicalHandoffError("handoff time must be timezone-aware")
    if timeout_seconds < 30 or timeout_seconds > 600:
        raise R2LabPhysicalHandoffError("handoff timeout must be between 30 and 600 seconds")

    known_hosts = known_hosts.expanduser().resolve()
    if not known_hosts.is_file():
        raise R2LabPhysicalHandoffError("strict SLICES known-hosts file is missing")

    try:
        verify_reservation(
            runner=runner,
            reservation_id=reservation_id,
            owner=owner,
            nodes={CORE_NODE, RAN_NODE},
            now=now,
            timeout_seconds=min(timeout_seconds, 60),
        )
        verify_allocations(
            runner=runner,
            allocation_id=allocation_id,
            owner=owner,
            nodes={CORE_NODE, RAN_NODE},
            timeout_seconds=min(timeout_seconds, 60),
        )
    except Exception as exc:
        raise R2LabPhysicalHandoffError("fresh SLICES authority was not proven") from exc

    namespace_owner = _checked(
        runner=runner,
        command=_ssh(
            known_hosts,
            "kubectl",
            "get",
            "namespace",
            NAMESPACE,
            "-o",
            "jsonpath={.metadata.labels.synthran\\.run/id}",
        ),
        timeout_seconds=min(timeout_seconds, 60),
        label="Open5GS namespace ownership query",
    ).stdout.strip()

    if namespace_owner not in {from_run_id, to_run_id}:
        raise R2LabPhysicalHandoffError(
            "Open5GS namespace is owned by neither the expected previous run nor the new run"
        )

    expected_deployment_owner = (
        to_run_id if namespace_owner == to_run_id else from_run_id
    )
    existing = _checked(
        runner=runner,
        command=_ssh(
            known_hosts,
            "kubectl",
            "get",
            f"deployment/{RELEASE}",
            "-n",
            NAMESPACE,
            "--ignore-not-found",
            "-o",
            "json",
        ),
        timeout_seconds=min(timeout_seconds, 60),
        label="existing physical gNB Deployment query",
    ).stdout
    deployment_present, desired = _parse_existing_deployment(
        existing, expected_owner=expected_deployment_owner
    )

    pod_count = _parse_pod_count(
        _checked(
            runner=runner,
            command=_ssh(
                known_hosts,
                "kubectl",
                "get",
                "pods",
                "-n",
                NAMESPACE,
                "-l",
                GNB_SELECTOR,
                "-o",
                "json",
            ),
            timeout_seconds=min(timeout_seconds, 60),
            label="existing physical gNB pod query",
        ).stdout
    )
    if pod_count != 0:
        raise R2LabPhysicalHandoffError(
            "existing physical gNB pods remain; ownership handoff requires zero pods"
        )

    if namespace_owner == to_run_id:
        return PhysicalNamespaceHandoffResult(
            from_run_id=from_run_id,
            to_run_id=to_run_id,
            changed=False,
            deployment_present=deployment_present,
            desired_replicas=desired,
            gnb_pod_count=pod_count,
        )

    # Re-prove both reservation and allocation immediately before the only write.
    try:
        verify_reservation(
            runner=runner,
            reservation_id=reservation_id,
            owner=owner,
            nodes={CORE_NODE, RAN_NODE},
            now=now,
            timeout_seconds=min(timeout_seconds, 60),
        )
        verify_allocations(
            runner=runner,
            allocation_id=allocation_id,
            owner=owner,
            nodes={CORE_NODE, RAN_NODE},
            timeout_seconds=min(timeout_seconds, 60),
        )
    except Exception as exc:
        raise R2LabPhysicalHandoffError(
            "SLICES authority changed before namespace ownership handoff"
        ) from exc

    _checked(
        runner=runner,
        command=_ssh(
            known_hosts,
            "kubectl",
            "label",
            "namespace",
            NAMESPACE,
            f"{DEPLOYMENT_RUN_LABEL}={to_run_id}",
            "--overwrite",
        ),
        timeout_seconds=min(timeout_seconds, 60),
        label="Open5GS namespace ownership handoff",
    )

    observed_owner = _checked(
        runner=runner,
        command=_ssh(
            known_hosts,
            "kubectl",
            "get",
            "namespace",
            NAMESPACE,
            "-o",
            "jsonpath={.metadata.labels.synthran\\.run/id}",
        ),
        timeout_seconds=min(timeout_seconds, 60),
        label="Open5GS namespace ownership verification",
    ).stdout.strip()
    if observed_owner != to_run_id:
        raise R2LabPhysicalHandoffError(
            "Open5GS namespace ownership handoff was not independently observed"
        )

    return PhysicalNamespaceHandoffResult(
        from_run_id=from_run_id,
        to_run_id=to_run_id,
        changed=True,
        deployment_present=deployment_present,
        desired_replicas=desired,
        gnb_pod_count=pod_count,
    )
