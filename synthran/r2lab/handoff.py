"""Guarded ownership handoff for an already-running physical Open5GS foundation.

This module exists for the physical R2Lab path where a follow-up run reuses the
same prepared Open5GS namespace but must not inherit a stale SynthRAN run owner.
The handoff is intentionally narrow: it never deploys workloads, never touches
R2Lab power state, and never starts or scales the physical gNB.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import shlex
from typing import Callable, Sequence

from synthran.live_preflight import CommandResult
from synthran.network.runtime import validate_run_id
from synthran.r2lab.deployment import (
    CORE_NODE,
    DEPLOYMENT_RUN_LABEL,
    GNB_SELECTOR,
    NAMESPACE,
    RELEASE,
)


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
            "status": "namespace-handed-off"
            if self.changed
            else "namespace-already-owned",
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


def _parse_existing_deployment(
    text: str, *, allowed_owners: set[str]
) -> tuple[bool, int | None, str | None]:
    if not text.strip():
        return False, None, None
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise R2LabPhysicalHandoffError(
            "existing physical gNB Deployment is not JSON"
        ) from exc
    if not isinstance(payload, dict):
        raise R2LabPhysicalHandoffError("existing physical gNB Deployment is malformed")
    metadata = payload.get("metadata")
    spec = payload.get("spec")
    if not isinstance(metadata, dict) or not isinstance(spec, dict):
        raise R2LabPhysicalHandoffError(
            "existing physical gNB Deployment is incomplete"
        )
    labels = metadata.get("labels")
    desired = spec.get("replicas")
    if not isinstance(labels, dict):
        raise R2LabPhysicalHandoffError(
            "existing physical gNB Deployment ownership is missing"
        )
    deployment_owner = labels.get(DEPLOYMENT_RUN_LABEL)
    if deployment_owner not in allowed_owners:
        raise R2LabPhysicalHandoffError(
            "existing physical gNB Deployment has an unexpected run owner"
        )
    if not isinstance(desired, int) or isinstance(desired, bool):
        raise R2LabPhysicalHandoffError(
            "existing physical gNB replica state is malformed"
        )
    if desired != 0:
        raise R2LabPhysicalHandoffError(
            "existing physical gNB is not stopped; ownership handoff requires replicas=0"
        )
    return True, desired, deployment_owner


def _parse_pod_count(text: str) -> int:
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise R2LabPhysicalHandoffError(
            "physical gNB pod query did not return JSON"
        ) from exc
    items = payload.get("items") if isinstance(payload, dict) else None
    if not isinstance(items, list):
        raise R2LabPhysicalHandoffError(
            "physical gNB pod query returned malformed JSON"
        )
    return len(items)


def execute_physical_namespace_handoff(
    *,
    from_run_id: str,
    to_run_id: str,
    known_hosts: Path,
    runner: Runner,
    authority_verifier: Callable[[], object],
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
    if timeout_seconds < 30 or timeout_seconds > 600:
        raise R2LabPhysicalHandoffError(
            "handoff timeout must be between 30 and 600 seconds"
        )

    known_hosts = known_hosts.expanduser().resolve()
    if not known_hosts.is_file():
        raise R2LabPhysicalHandoffError("strict SLICES known-hosts file is missing")

    try:
        authority_verifier()
    except Exception as exc:
        raise R2LabPhysicalHandoffError(
            "fresh physical authority was not proven"
        ) from exc

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
    deployment_present, desired, deployment_owner = _parse_existing_deployment(
        existing, allowed_owners={from_run_id, to_run_id}
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

    namespace_changed = namespace_owner != to_run_id
    deployment_changed = deployment_present and deployment_owner != to_run_id
    if not namespace_changed and not deployment_changed:
        return PhysicalNamespaceHandoffResult(
            from_run_id=from_run_id,
            to_run_id=to_run_id,
            changed=False,
            deployment_present=deployment_present,
            desired_replicas=desired,
            gnb_pod_count=pod_count,
        )

    try:
        authority_verifier()
    except Exception as exc:
        raise R2LabPhysicalHandoffError(
            "physical authority changed before namespace ownership handoff"
        ) from exc

    if deployment_changed:
        _checked(
            runner=runner,
            command=_ssh(
                known_hosts,
                "kubectl",
                "label",
                f"deployment/{RELEASE}",
                "-n",
                NAMESPACE,
                f"{DEPLOYMENT_RUN_LABEL}={to_run_id}",
                "--overwrite",
            ),
            timeout_seconds=min(timeout_seconds, 60),
            label="physical gNB Deployment ownership handoff",
        )

    if namespace_changed:
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

    if deployment_present:
        observed_deployment_owner = _checked(
            runner=runner,
            command=_ssh(
                known_hosts,
                "kubectl",
                "get",
                f"deployment/{RELEASE}",
                "-n",
                NAMESPACE,
                "-o",
                "jsonpath={.metadata.labels.synthran\\.run/id}",
            ),
            timeout_seconds=min(timeout_seconds, 60),
            label="physical gNB Deployment ownership verification",
        ).stdout.strip()
        if observed_deployment_owner != to_run_id:
            raise R2LabPhysicalHandoffError(
                "physical gNB Deployment ownership handoff was not independently observed"
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
