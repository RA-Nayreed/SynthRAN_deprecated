"""Pure lifecycle derivation and fail-closed reconciliation planning."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from synthran.workspace.desired import ExperimentDesiredState
from synthran.workspace.model import WorkspaceError, utc_now
from synthran.workspace.observed import Observation, ObservedState


LIFECYCLE_STATES = frozenset(
    {
        "CONFIGURED",
        "RESERVED",
        "ALLOCATED",
        "PREPARED",
        "NETWORK_READY",
        "PATH_PROVEN",
        "EXPERIMENT_RUNNING",
        "RECOVERY_REQUIRED",
        "BLOCKED",
    }
)
RISK_CLASSES = frozenset({"R0", "R1", "R2", "R3"})


@dataclass(frozen=True)
class ReconciliationStep:
    name: str
    risk: str
    reason: str
    mutates: bool

    def __post_init__(self) -> None:
        if self.risk not in RISK_CLASSES:
            raise WorkspaceError("unsupported reconciliation risk class")
        if not self.name or len(self.name) > 64:
            raise WorkspaceError("reconciliation step name is malformed")
        if not self.reason or len(self.reason) > 512:
            raise WorkspaceError("reconciliation step reason is malformed")
        if self.mutates and self.risk not in {"R2", "R3"}:
            raise WorkspaceError("mutating reconciliation step must require approval")


@dataclass(frozen=True)
class ReconciliationReport:
    lifecycle: str
    steps: tuple[ReconciliationStep, ...] = ()
    blocks: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.lifecycle not in LIFECYCLE_STATES:
            raise WorkspaceError("unsupported lifecycle state")
        if any(not item or len(item) > 512 for item in self.blocks):
            raise WorkspaceError("reconciliation block reason is malformed")

def _observation(state: ObservedState, dimension: str) -> Observation | None:
    return state.get(dimension)


def _current_ready(
    state: ObservedState,
    dimension: str,
    *,
    now: datetime,
) -> bool:
    item = _observation(state, dimension)
    return item is not None and item.state == "ready" and item.is_fresh(now)


def _required_network_dimensions(desired: ExperimentDesiredState) -> tuple[str, ...]:
    required: list[str] = ["kubernetes"]
    if desired.core.enabled:
        required.extend(("core", "upf"))
    if desired.ran.enabled:
        required.append("ran")
    if desired.ue.enabled:
        required.extend(("ue", "pdu"))
    required.append("radio")
    return tuple(required)


def _has_current_failure(
    state: ObservedState,
    dimensions: tuple[str, ...],
    *,
    now: datetime,
) -> bool:
    for dimension in dimensions:
        item = state.get(dimension)
        if item is not None and item.is_fresh(now) and item.state in {
            "degraded",
            "failed",
        }:
            return True
    return False


def _experiment_running(state: ObservedState, *, now: datetime) -> bool:
    item = state.get("experiment")
    if item is None or item.state != "ready" or not item.is_fresh(now):
        return False
    return item.facts.get("running") is True


def derive_lifecycle(
    desired: ExperimentDesiredState,
    observed: ObservedState,
    *,
    now: datetime | None = None,
) -> str:
    """Derive current lifecycle only from requested state and current observations."""

    current = (now or utc_now()).astimezone(timezone.utc)
    critical = (
        "controller",
        "project_access",
        "provider_experiment",
        "reservation",
        "allocation",
    ) + _required_network_dimensions(desired)
    for dimension in critical:
        item = observed.get(dimension)
        if item is not None and item.is_fresh(current) and item.state == "blocked":
            return "BLOCKED"
    if _has_current_failure(observed, critical, now=current):
        return "RECOVERY_REQUIRED"
    if _experiment_running(observed, now=current):
        return "EXPERIMENT_RUNNING"
    if _current_ready(observed, "path", now=current):
        return "PATH_PROVEN"
    if all(
        _current_ready(observed, dimension, now=current)
        for dimension in _required_network_dimensions(desired)
    ):
        return "NETWORK_READY"
    if _current_ready(observed, "preparation", now=current):
        return "PREPARED"
    if _current_ready(observed, "allocation", now=current):
        return "ALLOCATED"
    if _current_ready(observed, "reservation", now=current):
        return "RESERVED"
    return "CONFIGURED"


def _ownership_block(item: Observation, label: str) -> str | None:
    if item.ownership == "other":
        return f"{label} is owned by another operator"
    if item.ownership == "unknown":
        return f"{label} ownership is unknown"
    return None


def _require_current_control_fact(
    observed: ObservedState,
    dimension: str,
    label: str,
    *,
    now: datetime,
    steps: list[ReconciliationStep],
    blocks: list[str],
) -> Observation | None:
    item = observed.get(dimension)
    if item is None or not item.is_fresh(now) or item.state == "unknown":
        steps.append(
            ReconciliationStep(
                name=f"inspect-{dimension.replace('_', '-')}",
                risk="R0",
                reason=f"current {label} state is not known",
                mutates=False,
            )
        )
        return None
    block = _ownership_block(item, label)
    if block is not None and item.state != "absent":
        blocks.append(block)
    return item


def _report(
    desired: ExperimentDesiredState,
    observed: ObservedState,
    *,
    now: datetime,
    steps: list[ReconciliationStep],
    blocks: list[str],
) -> ReconciliationReport:
    lifecycle = "BLOCKED" if blocks else derive_lifecycle(desired, observed, now=now)
    return ReconciliationReport(lifecycle, tuple(steps), tuple(blocks))


def plan_reconciliation(
    desired: ExperimentDesiredState,
    observed: ObservedState,
    *,
    provider_experiment_required: bool = True,
    now: datetime | None = None,
) -> ReconciliationReport:
    """Describe only the next safe reconciliation boundary without executing it."""

    current = (now or utc_now()).astimezone(timezone.utc)
    steps: list[ReconciliationStep] = []
    blocks: list[str] = []

    for dimension, label in (
        ("controller", "controller"),
        ("project_access", "project access"),
    ):
        item = observed.get(dimension)
        if item is None or not item.is_fresh(current) or item.state == "unknown":
            steps.append(
                ReconciliationStep(
                    name=f"inspect-{dimension.replace('_', '-')}",
                    risk="R0",
                    reason=f"current {label} has not been verified",
                    mutates=False,
                )
            )
        elif item.state != "ready":
            blocks.append(f"{label} is not ready")

    if provider_experiment_required:
        provider = observed.get("provider_experiment")
        if provider is None or not provider.is_fresh(current) or provider.state == "unknown":
            steps.append(
                ReconciliationStep(
                    name="inspect-provider-experiment",
                    risk="R0",
                    reason="temporary provider experiment must be reverified",
                    mutates=False,
                )
            )
        elif provider.state != "ready":
            blocks.append("temporary provider experiment is not active")

    if blocks or steps:
        return _report(
            desired, observed, now=current, steps=steps, blocks=blocks
        )

    reservation = _require_current_control_fact(
        observed,
        "reservation",
        "reservation",
        now=current,
        steps=steps,
        blocks=blocks,
    )
    if reservation is None:
        return _report(
            desired, observed, now=current, steps=steps, blocks=blocks
        )
    if reservation.state == "absent":
        steps.append(
            ReconciliationStep(
                name="reserve",
                risk="R2",
                reason="no current reservation covers the requested testbed",
                mutates=True,
            )
        )
        return _report(
            desired, observed, now=current, steps=steps, blocks=blocks
        )
    if reservation.state != "ready":
        blocks.append("reservation is not usable")
        return _report(
            desired, observed, now=current, steps=steps, blocks=blocks
        )
    if blocks:
        return _report(
            desired, observed, now=current, steps=steps, blocks=blocks
        )

    allocation = _require_current_control_fact(
        observed,
        "allocation",
        "allocation",
        now=current,
        steps=steps,
        blocks=blocks,
    )
    if allocation is None:
        return _report(
            desired, observed, now=current, steps=steps, blocks=blocks
        )
    if blocks:
        return _report(
            desired, observed, now=current, steps=steps, blocks=blocks
        )
    if allocation.state == "absent":
        steps.append(
            ReconciliationStep(
                name="allocate",
                risk="R2",
                reason="required compute resources are not allocated",
                mutates=True,
            )
        )
        return _report(
            desired, observed, now=current, steps=steps, blocks=blocks
        )
    if allocation.state in {"degraded", "failed"}:
        if allocation.ownership == "synthran":
            steps.append(
                ReconciliationStep(
                    name="recover-allocation",
                    risk="R2",
                    reason="SynthRAN-owned allocation is incomplete",
                    mutates=True,
                )
            )
        else:
            blocks.append("incomplete allocation is not SynthRAN-owned")
        return _report(
            desired, observed, now=current, steps=steps, blocks=blocks
        )
    if allocation.state != "ready":
        blocks.append("allocation is not usable")
        return _report(
            desired, observed, now=current, steps=steps, blocks=blocks
        )

    if desired.radio.mode == "physical" or desired.radio.backend == "r2lab":
        lease = _require_current_control_fact(
            observed,
            "r2lab_lease",
            "R2Lab lease",
            now=current,
            steps=steps,
            blocks=blocks,
        )
        if lease is None:
            return _report(
                desired, observed, now=current, steps=steps, blocks=blocks
            )
        if blocks:
            return _report(
                desired, observed, now=current, steps=steps, blocks=blocks
            )
        if lease.state == "absent":
            steps.append(
                ReconciliationStep(
                    name="obtain-r2lab-lease",
                    risk="R0",
                    reason="physical radio operation requires an active R2Lab lease",
                    mutates=False,
                )
            )
            return _report(
                desired, observed, now=current, steps=steps, blocks=blocks
            )
        if lease.state != "ready":
            blocks.append("R2Lab lease is not usable")
            return _report(
                desired, observed, now=current, steps=steps, blocks=blocks
            )

    preparation = observed.get("preparation")
    if preparation is None or not preparation.is_fresh(current) or preparation.state == "unknown":
        steps.append(
            ReconciliationStep(
                name="inspect-preparation",
                risk="R0",
                reason="current preparation state is not known",
                mutates=False,
            )
        )
        return _report(
            desired, observed, now=current, steps=steps, blocks=blocks
        )
    if preparation.state == "absent":
        steps.append(
            ReconciliationStep(
                name="prepare",
                risk="R2",
                reason="allocated resources are not prepared for the requested network",
                mutates=True,
            )
        )
        return _report(
            desired, observed, now=current, steps=steps, blocks=blocks
        )
    if preparation.state in {"degraded", "failed"}:
        blocks.append("resource preparation requires recovery")
        return _report(
            desired, observed, now=current, steps=steps, blocks=blocks
        )
    if preparation.state != "ready":
        blocks.append("resource preparation is not usable")
        return _report(
            desired, observed, now=current, steps=steps, blocks=blocks
        )

    required_network = _required_network_dimensions(desired)
    unknown_network = [
        dimension
        for dimension in required_network
        if (
            observed.get(dimension) is None
            or not observed.get(dimension).is_fresh(current)  # type: ignore[union-attr]
            or observed.get(dimension).state == "unknown"  # type: ignore[union-attr]
        )
    ]
    if unknown_network:
        steps.append(
            ReconciliationStep(
                name="inspect-network",
                risk="R0",
                reason="current network runtime state is incomplete",
                mutates=False,
            )
        )
        return _report(
            desired, observed, now=current, steps=steps, blocks=blocks
        )

    failed_network = [
        dimension
        for dimension in required_network
        if observed.state(dimension) in {"degraded", "failed", "blocked"}
    ]
    if failed_network:
        blocks.append("network runtime requires recovery: " + ", ".join(failed_network))
        return _report(
            desired, observed, now=current, steps=steps, blocks=blocks
        )
    if any(observed.state(dimension) == "absent" for dimension in required_network):
        steps.append(
            ReconciliationStep(
                name="up",
                risk="R2",
                reason="requested network components are not all running",
                mutates=True,
            )
        )
        return _report(
            desired, observed, now=current, steps=steps, blocks=blocks
        )

    if not _current_ready(observed, "path", now=current):
        steps.append(
            ReconciliationStep(
                name="verify-path",
                risk="R1",
                reason="network is ready but end-to-end path has not been proven currently",
                mutates=False,
            )
        )

    return _report(
        desired, observed, now=current, steps=steps, blocks=blocks
    )
