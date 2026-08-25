"""Safe access to immutable operation plans, approvals, and execution."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping

from synthran.app.controller import (
    ApplicationController,
    RESOURCE_BOUND_MUTATIONS,
    RESOURCE_DECISION_INPUT,
)
from synthran.app.workflows import plan_workflow, workflow_targets
from synthran.control.live_operations import execute_live_operation
from synthran.operations import load_approval, load_plan, load_state, select_reconciliation_step
from synthran.operations.model import OperationPlan
from synthran.resources.model import ResourceInventory
from synthran.workspace.desired_store import load_desired_state
from synthran.workspace.model import WorkspaceError, utc_now, validate_operation_id
from synthran.workspace.observed import ObservedState
from synthran.workspace.observed_store import load_observed_state, observed_state_path
from synthran.workspace.reconciliation import ReconciliationStep, plan_reconciliation


SUPPORTED_ACTIONS = frozenset({"reserve", "up", "verify", "recover", "down"})
RESOURCE_ACTIONS = RESOURCE_BOUND_MUTATIONS | frozenset({"recover-allocation"})


class OperationInputError(ValueError):
    """Validated operation input that cannot be applied."""


def _action(params: Mapping[str, object]) -> str:
    if set(params) != {"action"}:
        raise OperationInputError("operation action requires only action")
    value = params.get("action")
    if not isinstance(value, str) or value not in SUPPORTED_ACTIONS:
        raise OperationInputError("operation action is unsupported")
    return value


def _operation_id(
    params: Mapping[str, object],
    *,
    allow_mode: bool = False,
) -> tuple[str, str | None]:
    allowed = {"operation_id", "mode"} if allow_mode else {"operation_id"}
    if set(params) != allowed:
        raise OperationInputError("operation request contains unsupported fields")
    value = params.get("operation_id")
    if not isinstance(value, str):
        raise OperationInputError("operation ID must be text")
    try:
        operation_id = validate_operation_id(value)
    except WorkspaceError as exc:
        raise OperationInputError(str(exc)) from exc
    mode = params.get("mode") if allow_mode else None
    if allow_mode and mode not in {"standard", "destructive"}:
        raise OperationInputError("approval mode must be standard or destructive")
    return operation_id, mode if isinstance(mode, str) else None


def _active_state(
    controller: ApplicationController,
    *,
    now: datetime,
) -> tuple[object, object, ObservedState]:
    controller.reload_authority()
    record = controller.authority.active_experiment
    if record is None:
        raise WorkspaceError("workspace has no active experiment")
    desired = load_desired_state(controller.root, record.experiment_id)
    path = observed_state_path(controller.root, record.experiment_id)
    if not path.is_file():
        raise WorkspaceError("active experiment has no observed-state snapshot")
    observed = load_observed_state(controller.root, record.experiment_id)
    return record, desired, observed


def _selected_step(
    controller: ApplicationController,
    action: str,
    *,
    now: datetime,
) -> tuple[ReconciliationStep, tuple[str, ...]]:
    _, desired, observed = _active_state(controller, now=now)

    if action == "down":
        report = plan_workflow(desired, observed, "down", now=now)
        if report.blocks:
            raise WorkspaceError("; ".join(report.blocks))
        step = select_reconciliation_step(report, "down")
        return step, workflow_targets(observed, "down", now=now)

    reconciliation = plan_reconciliation(desired, observed, now=now)
    if reconciliation.blocks:
        raise WorkspaceError("; ".join(reconciliation.blocks))

    if action == "reserve":
        step_name = "reserve"
    elif action == "verify":
        step_name = "verify-path"
    elif action == "recover":
        candidates = tuple(
            step.name
            for step in reconciliation.steps
            if step.name.startswith("recover-")
        )
        if len(candidates) != 1:
            raise WorkspaceError(
                "no single SynthRAN-owned recovery action is currently available"
            )
        step_name = candidates[0]
    else:
        if not reconciliation.steps:
            raise WorkspaceError("network already has no pending action")
        if len(reconciliation.steps) != 1:
            raise WorkspaceError(
                "current state has multiple read-only actions; inspect resources and network first"
            )
        step_name = reconciliation.steps[0].name
        if step_name == "verify-path":
            raise WorkspaceError(
                "network is ready; choose Verify to prove the end-to-end path"
            )
        if step_name not in {"reserve", "allocate", "prepare", "up"}:
            raise WorkspaceError(
                f"bring-up cannot perform current action {step_name}; inspect current state first"
            )

    step = select_reconciliation_step(reconciliation, step_name)
    return step, ()


def _resource_decision_view(
    controller: ApplicationController,
    step: ReconciliationStep,
    inventory: ResourceInventory | None,
    *,
    now: datetime,
) -> tuple[tuple[str, ...], str | None]:
    if step.name not in RESOURCE_ACTIONS:
        return (), None
    if inventory is None:
        return (), "Fresh provider inventory is required before this action can be prepared."
    try:
        decision = controller.resource_decision(inventory, now=now)
    except WorkspaceError as exc:
        return (), str(exc)
    return decision.targets, None


def _step_view(
    controller: ApplicationController,
    action: str,
    step: ReconciliationStep,
    targets: tuple[str, ...],
    inventory: ResourceInventory | None,
    *,
    now: datetime,
) -> dict[str, object]:
    resource_targets, resource_block = _resource_decision_view(
        controller,
        step,
        inventory,
        now=now,
    )
    effective_targets = targets or resource_targets
    return {
        "action": action,
        "kind": step.name,
        "risk": step.risk,
        "mutates": step.mutates,
        "reason": step.reason,
        "approval_required": step.risk in {"R2", "R3"},
        "approval_mode": (
            "destructive"
            if step.risk == "R3"
            else "standard"
            if step.risk == "R2"
            else None
        ),
        "targets": list(effective_targets),
        "can_plan": resource_block is None,
        "plan_block": resource_block,
    }


def inspect_operation_action(
    *,
    start: Path | None,
    environment: Mapping[str, str],
    params: Mapping[str, object],
    inventory: ResourceInventory | None = None,
    now: datetime | None = None,
) -> dict[str, object]:
    """Describe the exact current action without writing operation state."""

    action = _action(params)
    current = (now or utc_now()).astimezone(timezone.utc)
    controller = ApplicationController(start=start, environment=environment)
    step, targets = _selected_step(controller, action, now=current)
    return _step_view(
        controller,
        action,
        step,
        targets,
        inventory,
        now=current,
    )


def _plan_view(plan: OperationPlan) -> dict[str, object]:
    return {
        "operation_id": plan.operation_id,
        "experiment_id": plan.experiment_id,
        "kind": plan.kind,
        "risk": plan.risk,
        "mutates": plan.mutates,
        "reason": plan.reason,
        "approval_required": plan.approval_required,
        "approval_mode": (
            "destructive"
            if plan.risk == "R3"
            else "standard"
            if plan.approval_required
            else None
        ),
        "targets": list(plan.targets),
        "created_at_utc": plan.created_at_utc,
    }


def read_operation(
    *,
    start: Path | None,
    environment: Mapping[str, str],
    params: Mapping[str, object],
) -> dict[str, object]:
    operation_id, _ = _operation_id(params)
    controller = ApplicationController(start=start, environment=environment)
    plan = load_plan(controller.root, operation_id)
    state = load_state(controller.root, operation_id)
    approval = load_approval(controller.root, operation_id)
    events = controller.operation_events(operation_id)
    return {
        "plan": _plan_view(plan),
        "state": state.to_dict(),
        "approval": approval.to_dict() if approval is not None else None,
        "events": [event.to_dict() for event in events],
    }


def _begin_recovery_operation(
    controller: ApplicationController,
    *,
    step: ReconciliationStep,
    inventory: ResourceInventory,
    now: datetime,
) -> OperationPlan:
    record, desired, observed = _active_state(controller, now=now)
    if getattr(record, "slices_experiment", None) is None:
        raise WorkspaceError(
            "active experiment has no provider experiment binding; bind one before live control"
        )
    decision = controller.resource_decision(inventory, now=now)
    reconciliation = plan_reconciliation(desired, observed, now=now)
    return controller.operations.begin(
        desired=desired,
        observed=observed,
        step_name=step.name,
        targets=decision.targets,
        bound_inputs={RESOURCE_DECISION_INPUT: decision.to_dict()},
        policy_report=reconciliation,
        now=now,
    )


def plan_operation(
    *,
    start: Path | None,
    environment: Mapping[str, str],
    params: Mapping[str, object],
    inventory: ResourceInventory | None = None,
    now: datetime | None = None,
) -> dict[str, object]:
    """Persist one exact plan only when all immutable inputs are available."""

    action = _action(params)
    current = (now or utc_now()).astimezone(timezone.utc)
    controller = ApplicationController(start=start, environment=environment)
    step, _ = _selected_step(controller, action, now=current)

    if step.name in RESOURCE_ACTIONS and inventory is None:
        raise WorkspaceError(
            "fresh provider inventory is required before this resource action can be planned"
        )
    if action == "down":
        plan = controller.begin_workflow_operation("down", now=current)
    elif step.name == "recover-allocation":
        assert inventory is not None
        plan = _begin_recovery_operation(
            controller,
            step=step,
            inventory=inventory,
            now=current,
        )
    else:
        plan = controller.begin_operation(
            step_name=step.name,
            inventory=inventory,
            now=current,
        )
    return read_operation(
        start=start,
        environment=environment,
        params={"operation_id": plan.operation_id},
    )


def approve_operation(
    *,
    start: Path | None,
    environment: Mapping[str, str],
    params: Mapping[str, object],
    now: datetime | None = None,
) -> dict[str, object]:
    operation_id, mode = _operation_id(params, allow_mode=True)
    assert mode is not None
    controller = ApplicationController(start=start, environment=environment)
    plan = load_plan(controller.root, operation_id)
    expected = "destructive" if plan.risk == "R3" else "standard"
    if not plan.approval_required:
        raise WorkspaceError("this operation does not require approval")
    if mode != expected:
        raise WorkspaceError(f"{plan.risk} operation requires {expected} approval")
    controller.approve_operation(
        operation_id,
        destructive=mode == "destructive",
        now=now,
    )
    return read_operation(
        start=start,
        environment=environment,
        params={"operation_id": operation_id},
    )


def execute_operation(
    *,
    start: Path | None,
    environment: Mapping[str, str],
    params: Mapping[str, object],
    runner,
    now: datetime | None = None,
) -> dict[str, object]:
    operation_id, _ = _operation_id(params)
    execute_live_operation(
        start=start,
        environment=environment,
        operation_id=operation_id,
        runner=runner,
        now=now,
    )
    return read_operation(
        start=start,
        environment=environment,
        params={"operation_id": operation_id},
    )


def cancel_operation(
    *,
    start: Path | None,
    environment: Mapping[str, str],
    params: Mapping[str, object],
    now: datetime | None = None,
) -> dict[str, object]:
    operation_id, _ = _operation_id(params)
    controller = ApplicationController(start=start, environment=environment)
    state = load_state(controller.root, operation_id)
    if state.status == "running":
        raise WorkspaceError(
            "running provider work cannot be cancelled until executor cancellation is connected"
        )
    if state.status in {"completed", "failed", "recovery-required"}:
        raise WorkspaceError("operation is already closed")
    controller.interrupt_operation(operation_id, now=now)
    return read_operation(
        start=start,
        environment=environment,
        params={"operation_id": operation_id},
    )
