"""Read-only path proofs shared by Amber controlled measurements."""

from __future__ import annotations

from typing import Any, Callable

from synthran.dependencies import DependencyLock
from synthran.experiment.observe import discover_rfsim_ue_pod
from synthran.fiveg_ansible import NetworkInventory
from synthran.network.runtime import verify_network_path
from synthran.research import ResearchError, ResearchExperimentSpec


def require_network_ready(
    *,
    inventory: NetworkInventory,
    lock: DependencyLock,
    network_run_id: str,
    ue_pod: str,
    pdu_address: str,
) -> Any:
    """Re-prove the accepted network and exact runtime UE/PDU identity."""

    report = verify_network_path(
        inventory=inventory,
        lock=lock,
        run_id=network_run_id,
        timeout_seconds=120,
    )
    if not report.ready:
        failing = [
            f"{check.name}: {check.detail}"
            for check in report.checks
            if not check.passed
        ]
        detail = "; ".join(failing) if failing else "network verification failed"
        raise ResearchError(
            "controlled measurement requires a currently path-proven network: "
            + detail
        )
    current_ue = discover_rfsim_ue_pod(inventory, network_run_id)
    if current_ue != ue_pod:
        raise ResearchError(
            "controlled measurement UE pod changed after runtime handoff"
        )
    current_pdu = getattr(report, "pdu_address", None)
    if not isinstance(current_pdu, str) or current_pdu != pdu_address:
        raise ResearchError(
            "controlled measurement UE PDU changed after runtime handoff"
        )
    return report


def prove_pre_window_target(
    *,
    spec: ResearchExperimentSpec,
    prove_icmp: Callable[[], None],
    prove_transport: Callable[[], None],
) -> None:
    """Prove readiness with the transport that defines the condition."""

    if spec.load.enabled:
        prove_transport()
        return
    prove_icmp()
