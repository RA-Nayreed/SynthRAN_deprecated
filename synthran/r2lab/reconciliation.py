from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import time
from typing import TextIO

from synthran.live_preflight import Runner, subprocess_runner
from synthran.network.resources import SUPPORTED_NODES
from synthran.network.runtime import atomic_json
from synthran.r2lab.acceptance import (
    AcceptanceOutcome,
    PhysicalAcceptance,
    PhysicalAcceptanceStage,
    PhysicalRunEvidence,
)
from synthran.r2lab.foundation_convergence import (
    converge_kubernetes_foundation,
    converge_open5gs,
)
from synthran.r2lab.live_cluster import (
    REQUIRED_PHYSICAL_NETWORK_ATTACHMENTS,
    R2LabLiveClusterError,
    bind_namespace_owner,
    namespace_owner,
    open5gs_ready,
    physical_networks_ready,
    prove_user_plane,
    ready_nodes,
    verify_n2,
)
from synthran.r2lab.resources import load_topology
from synthran.r2lab.ue import R2LabPhysicalUeError
from synthran.r2lab.ue_activation import observe_functional_ue_runtime
from synthran.r2lab.ue_ansible import R2LabUeAnsibleError, execute_selected_ue_role
from synthran.r2lab.upstream_roles import R2LabUpstreamRoleError, converge_physical_gnb
from synthran.r2lab.workload_retry import (
    R2LabWorkloadRetryError,
    recover_failed_workload,
)


RESUME_SCHEMA = "synthran/r2lab-live-resume/v1alpha1"
KUBERNETES_OBSERVATION_ATTEMPTS = 3
KUBERNETES_OBSERVATION_INTERVAL_SECONDS = 5.0
UE_POSTCONDITION_SECONDS = 45
UE_POLL_SECONDS = 2.0


class R2LabLiveReconciliationError(RuntimeError):
    pass


@dataclass(frozen=True)
class LiveResumeResult:
    run_id: str
    allocation_id: str | None
    foundation_reconciled: bool
    gnb_restarted: bool
    ue_status: str | None
    user_plane_proven: bool
    evidence_path: Path

    def to_dict(self) -> dict[str, object]:
        return {
            "schema": RESUME_SCHEMA,
            "run_id": self.run_id,
            "allocation_id": self.allocation_id,
            "foundation_reconciled": self.foundation_reconciled,
            "gnb_restarted": self.gnb_restarted,
            "ue_status": self.ue_status,
            "user_plane_proven": self.user_plane_proven,
            "evidence_path": str(self.evidence_path),
        }


def _report(progress: TextIO | None, message: str) -> None:
    if progress is not None:
        print(f"[synthran] {message}", file=progress, flush=True)


def _observe_ready_nodes(*, topology, cluster_runner: Runner, timeout: int) -> bool:
    for attempt in range(1, KUBERNETES_OBSERVATION_ATTEMPTS + 1):
        try:
            ready_nodes(
                topology=topology,
                runner=cluster_runner,
                timeout_seconds=min(timeout, 300),
            )
            return True
        except R2LabLiveClusterError:
            if attempt < KUBERNETES_OBSERVATION_ATTEMPTS:
                time.sleep(KUBERNETES_OBSERVATION_INTERVAL_SECONDS)
    return False


def _foundation(
    *,
    run_id: str,
    slice_name: str,
    owner: str,
    allocation_id: str | None,
    known_hosts: Path,
    lock_path: Path,
    deps_root: Path,
    run_root: Path,
    cluster_runner: Runner,
    timeout: int,
    progress: TextIO | None,
) -> bool:
    topology = load_topology(run_root=run_root, run_id=run_id).validate()
    kubernetes_reconciled = False

    if _observe_ready_nodes(
        topology=topology,
        cluster_runner=cluster_runner,
        timeout=timeout,
    ):
        _report(progress, "resume-foundation: existing Kubernetes foundation is Ready; reusing sopnodes")
    else:
        _report(
            progress,
            "resume-foundation: Kubernetes unavailable; reconciling software before any POS fallback",
        )
        try:
            converge_kubernetes_foundation(
                run_id=run_id,
                slice_name=slice_name,
                owner=owner,
                allocation_id=allocation_id,
                known_hosts=known_hosts,
                lock_path=lock_path,
                deps_root=deps_root,
                run_root=run_root,
                timeout_seconds=timeout,
                progress=progress,
            )
        except R2LabUpstreamRoleError as exc:
            raise R2LabLiveReconciliationError(str(exc)) from exc
        if not _observe_ready_nodes(
            topology=topology,
            cluster_runner=cluster_runner,
            timeout=timeout,
        ):
            raise R2LabLiveReconciliationError(
                "Kubernetes foundation remained unavailable after upstream convergence"
            )
        kubernetes_reconciled = True
        _report(progress, "resume-foundation: Kubernetes foundation converged")

    try:
        current_owner = namespace_owner(
            topology=topology,
            runner=cluster_runner,
            timeout_seconds=min(timeout, 60),
        )
    except R2LabLiveClusterError as exc:
        raise R2LabLiveReconciliationError(str(exc)) from exc
    if current_owner not in {None, run_id}:
        raise R2LabLiveReconciliationError("current Open5GS namespace belongs to another run")

    healthy = False
    networks_ready = False
    ready_networks: tuple[str, ...] = ()
    try:
        healthy, _ = open5gs_ready(
            topology=topology,
            runner=cluster_runner,
            timeout_seconds=min(timeout, 300),
        )
        networks_ready, ready_networks = physical_networks_ready(
            topology=topology,
            runner=cluster_runner,
            timeout_seconds=min(timeout, 300),
        )
    except R2LabLiveClusterError:
        pass

    open5gs_reconciled = not (healthy and networks_ready)
    if open5gs_reconciled:
        _report(progress, "resume-foundation: converging Open5GS through pinned upstream roles")
        try:
            converge_open5gs(
                run_id=run_id,
                slice_name=slice_name,
                owner=owner,
                allocation_id=allocation_id,
                known_hosts=known_hosts,
                lock_path=lock_path,
                deps_root=deps_root,
                run_root=run_root,
                timeout_seconds=timeout,
                progress=progress,
            )
        except R2LabUpstreamRoleError as exc:
            raise R2LabLiveReconciliationError(str(exc)) from exc
        try:
            healthy, _ = open5gs_ready(
                topology=topology,
                runner=cluster_runner,
                timeout_seconds=min(timeout, 300),
            )
            networks_ready, ready_networks = physical_networks_ready(
                topology=topology,
                runner=cluster_runner,
                timeout_seconds=min(timeout, 300),
            )
        except R2LabLiveClusterError as exc:
            raise R2LabLiveReconciliationError(str(exc)) from exc

    try:
        ownership_changed = bind_namespace_owner(
            run_id=run_id,
            topology=topology,
            runner=cluster_runner,
            timeout_seconds=min(timeout, 60),
        )
    except R2LabLiveClusterError as exc:
        raise R2LabLiveReconciliationError(str(exc)) from exc

    if not healthy:
        raise R2LabLiveReconciliationError("current Open5GS AMF/SMF/UPF set is not ready")
    if not networks_ready:
        missing = ", ".join(
            name for name in REQUIRED_PHYSICAL_NETWORK_ATTACHMENTS if name not in ready_networks
        )
        raise R2LabLiveReconciliationError(
            f"current physical Multus networks are missing: {missing}"
        )
    try:
        if namespace_owner(
            topology=topology,
            runner=cluster_runner,
            timeout_seconds=min(timeout, 60),
        ) != run_id:
            raise R2LabLiveReconciliationError("current Open5GS namespace ownership is not proven")
    except R2LabLiveClusterError as exc:
        raise R2LabLiveReconciliationError(str(exc)) from exc

    _report(progress, "resume-foundation: current Kubernetes/Open5GS foundation proven")
    return bool(kubernetes_reconciled or open5gs_reconciled or ownership_changed)


def _n2(
    *,
    run_id: str,
    slice_name: str,
    owner: str,
    allocation_id: str | None,
    known_hosts: Path,
    lock_path: Path,
    deps_root: Path,
    run_root: Path,
    cluster_runner: Runner,
    timeout: int,
    n2_attempts: int,
    n2_convergence_attempts: int,
    n2_interval: float,
    progress: TextIO | None,
) -> bool:
    total = n2_attempts + n2_convergence_attempts - 1
    if n2_attempts < 1 or n2_convergence_attempts < 1 or total > 120:
        raise R2LabLiveReconciliationError("N2 resume proof counts are invalid")

    try:
        if verify_n2(
            run_id=run_id,
            run_root=run_root,
            runner=cluster_runner,
            timeout_seconds=min(timeout, 60),
        ):
            _report(progress, "resume-gNB/N2: current path already proven")
            return False
    except R2LabLiveClusterError:
        pass

    _report(progress, "resume-gNB/N2: converging physical gNB through pinned 5g-Ansible roles")
    try:
        converge_physical_gnb(
            run_id=run_id,
            slice_name=slice_name,
            owner=owner,
            allocation_id=allocation_id,
            known_hosts=known_hosts,
            lock_path=lock_path,
            deps_root=deps_root,
            run_root=run_root,
            timeout_seconds=timeout,
            progress=progress,
        )
    except R2LabUpstreamRoleError as exc:
        raise R2LabLiveReconciliationError(str(exc)) from exc

    consecutive = 0
    for attempt in range(1, total + 1):
        try:
            proven = verify_n2(
                run_id=run_id,
                run_root=run_root,
                runner=cluster_runner,
                timeout_seconds=min(timeout, 60),
            )
        except R2LabLiveClusterError:
            proven = False
        consecutive = consecutive + 1 if proven else 0
        if consecutive >= n2_attempts:
            _report(progress, f"resume-gNB/N2: stable N2 proven ({attempt} observations)")
            return True
        if attempt < total:
            time.sleep(n2_interval)
    raise R2LabLiveReconciliationError("stable current gNB/N2 proof was not re-established")


def _synthetic_gnb_boundary(evidence: PhysicalRunEvidence) -> PhysicalRunEvidence:
    if evidence.staged is None or evidence.gnb_start is None:
        raise R2LabLiveReconciliationError("accepted UE history is missing immutable gNB evidence")
    prefix = evidence.acceptance.evidence[:5]
    if (
        len(prefix) != 5
        or prefix[-1].stage is not PhysicalAcceptanceStage.GNB_N2
        or any(item.outcome is not AcceptanceOutcome.PASSED for item in prefix)
    ):
        raise R2LabLiveReconciliationError("accepted UE history has no valid gNB/N2 boundary")
    return PhysicalRunEvidence(
        run_id=evidence.run_id,
        staged=evidence.staged,
        gnb_start=evidence.gnb_start,
        acceptance=PhysicalAcceptance(evidence=prefix),
    )


def _pass_current_ue_path(state: PhysicalRunEvidence, ue: str) -> PhysicalRunEvidence:
    source = f"current-{ue}:wwan0:open5gs-upf"
    for stage, detail in (
        (PhysicalAcceptanceStage.CELL_ACQUISITION, "nr-sa-functional"),
        (PhysicalAcceptanceStage.REGISTRATION, "core-path-registered"),
        (PhysicalAcceptanceStage.PDU_SESSION, "ipv4-upf-reachable"),
    ):
        if state.acceptance.next_stage is stage:
            state = state.pass_stage(stage, source=f"{source}:{detail}")
    return state


def _ue_path(
    *,
    evidence: PhysicalRunEvidence,
    slice_name: str,
    known_hosts: Path,
    lock_path: Path,
    deps_root: Path,
    run_root: Path,
    r2lab_runner: Runner,
    cluster_runner: Runner,
    timeout: int,
    progress: TextIO | None,
) -> tuple[str, bool]:
    del known_hosts
    topology = load_topology(run_root=run_root, run_id=evidence.run_id).validate()
    try:
        if not verify_n2(
            run_id=evidence.run_id,
            run_root=run_root,
            runner=cluster_runner,
            timeout_seconds=min(timeout, 60),
        ):
            raise R2LabLiveReconciliationError("current singleton gNB/N2 path is not proven")
    except R2LabLiveClusterError as exc:
        raise R2LabLiveReconciliationError(str(exc)) from exc

    state = _synthetic_gnb_boundary(evidence)
    if state.acceptance.next_stage is PhysicalAcceptanceStage.UE_MANAGEMENT:
        state = state.pass_stage(
            PhysicalAcceptanceStage.UE_MANAGEMENT,
            source=f"current-management:{topology.ue}:{topology.ue_profile.mode}",
        )

    runtime = observe_functional_ue_runtime(
        run_id=evidence.run_id,
        slice_name=slice_name,
        run_root=run_root,
        runner=r2lab_runner,
    )
    status = "already-ready"
    if not runtime.pdu_session_established:
        try:
            execute_selected_ue_role(
                run_id=evidence.run_id,
                slice_name=slice_name,
                topology=topology,
                action="connect",
                lock_path=lock_path,
                deps_root=deps_root,
                run_root=run_root,
                timeout_seconds=min(timeout, 180),
                progress=progress,
            )
        except R2LabUeAnsibleError as exc:
            raise R2LabLiveReconciliationError(str(exc)) from exc
        deadline = time.monotonic() + min(UE_POSTCONDITION_SECONDS, max(10, int(timeout)))
        while True:
            runtime = observe_functional_ue_runtime(
                run_id=evidence.run_id,
                slice_name=slice_name,
                run_root=run_root,
                runner=r2lab_runner,
            )
            if runtime.pdu_session_established or time.monotonic() >= deadline:
                break
            time.sleep(UE_POLL_SECONDS)
        status = "activated"

    if not runtime.pdu_session_established:
        raise R2LabLiveReconciliationError("current UE registration/PDU path was not re-established")
    state = _pass_current_ue_path(state, topology.ue)
    if state.acceptance.next_stage is not PhysicalAcceptanceStage.USER_PLANE:
        raise R2LabLiveReconciliationError("current UE registration/PDU path was not re-established")

    try:
        proof = prove_user_plane(
            evidence=state,
            slice_name=slice_name,
            peer=SUPPORTED_NODES[topology.ran_node].ip,
            run_root=run_root,
            r2lab_runner=r2lab_runner,
            cluster_runner=cluster_runner,
            timeout_seconds=min(timeout, 300),
        )
    except (R2LabPhysicalUeError, R2LabLiveClusterError) as exc:
        raise R2LabLiveReconciliationError(str(exc)) from exc
    if not proof.probe.proven:
        raise R2LabLiveReconciliationError("current physical user plane was not re-proven")
    _report(progress, "resume-UE path: registration, PDU and user plane re-proven")
    return status, True


def reconcile_live_resume(
    *,
    run_id: str,
    slice_name: str,
    owner: str,
    allocation_id: str | None,
    known_hosts: Path,
    lock_path: Path,
    deps_root: Path,
    run_root: Path,
    r2lab_runner: Runner = subprocess_runner,
    cluster_runner: Runner = subprocess_runner,
    timeout_seconds: int = 1800,
    n2_attempts: int = 12,
    n2_convergence_attempts: int = 12,
    n2_interval: float = 5.0,
    progress: TextIO | None = None,
) -> LiveResumeResult:
    run_root = run_root.expanduser().resolve()
    known_hosts = known_hosts.expanduser().resolve()
    evidence_path = run_root / run_id / "physical-run.json"
    evidence = PhysicalRunEvidence.read_json(evidence_path)
    if evidence.acceptance.accepted:
        raise R2LabLiveReconciliationError("accepted physical run does not require live resume")
    if evidence.acceptance.outcome_for(PhysicalAcceptanceStage.OPEN5GS) is not AcceptanceOutcome.PASSED:
        raise R2LabLiveReconciliationError("live resume requires previously accepted Open5GS history")

    _report(progress, "resume: reconciling current state behind historical acceptance")
    foundation_reconciled = _foundation(
        run_id=run_id,
        slice_name=slice_name,
        owner=owner,
        allocation_id=allocation_id,
        known_hosts=known_hosts,
        lock_path=lock_path,
        deps_root=deps_root,
        run_root=run_root,
        cluster_runner=cluster_runner,
        timeout=timeout_seconds,
        progress=progress,
    )

    gnb_restarted = False
    if evidence.acceptance.outcome_for(PhysicalAcceptanceStage.GNB_N2) is AcceptanceOutcome.PASSED:
        gnb_restarted = _n2(
            run_id=run_id,
            slice_name=slice_name,
            owner=owner,
            allocation_id=allocation_id,
            known_hosts=known_hosts,
            lock_path=lock_path,
            deps_root=deps_root,
            run_root=run_root,
            cluster_runner=cluster_runner,
            timeout=timeout_seconds,
            n2_attempts=n2_attempts,
            n2_convergence_attempts=n2_convergence_attempts,
            n2_interval=n2_interval,
            progress=progress,
        )

    ue_status = None
    user_plane_proven = False
    if evidence.acceptance.outcome_for(PhysicalAcceptanceStage.USER_PLANE) is AcceptanceOutcome.PASSED:
        ue_status, user_plane_proven = _ue_path(
            evidence=evidence,
            slice_name=slice_name,
            known_hosts=known_hosts,
            lock_path=lock_path,
            deps_root=deps_root,
            run_root=run_root,
            r2lab_runner=r2lab_runner,
            cluster_runner=cluster_runner,
            timeout=timeout_seconds,
            progress=progress,
        )

    try:
        evidence, workload_failure_recovered = recover_failed_workload(
            evidence=evidence,
            run_root=run_root,
        )
    except R2LabWorkloadRetryError as exc:
        raise R2LabLiveReconciliationError(str(exc)) from exc
    if workload_failure_recovered:
        _report(
            progress,
            "resume-workload: previous failed attempt cleanup proven; reopening workload boundary",
        )

    resume_path = run_root / run_id / "physical" / "live-resume.json"
    atomic_json(
        resume_path,
        {
            "schema": RESUME_SCHEMA,
            "run_id": run_id,
            "observed_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "historical_acceptance_unchanged": not workload_failure_recovered,
            "workload_failure_recovered": workload_failure_recovered,
            "foundation_reconciled": foundation_reconciled,
            "gnb_restarted": gnb_restarted,
            "ue_status": ue_status,
            "user_plane_proven": user_plane_proven,
        },
    )
    _report(progress, "resume: current live prerequisites re-proven")
    return LiveResumeResult(
        run_id=run_id,
        allocation_id=allocation_id,
        foundation_reconciled=foundation_reconciled,
        gnb_restarted=gnb_restarted,
        ue_status=ue_status,
        user_plane_proven=user_plane_proven,
        evidence_path=resume_path,
    )