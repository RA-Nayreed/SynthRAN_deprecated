from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import time
from typing import TextIO

from synthran.live_preflight import Runner, subprocess_runner
from synthran.network.resources import SUPPORTED_NODES
from synthran.network.runtime import atomic_json, run_command
from synthran.r2lab.acceptance import (
    AcceptanceOutcome,
    PhysicalAcceptance,
    PhysicalAcceptanceStage,
    PhysicalRunEvidence,
)
from synthran.r2lab.foundation_topology import (
    REQUIRED_PHYSICAL_NETWORK_ATTACHMENTS,
    R2LabTopologyFoundationError,
    _handoff_namespace,
    _namespace_owner,
    _open5gs_ready,
    _physical_networks_ready,
    _ready_nodes,
    reconcile_open5gs_topology,
)
from synthran.r2lab.resources import load_topology, verify_physical_authority
from synthran.r2lab.ue import (
    R2LabPhysicalUeError,
    prove_physical_user_plane,
    verify_current_n3xx_n2,
)
from synthran.r2lab.ue_activation import activate_physical_ue
from synthran.r2lab.upstream_roles import (
    R2LabUpstreamRoleError,
    converge_kubernetes_foundation,
    converge_physical_gnb,
)


RESUME_SCHEMA = "synthran/r2lab-live-resume/v1alpha1"
KUBERNETES_OBSERVATION_ATTEMPTS = 3
KUBERNETES_OBSERVATION_INTERVAL_SECONDS = 5.0


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


def _observe_ready_nodes(
    *,
    topology,
    known_hosts: Path,
    cluster_runner: Runner,
    timeout: int,
) -> bool:
    for attempt in range(1, KUBERNETES_OBSERVATION_ATTEMPTS + 1):
        try:
            _ready_nodes(
                topology=topology,
                known_hosts=known_hosts,
                runner=cluster_runner,
                timeout_seconds=min(timeout, 300),
            )
            return True
        except R2LabTopologyFoundationError:
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
    r2lab_runner: Runner,
    cluster_runner: Runner,
    timeout: int,
    progress: TextIO | None,
) -> bool:
    topology = load_topology(run_root=run_root, run_id=run_id).validate()
    verify_physical_authority(
        run_id=run_id,
        slice_name=slice_name,
        run_root=run_root,
        runner=r2lab_runner,
        timeout_seconds=min(timeout, 300),
    )

    kubernetes_reconciled = False
    if not _observe_ready_nodes(
        topology=topology,
        known_hosts=known_hosts,
        cluster_runner=cluster_runner,
        timeout=timeout,
    ):
        _report(
            progress,
            "resume-foundation: Kubernetes unavailable after bounded observation; converging pinned upstream foundation",
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
            known_hosts=known_hosts,
            cluster_runner=cluster_runner,
            timeout=timeout,
        ):
            raise R2LabLiveReconciliationError(
                "Kubernetes foundation remained unavailable after upstream convergence"
            )
        kubernetes_reconciled = True
        _report(progress, "resume-foundation: Kubernetes foundation converged")

    current_owner = _namespace_owner(
        topology=topology,
        known_hosts=known_hosts,
        runner=cluster_runner,
        timeout_seconds=min(timeout, 60),
    )
    if current_owner not in {None, run_id}:
        raise R2LabLiveReconciliationError("current Open5GS namespace belongs to another run")

    healthy = False
    networks_ready = False
    ready_networks: tuple[str, ...] = ()
    try:
        healthy, _ = _open5gs_ready(
            topology=topology,
            known_hosts=known_hosts,
            runner=cluster_runner,
            timeout_seconds=min(timeout, 300),
        )
        networks_ready, ready_networks = _physical_networks_ready(
            topology=topology,
            known_hosts=known_hosts,
            runner=cluster_runner,
            timeout_seconds=min(timeout, 300),
        )
    except R2LabTopologyFoundationError:
        pass

    open5gs_reconciled = not (healthy and networks_ready)
    if open5gs_reconciled:
        _report(progress, "resume-foundation: converging Open5GS through pinned upstream roles")

        def authority() -> object:
            return verify_physical_authority(
                run_id=run_id,
                slice_name=slice_name,
                run_root=run_root,
                runner=r2lab_runner,
                timeout_seconds=min(timeout, 300),
            )

        reconcile_open5gs_topology(
            run_id=run_id,
            slice_name=slice_name,
            topology=topology,
            known_hosts=known_hosts,
            authority_verifier=authority,
            lock_path=lock_path,
            dependency_root=deps_root,
            run_root=run_root,
            repository_root=Path("."),
            runner=run_command,
            timeout_seconds=timeout,
            progress=progress,
        )
        healthy, _ = _open5gs_ready(
            topology=topology,
            known_hosts=known_hosts,
            runner=cluster_runner,
            timeout_seconds=min(timeout, 300),
        )
        networks_ready, ready_networks = _physical_networks_ready(
            topology=topology,
            known_hosts=known_hosts,
            runner=cluster_runner,
            timeout_seconds=min(timeout, 300),
        )

    current_owner = _namespace_owner(
        topology=topology,
        known_hosts=known_hosts,
        runner=cluster_runner,
        timeout_seconds=min(timeout, 60),
    )
    ownership_changed = False
    if current_owner != run_id:
        ownership_changed = _handoff_namespace(
            run_id=run_id,
            previous_run_id=None,
            slice_name=slice_name,
            topology=topology,
            run_root=run_root,
            known_hosts=known_hosts,
            r2lab_runner=r2lab_runner,
            cluster_runner=cluster_runner,
            timeout_seconds=min(timeout, 300),
        )
    if not healthy:
        raise R2LabLiveReconciliationError("current Open5GS AMF/SMF/UPF set is not ready")
    if not networks_ready:
        missing = ", ".join(
            name
            for name in REQUIRED_PHYSICAL_NETWORK_ATTACHMENTS
            if name not in ready_networks
        )
        raise R2LabLiveReconciliationError(
            f"current physical Multus networks are missing: {missing}"
        )
    if (
        _namespace_owner(
            topology=topology,
            known_hosts=known_hosts,
            runner=cluster_runner,
            timeout_seconds=min(timeout, 60),
        )
        != run_id
    ):
        raise R2LabLiveReconciliationError("current Open5GS namespace ownership is not proven")
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
        if verify_current_n3xx_n2(
            run_id=run_id,
            known_hosts=known_hosts,
            run_root=run_root,
            runner=cluster_runner,
            timeout_seconds=min(timeout, 60),
        ):
            _report(progress, "resume-gNB/N2: current path already proven")
            return False
    except R2LabPhysicalUeError:
        pass

    _report(
        progress,
        "resume-gNB/N2: converging physical gNB through pinned 5g-Ansible roles",
    )
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
            proven = verify_current_n3xx_n2(
                run_id=run_id,
                known_hosts=known_hosts,
                run_root=run_root,
                runner=cluster_runner,
                timeout_seconds=min(timeout, 60),
            )
        except R2LabPhysicalUeError:
            proven = False
        consecutive = consecutive + 1 if proven else 0
        if consecutive >= n2_attempts:
            _report(
                progress,
                f"resume-gNB/N2: stable N2 proven ({attempt} observations)",
            )
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


def _ue_path(
    *,
    evidence: PhysicalRunEvidence,
    slice_name: str,
    owner: str,
    allocation_id: str | None,
    known_hosts: Path,
    lock_path: Path,
    deps_root: Path,
    run_root: Path,
    r2lab_runner: Runner,
    cluster_runner: Runner,
    timeout: int,
    progress: TextIO | None,
) -> tuple[str, bool]:
    state, activation = activate_physical_ue(
        evidence=_synthetic_gnb_boundary(evidence),
        slice_name=slice_name,
        owner=owner,
        allocation_id=allocation_id,
        known_hosts=known_hosts,
        lock_path=lock_path,
        deps_root=deps_root,
        run_root=run_root,
        r2lab_runner=r2lab_runner,
        cluster_runner=cluster_runner,
        timeout_seconds=min(timeout, 300),
        progress=progress,
    )
    if state.acceptance.next_stage is not PhysicalAcceptanceStage.USER_PLANE:
        raise R2LabLiveReconciliationError("current UE registration/PDU path was not re-established")
    topology = load_topology(run_root=run_root, run_id=evidence.run_id).validate()
    proof = prove_physical_user_plane(
        evidence=state,
        slice_name=slice_name,
        owner=owner,
        allocation_id=allocation_id,
        known_hosts=known_hosts,
        peer=SUPPORTED_NODES[topology.ran_node].ip,
        run_root=run_root,
        r2lab_runner=r2lab_runner,
        cluster_runner=cluster_runner,
        timeout_seconds=min(timeout, 300),
    )
    if not proof.probe.proven:
        raise R2LabLiveReconciliationError("current physical user plane was not re-proven")
    _report(progress, "resume-UE path: registration, PDU and user plane re-proven")
    return activation.status, True


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
    if (
        evidence.acceptance.outcome_for(PhysicalAcceptanceStage.OPEN5GS)
        is not AcceptanceOutcome.PASSED
    ):
        raise R2LabLiveReconciliationError(
            "live resume requires previously accepted Open5GS history"
        )

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
        r2lab_runner=r2lab_runner,
        cluster_runner=cluster_runner,
        timeout=timeout_seconds,
        progress=progress,
    )
    gnb_restarted = False
    if (
        evidence.acceptance.outcome_for(PhysicalAcceptanceStage.GNB_N2)
        is AcceptanceOutcome.PASSED
    ):
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
    if (
        evidence.acceptance.outcome_for(PhysicalAcceptanceStage.USER_PLANE)
        is AcceptanceOutcome.PASSED
    ):
        ue_status, user_plane_proven = _ue_path(
            evidence=evidence,
            slice_name=slice_name,
            owner=owner,
            allocation_id=allocation_id,
            known_hosts=known_hosts,
            lock_path=lock_path,
            deps_root=deps_root,
            run_root=run_root,
            r2lab_runner=r2lab_runner,
            cluster_runner=cluster_runner,
            timeout=timeout_seconds,
            progress=progress,
        )

    resume_path = run_root / run_id / "physical" / "live-resume.json"
    atomic_json(
        resume_path,
        {
            "schema": RESUME_SCHEMA,
            "run_id": run_id,
            "observed_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "historical_acceptance_unchanged": True,
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
