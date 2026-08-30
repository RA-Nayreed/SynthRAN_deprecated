"""Canonical live boundary immediately before the physical workload."""

from __future__ import annotations

from pathlib import Path

from synthran.r2lab.acceptance import PhysicalAcceptanceStage, PhysicalRunEvidence
from synthran.r2lab.live_cluster import R2LabLiveClusterError, verify_n2
from synthran.r2lab.resources import verify_physical_authority
from synthran.r2lab.ue import (
    PhysicalWorkloadContext,
    PhysicalWorkloadExecutor,
    PhysicalWorkloadResult,
    R2LabPhysicalUeError,
    Runner,
    UE_INTERFACE,
    _write_json,
)


def execute_physical_workload_handoff(
    *,
    evidence: PhysicalRunEvidence,
    slice_name: str,
    owner: str,
    allocation_id: str | None,
    known_hosts: Path,
    run_root: Path,
    r2lab_runner: Runner,
    cluster_runner: Runner,
    executor: PhysicalWorkloadExecutor,
    evidence_path: Path | None = None,
    workload_evidence_path: Path | None = None,
    timeout_seconds: int = 30,
) -> tuple[PhysicalRunEvidence, PhysicalWorkloadResult | None]:
    """Re-prove current authority/N2 with the canonical observer, then run the workload."""

    if evidence.acceptance.next_stage is not PhysicalAcceptanceStage.WORKLOAD:
        raise R2LabPhysicalUeError("physical workload is not the next lifecycle boundary")

    known_hosts = known_hosts.expanduser().resolve()
    if not known_hosts.is_file():
        raise R2LabPhysicalUeError("strict SLICES known-hosts file is missing")

    authority = verify_physical_authority(
        run_id=evidence.run_id,
        slice_name=slice_name,
        run_root=run_root,
        runner=r2lab_runner,
        timeout_seconds=min(timeout_seconds, 300),
    )
    topology = authority.topology.validate()

    try:
        n2_proven = verify_n2(
            run_id=evidence.run_id,
            run_root=run_root,
            runner=cluster_runner,
            timeout_seconds=min(timeout_seconds, 60),
        )
    except R2LabLiveClusterError as exc:
        raise R2LabPhysicalUeError(str(exc)) from exc
    if not n2_proven:
        raise R2LabPhysicalUeError("current singleton gNB/N2 path is not proven")

    context = PhysicalWorkloadContext(
        run_id=evidence.run_id,
        ue=topology.ue,
        interface=UE_INTERFACE,
    )
    result = executor(context)
    if result.run_id != evidence.run_id:
        raise R2LabPhysicalUeError("physical workload result belongs to another physical run")

    accepted = result.accepted and result.cleanup_proven
    source = (
        f"physical-iot:{result.workload_id}:accepted"
        if accepted
        else f"physical-iot:{result.workload_id}:not-proven"
    )
    state = (
        evidence.pass_stage(PhysicalAcceptanceStage.WORKLOAD, source=source)
        if accepted
        else evidence.fail_stage(PhysicalAcceptanceStage.WORKLOAD, source=source)
    )
    if evidence_path is not None:
        state.write_json(evidence_path)
    if workload_evidence_path is not None:
        _write_json(workload_evidence_path, result.to_dict())
    return state, result
