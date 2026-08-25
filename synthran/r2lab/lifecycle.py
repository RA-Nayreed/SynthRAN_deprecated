"""High-level R2Lab lifecycle composition behind the common backend boundary.

This module resumes an already prepared physical run from persisted acceptance
state, but live authority and path state are re-observed at each mutation or
traffic boundary. Digests from lower-level deployment evidence remain
provenance; they are not re-checked here as runtime authorization.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import tempfile
from typing import Mapping

from synthran.dependencies import load_lock
from synthran.experiment.r2lab import (
    DEFAULT_PHYSICAL_RUN_ROOT,
    PhysicalExperimentConfig,
    build_physical_workload_executor,
)
from synthran.fiveg_ansible import load_inventory
from synthran.live_preflight import subprocess_runner as cluster_subprocess_runner
from synthran.privacy import repository_root
from synthran.r2lab.acceptance import (
    PhysicalAcceptanceStage,
    PhysicalRunEvidence,
    R2LabAcceptanceError,
)
from synthran.r2lab.controller import subprocess_runner as r2lab_subprocess_runner
from synthran.r2lab.ue import (
    R2LabQfitActivationError,
    execute_authorized_qfit_activation,
    execute_authorized_qfit_user_plane,
    execute_physical_workload_handoff,
)


DEFAULT_R2LAB_RUN_ROOT = Path(".synthran/r2lab")


class R2LabPhysicalLifecycleError(RuntimeError):
    """Raised when the composed physical lifecycle cannot advance safely."""


def _paths(run_root: Path, run_id: str) -> tuple[Path, Path]:
    directory = run_root.expanduser().resolve() / run_id
    return directory / "physical-run.json", directory / "physical"


def _write_json(path: Path, payload: Mapping[str, object]) -> Path:
    path = path.expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    content = json.dumps(dict(payload), indent=2, sort_keys=True) + "\n"
    temporary_path: Path | None = None
    try:
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
    except OSError as exc:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
        raise R2LabPhysicalLifecycleError(
            "physical lifecycle evidence could not be persisted"
        ) from exc
    return path


def _stage_name(evidence: PhysicalRunEvidence) -> str | None:
    stage = evidence.acceptance.next_stage
    return stage.value if stage is not None else None


def _failed_stage_name(evidence: PhysicalRunEvidence) -> str | None:
    stage = evidence.acceptance.failed_stage
    return stage.value if stage is not None else None


@dataclass(frozen=True)
class PhysicalPathSummary:
    run_id: str
    evidence_path: Path
    next_stage: str | None
    failed_stage: str | None
    activation_status: str | None
    user_plane_proven: bool

    @property
    def ready_for_workload(self) -> bool:
        return self.failed_stage is None and self.next_stage == PhysicalAcceptanceStage.WORKLOAD.value

    def to_dict(self) -> dict[str, object]:
        return {
            "schema": "synthran/r2lab-path/v1alpha1",
            "run_id": self.run_id,
            "next_stage": self.next_stage,
            "failed_stage": self.failed_stage,
            "activation_status": self.activation_status,
            "user_plane_proven": self.user_plane_proven,
            "ready_for_workload": self.ready_for_workload,
            "evidence_path": str(self.evidence_path),
        }


@dataclass(frozen=True)
class PhysicalWorkloadSummary:
    run_id: str
    workload_id: str
    evidence_path: Path
    workload_result_path: Path
    accepted: bool
    cleanup_proven: bool

    def to_dict(self) -> dict[str, object]:
        return {
            "schema": "synthran/r2lab-workload/v1alpha1",
            "run_id": self.run_id,
            "workload_id": self.workload_id,
            "backend": "r2lab",
            "stages": {
                "workload": self.accepted,
                "data": self.accepted,
                "acceptance": self.accepted,
                "cleanup": self.cleanup_proven,
            },
            "accepted": self.accepted,
            "cleanup_proven": self.cleanup_proven,
            "evidence_path": str(self.evidence_path),
            "workload_result_path": str(self.workload_result_path),
        }


def continue_physical_path(
    *,
    run_id: str,
    slice_name: str,
    known_hosts: Path,
    peer: str,
    run_root: Path = DEFAULT_R2LAB_RUN_ROOT,
    timeout_seconds: int = 30,
    r2lab_runner=r2lab_subprocess_runner,
    cluster_runner=cluster_subprocess_runner,
) -> PhysicalPathSummary:
    """Advance a proven physical gNB from UE management through user plane."""

    evidence_path, physical_directory = _paths(run_root, run_id)
    try:
        evidence = PhysicalRunEvidence.read_json(evidence_path)
        activation_status: str | None = None

        if evidence.acceptance.next_stage in {
            PhysicalAcceptanceStage.GNB_N2,
            PhysicalAcceptanceStage.UE_MANAGEMENT,
            PhysicalAcceptanceStage.CELL_ACQUISITION,
            PhysicalAcceptanceStage.PDU_SESSION,
        }:
            activation = execute_authorized_qfit_activation(
                evidence=evidence,
                slice_name=slice_name,
                run_root=run_root,
                known_hosts=known_hosts,
                r2lab_runner=r2lab_runner,
                cluster_runner=cluster_runner,
                evidence_path=evidence_path,
                activation_evidence_path=physical_directory / "qfit-activation.json",
                timeout_seconds=timeout_seconds,
            )
            evidence = activation.evidence
            if activation.activation is not None:
                activation_status = activation.activation.status

        user_plane_proven = (
            evidence.acceptance.outcome_for(PhysicalAcceptanceStage.USER_PLANE).value
            == "passed"
        )
        if evidence.acceptance.next_stage is PhysicalAcceptanceStage.USER_PLANE:
            user_plane = execute_authorized_qfit_user_plane(
                evidence=evidence,
                slice_name=slice_name,
                run_root=run_root,
                known_hosts=known_hosts,
                peer=peer,
                r2lab_runner=r2lab_runner,
                cluster_runner=cluster_runner,
                evidence_path=evidence_path,
                timeout_seconds=timeout_seconds,
            )
            evidence = user_plane.evidence
            user_plane_proven = user_plane.probe.proven
            _write_json(
                physical_directory / "qfit-user-plane.json",
                user_plane.probe.to_dict(),
            )

        allowed_terminal = {
            PhysicalAcceptanceStage.WORKLOAD,
            None,
        }
        if evidence.acceptance.next_stage not in allowed_terminal:
            raise R2LabPhysicalLifecycleError(
                "physical path did not reach the workload boundary"
            )

        return PhysicalPathSummary(
            run_id=run_id,
            evidence_path=evidence_path,
            next_stage=_stage_name(evidence),
            failed_stage=_failed_stage_name(evidence),
            activation_status=activation_status,
            user_plane_proven=user_plane_proven,
        )
    except (
        R2LabAcceptanceError,
        R2LabQfitActivationError,
        OSError,
        ValueError,
    ) as exc:
        if isinstance(exc, R2LabPhysicalLifecycleError):
            raise
        raise R2LabPhysicalLifecycleError(str(exc)) from exc


def run_physical_workload(
    *,
    run_id: str,
    workload_id: str,
    slice_name: str,
    known_hosts: Path,
    inventory_path: Path,
    lock_path: Path = Path("dependencies.lock.yml"),
    deps_root: Path = Path(".deps"),
    run_root: Path = DEFAULT_R2LAB_RUN_ROOT,
    experiment_root: Path = DEFAULT_PHYSICAL_RUN_ROOT,
    collection_seconds: int = 180,
    minimum_per_sensor: int = 3,
    timeout_seconds: int = 30,
    r2lab_runner=r2lab_subprocess_runner,
    cluster_runner=cluster_subprocess_runner,
) -> PhysicalWorkloadSummary:
    """Run the canonical deterministic IoT workload over an accepted physical path."""

    evidence_path, physical_directory = _paths(run_root, run_id)
    workload_result_path = physical_directory / "physical-workload-result.json"
    try:
        evidence = PhysicalRunEvidence.read_json(evidence_path)
        if evidence.acceptance.next_stage is not PhysicalAcceptanceStage.WORKLOAD:
            raise R2LabPhysicalLifecycleError(
                "physical workload requires an accepted UE/PDU/user-plane path"
            )

        config = PhysicalExperimentConfig(
            slice_name=slice_name,
            inventory=load_inventory(inventory_path),
            lock=load_lock(lock_path),
            dependency_root=deps_root,
            repository_root=repository_root(),
            workload_id=workload_id,
            run_root=experiment_root,
            collection_seconds=collection_seconds,
            minimum_per_sensor=minimum_per_sensor,
        ).validate()
        outcome = execute_physical_workload_handoff(
            evidence=evidence,
            slice_name=slice_name,
            run_root=run_root,
            known_hosts=known_hosts,
            r2lab_runner=r2lab_runner,
            cluster_runner=cluster_runner,
            executor=build_physical_workload_executor(config),
            evidence_path=evidence_path,
            workload_evidence_path=workload_result_path,
            timeout_seconds=timeout_seconds,
        )
        result = outcome.result
        accepted = (
            result is not None
            and result.accepted
            and result.cleanup_proven
            and outcome.evidence.acceptance.accepted
        )
        return PhysicalWorkloadSummary(
            run_id=run_id,
            workload_id=workload_id,
            evidence_path=evidence_path,
            workload_result_path=workload_result_path,
            accepted=accepted,
            cleanup_proven=(result.cleanup_proven if result is not None else False),
        )
    except (
        R2LabAcceptanceError,
        R2LabQfitActivationError,
        OSError,
        ValueError,
    ) as exc:
        if isinstance(exc, R2LabPhysicalLifecycleError):
            raise
        raise R2LabPhysicalLifecycleError(str(exc)) from exc
