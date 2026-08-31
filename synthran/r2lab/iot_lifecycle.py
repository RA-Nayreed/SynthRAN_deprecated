"""Physical workload composition for the portable Amber IoT source."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TextIO

from synthran.dependencies import load_lock
from synthran.live_preflight import subprocess_runner as cluster_subprocess_runner
from synthran.privacy import repository_root
from synthran.r2lab.acceptance import PhysicalAcceptanceStage, PhysicalRunEvidence
from synthran.r2lab.controller import subprocess_runner as r2lab_subprocess_runner
from synthran.r2lab.iot_workload import PhysicalIoTConfig, build_physical_iot_executor
from synthran.r2lab.physical_inventory import load_physical_inventory
from synthran.r2lab.resources import load_topology
from synthran.r2lab.workload_boundary import execute_physical_workload_handoff
from synthran.r2lab.workload_retry import next_workload_attempt_id
from synthran.utils.environment import scoped_environment


DEFAULT_R2LAB_RUN_ROOT = Path(".synthran/r2lab")
DEFAULT_PHYSICAL_RUN_ROOT = Path(".synthran/experiments-r2lab")


class R2LabIoTLifecycleError(RuntimeError):
    """Raised when the portable physical workload cannot advance safely."""


@dataclass(frozen=True)
class PhysicalIoTWorkloadSummary:
    run_id: str
    workload_id: str
    evidence_path: Path
    workload_result_path: Path
    accepted: bool
    cleanup_proven: bool

    def to_dict(self) -> dict[str, object]:
        return {
            "schema": "synthran/r2lab-workload/v2alpha1",
            "run_id": self.run_id,
            "workload_id": self.workload_id,
            "backend": "r2lab",
            "iot_source": "amber",
            "ue_interface": "wwan0",
            "stages": {
                "workload": self.accepted,
                "data": self.accepted,
                "acceptance": self.accepted,
                "workload_cleanup": self.cleanup_proven,
            },
            "accepted": self.accepted,
            "cleanup_proven": self.cleanup_proven,
            "evidence_path": str(self.evidence_path),
            "workload_result_path": str(self.workload_result_path),
        }


def run_physical_iot_workload(
    *,
    run_id: str,
    workload_id: str,
    slice_name: str,
    owner: str,
    allocation_id: str | None,
    known_hosts: Path,
    inventory_path: Path,
    lock_path: Path = Path("dependencies.lock.yml"),
    deps_root: Path = Path(".deps"),
    run_root: Path = DEFAULT_R2LAB_RUN_ROOT,
    experiment_root: Path = DEFAULT_PHYSICAL_RUN_ROOT,
    collection_seconds: int = 180,
    minimum_per_sensor: int = 3,
    timeout_seconds: int = 30,
    iot_profile: str = "transport-v1",
    iot_seed: int = 424242,
    sensor_period_seconds: int = 10,
    r2lab_runner=r2lab_subprocess_runner,
    cluster_runner=cluster_subprocess_runner,
    progress: TextIO | None = None,
) -> PhysicalIoTWorkloadSummary:
    """Run Amber through the canonical selected-topology physical handoff."""

    run_directory = run_root.expanduser().resolve() / run_id
    evidence_path = run_directory / "physical-run.json"
    physical_directory = run_directory / "physical"
    workload_result_path = physical_directory / "physical-workload-result.json"
    try:
        evidence = PhysicalRunEvidence.read_json(evidence_path)
        if evidence.acceptance.next_stage is not PhysicalAcceptanceStage.WORKLOAD:
            raise R2LabIoTLifecycleError(
                "physical IoT workload requires an accepted UE/PDU/user-plane path"
            )

        effective_workload_id = next_workload_attempt_id(
            workload_id,
            physical_run_id=run_id,
            physical_run_root=run_root,
            experiment_root=experiment_root,
        )
        topology = load_topology(run_root=run_root, run_id=run_id).validate()
        inventory = load_physical_inventory(inventory_path, topology=topology)
        config = PhysicalIoTConfig(
            slice_name=slice_name,
            inventory=inventory,
            lock=load_lock(lock_path),
            dependency_root=deps_root,
            repository_root=repository_root(),
            known_hosts=known_hosts,
            workload_id=effective_workload_id,
            run_root=experiment_root,
            physical_run_root=run_root,
            collection_seconds=collection_seconds,
            minimum_per_sensor=minimum_per_sensor,
            iot_profile=iot_profile,
            iot_seed=iot_seed,
            sensor_period_seconds=sensor_period_seconds,
            progress=progress,
        ).validate()

        cluster_known_hosts = known_hosts.expanduser().resolve()
        if not cluster_known_hosts.is_file():
            raise R2LabIoTLifecycleError("strict SLICES known-hosts file is missing")
        with scoped_environment({"SYNTHRAN_KNOWN_HOSTS": str(cluster_known_hosts)}):
            state, result = execute_physical_workload_handoff(
                evidence=evidence,
                slice_name=slice_name,
                owner=owner,
                allocation_id=allocation_id,
                run_root=run_root,
                known_hosts=cluster_known_hosts,
                r2lab_runner=r2lab_runner,
                cluster_runner=cluster_runner,
                executor=build_physical_iot_executor(config),
                evidence_path=evidence_path,
                workload_evidence_path=workload_result_path,
                timeout_seconds=timeout_seconds,
            )

        accepted = (
            result is not None
            and result.accepted
            and result.cleanup_proven
            and state.acceptance.accepted
        )
        return PhysicalIoTWorkloadSummary(
            run_id=run_id,
            workload_id=effective_workload_id,
            evidence_path=evidence_path,
            workload_result_path=workload_result_path,
            accepted=accepted,
            cleanup_proven=(result.cleanup_proven if result is not None else False),
        )
    except R2LabIoTLifecycleError:
        raise
    except Exception as exc:
        raise R2LabIoTLifecycleError(str(exc)) from exc
